local errors = require('errors')

local call_cache = require('crud.common.call_cache')
local dev_checks = require('crud.common.dev_checks')
local yield_checks = require('crud.common.yield_checks')
local utils = require('crud.common.utils')
local sharding_utils = require('crud.common.sharding.utils')
local fiber = require('fiber')
local fiber_clock = fiber.clock
local const = require('crud.common.const')
local bucket_ref_unref = require('crud.common.sharding.bucket_ref_unref')

local BaseIterator = require('crud.common.map_call_cases.base_iter')
local BasePostprocessor = require('crud.common.map_call_cases.base_postprocessor')

local CallError = errors.new_class('CallError')

local CALL_FUNC_NAME = 'call_on_storage'
local CRUD_CALL_FUNC_NAME = utils.get_storage_call(CALL_FUNC_NAME)

local call = {}

local function call_on_storage(run_as_user, func_name, ...)
    return yield_checks.guard(box.session.su, run_as_user, call_cache.func_name_to_func(func_name), ...)
end

call.storage_api = {[CALL_FUNC_NAME] = call_on_storage}

function call.get_vshard_call_name(mode, prefer_replica, balance)
    dev_checks('string', '?boolean', '?boolean')

    if mode ~= 'write' and mode ~= 'read' then
        return nil, CallError:new("Unknown call mode: %s", mode)
    end

    if mode == 'write' then
        return 'callrw'
    end

    if not prefer_replica and not balance then
        return 'callro'
    end

    if not prefer_replica and balance then
        return 'callbro'
    end

    if prefer_replica and not balance then
        return 'callre'
    end

    -- prefer_replica and balance
    return 'callbre'
end

local function wrap_vshard_err(err, func_name, replicaset_id)
    -- Do not rewrite ShardingHashMismatchError class.
    if err.class_name == sharding_utils.ShardingHashMismatchError.name then
        return errors.wrap(err)
    end

    if replicaset_id == nil then
        return CallError:new(
            "Function returned an error, but we couldn't figure out the replicaset id: %s", err
        )
    end

    err = utils.update_storage_call_error_description(err, func_name, replicaset_id)
    err = errors.wrap(err)

    return CallError:new(utils.format_replicaset_error(
        replicaset_id, "Function returned an error: %s", err
    ))
end

--- Executes a CRUD function on a vshard replicaset.
local function call_on_replicaset(replicaset, method, func_name, func_args, call_opts)
    local func_args_ext = utils.append_array({ box.session.effective_user(), func_name }, func_args)
    return replicaset[method](replicaset, CRUD_CALL_FUNC_NAME, func_args_ext, call_opts)
end

--- The bucket is not on the replicaset it is routed to anymore, so its
--- route is stale and must be dropped from the router cache.
local BUCKET_MOVED_ERRS = {
    WRONG_BUCKET = true,
    BUCKET_IS_LOCKED = true,
    TRANSFER_IS_IN_PROGRESS = true,
}

--- Performs a recovery action for a call error:
--- * MISSING_MASTER - the replicaset master is not discovered yet, so it
---   is discovered explicitly;
--- * NON_MASTER - the cached master is stale, but the bucket did not move,
---   so the master is updated and the same replicaset can be retried;
--- * WRONG_BUCKET, BUCKET_IS_LOCKED, TRANSFER_IS_IN_PROGRESS - the bucket
---   route is stale, so the route cache is reset and the bucket is routed
---   anew.
---
--- The routes of all the moved buckets are reset even when the request itself
--- cannot be retried: the cache is stale regardless of the retry decision.
---
--- Returns:
--- * replicaset - the replicaset to retry the request on;
--- * nil - the request cannot be retried;
--- * nil, err - the recovery itself failed, so the request cannot be retried.
local function recover_from_err(vshard_router, replicaset, err)
    local vshard_err = err

    if err.class_name == bucket_ref_unref.BucketRefError.name then
        for _, bucket_ref_err in ipairs(err.bucket_ref_errs) do
            if BUCKET_MOVED_ERRS[bucket_ref_err.vshard_err.name] then
                vshard_router:_bucket_reset(bucket_ref_err.bucket_id)
            end
        end

        -- A request that failed on several buckets has no single replicaset
        -- to be retried on, so only a single-bucket one is recovered further.
        if #err.bucket_ref_errs ~= 1 then
            return nil
        end

        vshard_err = err.bucket_ref_errs[1].vshard_err

        if BUCKET_MOVED_ERRS[vshard_err.name] then
            -- The stale route has been reset above, so the bucket is routed
            -- anew.
            local new_replicaset, route_err = vshard_router:route(err.bucket_ref_errs[1].bucket_id)
            if route_err ~= nil then
                return nil, CallError:new(
                    "Failed to get router replicaset: %s, after an error: %s",
                    tostring(route_err),
                    tostring(err)
                )
            end
            return new_replicaset
        end
    end

    if vshard_err.name == 'MISSING_MASTER' then
        replicaset:locate_master()
        -- The master is not found, so the retry would get the same error.
        if replicaset.master == nil then
            return nil
        end
        return replicaset
    elseif vshard_err.name == 'NON_MASTER' then
        -- If the master update failed, the retry would get the same error.
        if not replicaset:update_master(vshard_err.replica, vshard_err.master) then
            return nil
        end
        return replicaset
    end

    return nil
end

local function call_single_with_recovery(vshard_router,
    replicaset, method, func_name, func_args, call_opts)
    local deadline = fiber_clock() + call_opts.timeout

    local resp, err = call_on_replicaset(replicaset, method, func_name, func_args, call_opts)
    if err == nil then
        return resp, err, replicaset.id
    end

    local retry_replicaset, recover_err = recover_from_err(vshard_router, replicaset, err)
    if retry_replicaset == nil then
        return resp, recover_err or err, replicaset.id
    end

    local timeout = deadline - fiber_clock()
    if timeout <= 0 then
        return resp, err, replicaset.id
    end

    replicaset = retry_replicaset

    call_opts.timeout = timeout
    if call_opts.request_timeout ~= nil and call_opts.request_timeout > timeout then
        call_opts.request_timeout = timeout
    end

    resp, err = call_on_replicaset(replicaset, method, func_name, func_args, call_opts)
    return resp, err, replicaset.id
end

local function call_map_with_recovery(replicaset, method, func_name, func_args,
    call_opts, deadline)
    local future, err = call_on_replicaset(replicaset, method, func_name, func_args, call_opts)
    if err == nil or err.name ~= 'MISSING_MASTER' then
        return future, err
    end

    if fiber_clock() >= deadline then
        return future, err
    end

    replicaset:locate_master()

    -- The master is not found, so the retry would get the same error.
    if replicaset.master == nil then
        return future, err
    end

    local timeout = deadline - fiber_clock()
    if timeout <= 0 then
        return future, err
    end

    if call_opts.request_timeout ~= nil and call_opts.request_timeout > timeout then
        call_opts.request_timeout = timeout
    end

    return call_on_replicaset(replicaset, method, func_name, func_args, call_opts)
end

function call.map(vshard_router, func_name, func_args, opts)
    dev_checks('table', 'string', '?table', {
        mode = 'string',
        prefer_replica = '?boolean',
        balance = '?boolean',
        timeout = '?number',
        request_timeout = '?number',
        replicasets = '?table',
        iter = '?table',
        postprocessor = '?table',
    })
    opts = opts or {}

    local vshard_call_name, err = call.get_vshard_call_name(opts.mode, opts.prefer_replica, opts.balance)
    if err ~= nil then
        return nil, err
    end

    local timeout = opts.timeout or const.DEFAULT_VSHARD_CALL_TIMEOUT

    local iter = opts.iter
    if iter == nil then
        iter, err = BaseIterator:new({
                        func_args = func_args,
                        replicasets = opts.replicasets,
                        vshard_router = vshard_router,
                    })
        if err ~= nil then
            return nil, err
        end
    end

    local postprocessor = opts.postprocessor
    if postprocessor == nil then
        postprocessor = BasePostprocessor:new()
    end

    local futures_by_replicasets = {}
    local call_opts = {
        is_async = true,
        request_timeout = opts.mode == 'read' and opts.request_timeout or nil,
    }

    local deadline = fiber_clock() + timeout
    while iter:has_next() do
        local args, replicaset, replicaset_id = iter:get()

        local future, err = call_map_with_recovery(replicaset, vshard_call_name,
            func_name, args, call_opts, deadline)

        if err ~= nil then
            local result_info = {
                key = replicaset_id,
                value = nil,
            }

            local err_info = {
                err_wrapper = wrap_vshard_err,
                err = err,
                wrapper_args = {func_name, replicaset_id},
            }

            -- Enforce early exit on futures build fail.
            postprocessor:collect(result_info, err_info)
            return postprocessor:get()
        end

        futures_by_replicasets[replicaset_id] = future
    end

    for replicaset_id, future in pairs(futures_by_replicasets) do
        local wait_timeout = deadline - fiber_clock()
        if wait_timeout < 0 then
            wait_timeout = 0
        end

        local result, err = future:wait_result(wait_timeout)

        local result_info = {
            key = replicaset_id,
            value = result,
        }

        local err_info = {
            err_wrapper = wrap_vshard_err,
            err = err,
            wrapper_args = {func_name, replicaset_id},
        }

        local early_exit = postprocessor:collect(result_info, err_info)
        if early_exit then
            break
        end
    end

    return postprocessor:get()
end

function call.single(vshard_router, bucket_id, func_name, func_args, opts)
    dev_checks('table', 'number', 'string', '?table', {
        mode = 'string',
        prefer_replica = '?boolean',
        balance = '?boolean',
        timeout = '?number',
        request_timeout = '?number',
    })

    local vshard_call_name, err = call.get_vshard_call_name(opts.mode, opts.prefer_replica, opts.balance)
    if err ~= nil then
        return nil, err
    end

    local replicaset, err = vshard_router:route(bucket_id)
    if err ~= nil then
        return nil, CallError:new("Failed to get router replicaset: %s", tostring(err))
    end

    local timeout = opts.timeout or const.DEFAULT_VSHARD_CALL_TIMEOUT
    local request_timeout = opts.mode == 'read' and opts.request_timeout or nil

    local res, err, replicaset_id = call_single_with_recovery(vshard_router, replicaset, vshard_call_name,
        func_name, func_args, {timeout = timeout, request_timeout = request_timeout})

    if err ~= nil then
        return nil, wrap_vshard_err(err, func_name, replicaset_id)
    end

    if res == box.NULL then
        return nil
    end

    return res
end

function call.any(vshard_router, func_name, func_args, opts)
    dev_checks('table', 'string', '?table', {
        timeout = '?number',
    })

    local timeout = opts.timeout or const.DEFAULT_VSHARD_CALL_TIMEOUT

    local replicasets, err = vshard_router:routeall()
    if replicasets == nil then
        return nil, CallError:new("Failed to get router replicasets: %s", err.err)
    end

    local last_replicaset_id = nil
    local last_err = nil

    local deadline = fiber_clock() + timeout

    for _, replicaset in pairs(replicasets) do
        local wait_timeout = deadline - fiber_clock()

        local is_timeout = wait_timeout < 0
        if is_timeout then
            wait_timeout = 0
        end

        local res, err, replicaset_id = call_single_with_recovery(vshard_router, replicaset, 'callro',
            func_name, func_args, {timeout = wait_timeout})

        if err == nil then
            if res == box.NULL then
                return nil
            end
            return res
        end

        last_replicaset_id = replicaset_id
        last_err = err

        if is_timeout then
            break
        end
    end

    return nil, wrap_vshard_err(last_err, func_name, last_replicaset_id)
end

return call
