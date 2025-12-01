local errors = require('errors')

local call_cache = require('crud.common.call_cache')
local dev_checks = require('crud.common.dev_checks')
local utils = require('crud.common.utils')
local sharding_utils = require('crud.common.sharding.utils')
local fiber = require('fiber')
local const = require('crud.common.const')
local rebalance = require('crud.common.rebalance')
local bucket_ref_unref = require('crud.common.sharding.bucket_ref_unref')

local BaseIterator = require('crud.common.map_call_cases.base_iter')
local BasePostprocessor = require('crud.common.map_call_cases.base_postprocessor')

local CallError = errors.new_class('CallError')

local CALL_FUNC_NAME = 'call_on_storage'
local CRUD_CALL_FUNC_NAME = utils.get_storage_call(CALL_FUNC_NAME)
local CRUD_CALL_FIBER_NAME = CRUD_CALL_FUNC_NAME .. '/'

local call = {}

local function call_on_storage_safe(run_as_user, func_name, ...)
    fiber.name(CRUD_CALL_FIBER_NAME .. 'safe/' .. func_name)
    return box.session.su(run_as_user, call_cache.func_name_to_func(func_name), ...)
end

local function call_on_storage_fast(run_as_user, func_name, ...)
    fiber.name(CRUD_CALL_FIBER_NAME .. 'fast/' .. func_name)
    return box.session.su(run_as_user, call_cache.func_name_to_func(func_name), ...)
end

local call_on_storage = rebalance.safe_mode and call_on_storage_safe or call_on_storage_fast

local function safe_mode_enable()
    call_on_storage = call_on_storage_safe

    for fb_id, fb in pairs(fiber.info()) do
        if string.find(fb.name, CRUD_CALL_FIBER_NAME) then
            fiber.kill(fb_id)
        end
    end
end

local function safe_mode_disable()
    call_on_storage = call_on_storage_fast
end

rebalance.register_safe_mode_enable_hook(safe_mode_enable)
rebalance.register_safe_mode_disable_hook(safe_mode_disable)

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

local function wrap_vshard_err(vshard_router, err, func_name, replicaset_id, bucket_id)
    -- Do not rewrite ShardingHashMismatchError class.
    if err.class_name == sharding_utils.ShardingHashMismatchError.name then
        return errors.wrap(err)
    end

    if replicaset_id == nil then
        local replicaset, _ = vshard_router:route(bucket_id)
        if replicaset == nil then
            return CallError:new(
                "Function returned an error, but we couldn't figure out the replicaset: %s", err
            )
        end

        replicaset_id = utils.get_replicaset_id(vshard_router, replicaset)

        if replicaset_id == nil then
            return CallError:new(
                "Function returned an error, but we couldn't figure out the replicaset id: %s", err
            )
        end
    end

    err = utils.update_storage_call_error_description(err, func_name, replicaset_id)
    err = errors.wrap(err)

    return CallError:new(utils.format_replicaset_error(
        replicaset_id, "Function returned an error: %s", err
    ))
end

--- Executes a vshard call and retries once after performing recovery actions
--- like bucket cache reset, destination redirect (for single calls), or master discovery.
local function call_with_retry_and_recovery(vshard_router,
    replicaset, method, func_name, func_args, call_opts, is_single_call)
    local func_args_ext = utils.append_array({ box.session.effective_user(), func_name }, func_args)

    -- In case cluster was just bootstrapped with auto master discovery,
    -- replicaset may miss master.
    local resp, err = replicaset[method](replicaset, CRUD_CALL_FUNC_NAME, func_args_ext, call_opts)

    if err == nil then
        return resp, err
    end

    -- This is a partial copy of error handling from vshard.router.router_call_impl()
    -- It is much simpler mostly because bucket_set() can't be accessed from outside vshard.
    if err.class_name == bucket_ref_unref.BucketRefError.name then
        local redirect_replicaset
        if is_single_call and #err.bucket_ref_errs == 1 then
            local single_err = err.bucket_ref_errs[1]
            local destination = single_err.vshard_err.destination
            if destination and vshard_router.replicasets[destination] then
                redirect_replicaset = vshard_router.replicasets[destination]
            end
        end

        for _, bucket_ref_err in pairs(err.bucket_ref_errs) do
            local bucket_id = bucket_ref_err.bucket_id
            local vshard_err = bucket_ref_err.vshard_err
            if vshard_err.name == 'WRONG_BUCKET' or
                vshard_err.name == 'BUCKET_IS_LOCKED' or
                vshard_err.name == 'TRANSFER_IS_IN_PROGRESS' then
                vshard_router:_bucket_reset(bucket_id)
            end
        end

        if redirect_replicaset ~= nil then
            replicaset = redirect_replicaset
        end
    elseif err.name == 'MISSING_MASTER' and replicaset.locate_master ~= nil then
        replicaset:locate_master()
    end

    -- Retry only once: should be enough for initial discovery,
    -- otherwise force user fix up cluster bootstrap.
    return replicaset[method](replicaset, CRUD_CALL_FUNC_NAME, func_args_ext, call_opts)
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
    while iter:has_next() do
        local args, replicaset, replicaset_id = iter:get()

        local future, err = call_with_retry_and_recovery(vshard_router, replicaset, vshard_call_name,
            func_name, args, call_opts, false)

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

    local deadline = fiber.clock() + timeout
    for replicaset_id, future in pairs(futures_by_replicasets) do
        local wait_timeout = deadline - fiber.clock()
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

    local res, err = call_with_retry_and_recovery(vshard_router, replicaset, vshard_call_name,
        func_name, func_args, {timeout = timeout, request_timeout = request_timeout}, true)
    if err ~= nil then
        return nil, wrap_vshard_err(vshard_router, err, func_name, nil, bucket_id)
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
    local replicaset_id, replicaset = next(replicasets)

    local res, err = call_with_retry_and_recovery(vshard_router, replicaset, 'call',
        func_name, func_args, {timeout = timeout}, false)
    if err ~= nil then
        return nil, wrap_vshard_err(vshard_router, err, func_name, replicaset_id)
    end

    if res == box.NULL then
        return nil
    end

    return res
end

return call
