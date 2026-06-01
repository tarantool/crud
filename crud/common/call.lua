local errors = require('errors')

local call_cache = require('crud.common.call_cache')
local dev_checks = require('crud.common.dev_checks')
local storage_call = require('crud.common.storage_call')
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

local call = {}

local function call_on_storage(run_as_user, func_name, ...)
    return yield_checks.guard(box.session.su, run_as_user, call_cache.func_name_to_func(func_name), ...)
end

call.storage_api = {[storage_call.CALL_ON_STORAGE_FUNC_NAME] = call_on_storage}

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

local function perform_storage_call(replicaset, method, replicaset_id, func_name, func_args, call_opts, force_legacy)
    local call_func_name, call_args, used_legacy_call = storage_call.prepare(
        replicaset_id, func_name, func_args, force_legacy
    )

    local resp, err = replicaset[method](replicaset, call_func_name, call_args, call_opts)
    if err == nil and not used_legacy_call and not (call_opts or {}).is_async then
        storage_call.mark_call_on_storage_supported(replicaset_id)
    end

    return resp, err, used_legacy_call
end

local function fallback_to_legacy_if_needed(replicaset, method, replicaset_id,
    func_name, func_args, call_opts, resp, err, used_legacy_call)
    if storage_call.should_fallback_to_legacy(replicaset_id, err, used_legacy_call) then
        resp, err, used_legacy_call = perform_storage_call(
            replicaset, method, replicaset_id, func_name, func_args, call_opts, true
        )
    end

    return resp, err, used_legacy_call
end

--- Executes a vshard call and retries once after performing recovery actions
--- like bucket cache reset, destination redirect (for single calls), or master discovery.
local function call_with_retry_and_recovery(vshard_router,
    replicaset, replicaset_id, method, func_name, func_args, call_opts, is_single_call)
    -- In case cluster was just bootstrapped with auto master discovery,
    -- replicaset may miss master.
    local resp, err, used_legacy_call = perform_storage_call(
        replicaset, method, replicaset_id, func_name, func_args, call_opts
    )
    resp, err, used_legacy_call = fallback_to_legacy_if_needed(
        replicaset, method, replicaset_id, func_name, func_args, call_opts, resp, err, used_legacy_call
    )

    if err == nil then
        return resp, err, used_legacy_call
    end

    -- This is a partial copy of error handling from vshard.router.router_call_impl()
    -- It is much simpler mostly because bucket_set() can't be accessed from outside vshard.
    if err.class_name == bucket_ref_unref.BucketRefError.name then
        if is_single_call and #err.bucket_ref_errs == 1 then
            local single_err = err.bucket_ref_errs[1]
            local destination = single_err.vshard_err.destination
            if destination and vshard_router.replicasets[destination] then
                replicaset = vshard_router.replicasets[destination]
                replicaset_id = destination
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
    elseif err.name == 'MISSING_MASTER' and replicaset.locate_master ~= nil then
        replicaset:locate_master()
    end

    -- Retry only once: should be enough for initial discovery,
    -- otherwise force user fix up cluster bootstrap.
    resp, err, used_legacy_call = perform_storage_call(
        replicaset, method, replicaset_id, func_name, func_args, call_opts
    )
    resp, err, used_legacy_call = fallback_to_legacy_if_needed(
        replicaset, method, replicaset_id, func_name, func_args, call_opts, resp, err, used_legacy_call
    )

    return resp, err, used_legacy_call
end

function call.storage_call(replicaset, method, replicaset_id, func_name, func_args, call_opts, force_legacy)
    return perform_storage_call(replicaset, method, replicaset_id, func_name, func_args, call_opts, force_legacy)
end

local function wait_result_with_compat(future_info, wait_timeout)
    local result, err = future_info.future:wait_result(wait_timeout)
    local result_err = storage_call.result_error(result)

    if err == nil and result_err == nil and not future_info.used_legacy_call then
        storage_call.mark_call_on_storage_supported(future_info.replicaset_id)
    end

    if storage_call.should_fallback_to_legacy(
        future_info.replicaset_id, err or result_err, future_info.used_legacy_call
    ) then
        local future, call_err, used_legacy_call = perform_storage_call(
            future_info.replicaset,
            future_info.method,
            future_info.replicaset_id,
            future_info.func_name,
            future_info.func_args,
            future_info.call_opts,
            true
        )
        if call_err ~= nil then
            return nil, call_err
        end

        future_info.future = future
        future_info.used_legacy_call = used_legacy_call
        result, err = future:wait_result(wait_timeout)
    end

    return result, err
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

        local future, err, used_legacy_call = call_with_retry_and_recovery(vshard_router, replicaset, replicaset_id,
            vshard_call_name, func_name, args, call_opts, false)

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

        futures_by_replicasets[replicaset_id] = {
            future = future,
            replicaset = replicaset,
            replicaset_id = replicaset_id,
            method = vshard_call_name,
            func_name = func_name,
            func_args = args,
            call_opts = call_opts,
            used_legacy_call = used_legacy_call,
        }
    end

    local deadline = fiber_clock() + timeout
    for replicaset_id, future_info in pairs(futures_by_replicasets) do
        local wait_timeout = deadline - fiber_clock()
        if wait_timeout < 0 then
            wait_timeout = 0
        end

        local result, err = wait_result_with_compat(future_info, wait_timeout)

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

    local replicaset_id = utils.get_replicaset_id(vshard_router, replicaset)
    local res, err = call_with_retry_and_recovery(vshard_router, replicaset, replicaset_id, vshard_call_name,
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

    local res, err = call_with_retry_and_recovery(vshard_router, replicaset, replicaset_id, 'call',
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
