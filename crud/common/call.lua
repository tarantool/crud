local errors = require('errors')

local dev_checks = require('crud.common.dev_checks')
local utils = require('crud.common.utils')
local sharding_utils = require('crud.common.sharding.utils')
local fiber_clock = require('fiber').clock
local const = require('crud.common.const')

local BaseIterator = require('crud.common.map_call_cases.base_iter')
local BasePostprocessor = require('crud.common.map_call_cases.base_postprocessor')

local CallError = errors.new_class('CallError')

local call = {}

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

function call.map(vshard_router, func_name, func_args, opts)
    dev_checks('table', 'string', '?table', {
        mode = 'string',
        prefer_replica = '?boolean',
        balance = '?boolean',
        timeout = '?number',
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
    local call_opts = {is_async = true}
    while iter:has_next() do
        local args, replicaset, replicaset_id = iter:get()
        local future = replicaset[vshard_call_name](replicaset, func_name, args, call_opts)
        futures_by_replicasets[replicaset_id] = future
    end

    local deadline = fiber_clock() + timeout
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
    })

    local vshard_call_name, err = call.get_vshard_call_name(opts.mode, opts.prefer_replica, opts.balance)
    if err ~= nil then
        return nil, err
    end

    local timeout = opts.timeout or const.DEFAULT_VSHARD_CALL_TIMEOUT

    local res, err = vshard_router[vshard_call_name](vshard_router, bucket_id, func_name, func_args, {
        timeout = timeout,
    })

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

    local res, err = replicaset:call(func_name, func_args, {
        timeout = timeout,
    })
    if err ~= nil then
        return nil, wrap_vshard_err(vshard_router, err, func_name, replicaset_id)
    end

    if res == box.NULL then
        return nil
    end

    return res
end

return call
