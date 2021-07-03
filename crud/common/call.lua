local vshard = require('vshard')
local errors = require('errors')

local dev_checks = require('crud.common.dev_checks')
local utils = require('crud.common.utils')
local fiber_clock = require('fiber').clock

local CallError = errors.new_class('CallError')
local NotInitializedError = errors.new_class('NotInitialized')

local call = {}

call.DEFAULT_VSHARD_CALL_TIMEOUT = 2

function call.get_vshard_call_name(mode, prefer_replica, balance)
    dev_checks('string', '?boolean', '?boolean')

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

local function wrap_vshard_err(err, func_name, replicaset_uuid, bucket_id)
    if err.type == 'ClientError' and type(err.message) == 'string' then
        if err.message == string.format("Procedure '%s' is not defined", func_name) then
            if func_name:startswith('_crud.') then
                err = NotInitializedError:new("crud isn't initialized on replicaset: %q", replicaset_uuid)
            else
                err = NotInitializedError:new("Function %s is not registered", func_name)
            end
        end
    end

    if replicaset_uuid == nil then
        local replicaset, _ = vshard.router.route(bucket_id)
        if replicaset == nil then
            return CallError:new(
                "Function returned an error, but we couldn't figure out the replicaset: %s", err
            )
        end

        replicaset_uuid = replicaset.uuid
    end

    err = errors.wrap(err)

    return CallError:new(utils.format_replicaset_error(
        replicaset_uuid, "Function returned an error: %s", err
    ))
end

function call.map(func_name, func_args, opts)
    dev_checks('string', '?table', {
        mode = 'string',
        prefer_replica = '?boolean',
        balance = '?boolean',
        timeout = '?number',
        replicasets = '?table',
    })
    opts = opts or {}

    local vshard_call_name = call.get_vshard_call_name(opts.mode, opts.prefer_replica, opts.balance)

    local timeout = opts.timeout or call.DEFAULT_VSHARD_CALL_TIMEOUT

    local replicasets, err
    if opts.replicasets ~= nil then
        replicasets = opts.replicasets
    else
        replicasets, err = vshard.router.routeall()
        if replicasets == nil then
            return nil, CallError:new("Failed to get all replicasets: %s", err.err)
        end
    end

    local futures_by_replicasets = {}
    local call_opts = {is_async = true}
    for _, replicaset in pairs(replicasets) do
        local future = replicaset[vshard_call_name](replicaset, func_name, func_args, call_opts)
        futures_by_replicasets[replicaset.uuid] = future
    end

    local results = {}
    local deadline = fiber_clock() + timeout
    for replicaset_uuid, future in pairs(futures_by_replicasets) do
        local wait_timeout = deadline - fiber_clock()
        if wait_timeout < 0 then
            wait_timeout = 0
        end

        local result, err = future:wait_result(wait_timeout)
        if err == nil and result[1] == nil then
            err = result[2]
        end

        if err ~= nil then
            return nil, wrap_vshard_err(err, func_name, replicaset_uuid)
        end

        results[replicaset_uuid] = result
    end

    return results
end

function call.single(bucket_id, func_name, func_args, opts)
    dev_checks('number', 'string', '?table', {
        mode = 'string',
        prefer_replica = '?boolean',
        balance = '?boolean',
        timeout = '?number',
    })

    local vshard_call_name = call.get_vshard_call_name(opts.mode, opts.prefer_replica, opts.balance, opts.mode)

    local timeout = opts.timeout or call.DEFAULT_VSHARD_CALL_TIMEOUT

    local res, err = vshard.router[vshard_call_name](bucket_id, func_name, func_args, {
        timeout = timeout,
    })

    if err ~= nil then
        return nil, wrap_vshard_err(err, func_name, nil, bucket_id)
    end

    if res == box.NULL then
        return nil
    end

    return res
end

return call
