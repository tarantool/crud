local fiber = require('fiber')
local vshard = require('vshard')
local errors = require('errors')

local dev_checks = require('crud.common.dev_checks')
local utils = require('crud.common.utils')

local CallError = errors.new_class('Call')
local NotInitializedError = errors.new_class('NotInitialized')

local call = {}

local DEFAULT_VSHARD_CALL_TIMEOUT = 2

local function call_on_replicaset(replicaset, channel, vshard_call, func_name, func_args, opts)
    -- replicaset:<vshard_call>(func_name,...)
    local func_ret, err = replicaset[vshard_call](replicaset, func_name, func_args, opts)
    if type(err) == 'table' and err.type == 'ClientError' and type(err.message) == 'string' then
        if err.message == string.format("Procedure '%s' is not defined", func_name) then
            if func_name:startswith('_crud.') then
                err = NotInitializedError:new("crud isn't initialized on replicaset")
            else
                err = NotInitializedError:new("Function %s is not registered", func_name)
            end
        end
    end

    channel:put({
        replicaset_uuid = replicaset.uuid,
        func_ret = func_ret,
        err = err,
    })
end

local function call_impl(vshard_call, func_name, func_args, opts)
    dev_checks('string', 'string', '?table', {
        timeout = '?number',
        replicasets = '?table',
    })

    opts = opts or {}

    local timeout = opts.timeout or DEFAULT_VSHARD_CALL_TIMEOUT

    local replicasets, err
    if opts.replicasets ~= nil then
        replicasets = opts.replicasets
    else
        replicasets, err = vshard.router.routeall()
        if replicasets == nil then
            return nil, CallError:new("Failed to get all replicasets: %s", err.err)
        end
    end

    local nodes_count = utils.table_count(replicasets)
    local channel = fiber.channel(nodes_count)

    for _, replicaset in pairs(replicasets) do
        fiber.create(
            call_on_replicaset, replicaset, channel, vshard_call, func_name, func_args, {
                timeout = timeout,
            }
        )
    end

    local results = {}

    for _ = 1, channel:size() do
        local res = channel:get()

        if res == nil then
            if channel:is_closed() then
                return nil, CallError:new("Channel is closed")
            end

            return nil, CallError:new("Timeout was reached")
        end

        if res.err ~= nil then
            res.err = errors.wrap(res.err)

            return nil, CallError:new(utils.format_replicaset_error(
                res.replicaset_uuid, "Function returned an error: %s", res.err
            ))
        end

        results[res.replicaset_uuid] = res.func_ret
    end

    return results
end

--- Calls specified function on all cluster storages.
--
-- Allowed functions to call can be specified by `crud.register` call.
-- If function with specified `opts.func_name` isn't registered,
-- global function with this name is called.
--
-- Uses vshard `replicaset:callrw`
--
-- @function rw
--
-- @param string func_name
--  A function name
--
-- @param ?table func_args
--  Array of arguments to be passed to the function
--
-- @tparam table opts Available options are:
--
-- @tparam ?number opts.timeout
--  Function call timeout
--
-- @tparam ?table opts.replicasets
--  vshard replicasets to call the function.
--  By default, function is called on the all storages.
--
-- Returns map {replicaset_uuid: result} with all specified replicasets results
--
-- @return[1] table
-- @treturn[2] nil
-- @treturn[2] table Error description
--
function call.rw(func_name, func_args, opts)
    return call_impl('callrw', func_name, func_args, opts)
end

--- Calls specified function on all cluster storages.
--
-- The same as `rw`, but uses vshard `replicaset:callro`
--
-- @function ro
--
function call.ro(func_name, func_args, opts)
    return call_impl('callro', func_name, func_args, opts)
end

--- Calls specified function on a node according to bucket_id.
--
-- Exactly mimics the contract of vshard.router.callrw, but adds
-- better error hangling
--
-- @function rw_single
--
function call.rw_single(bucket_id, func_name, func_args, options)
    local res, err = vshard.router.callrw(bucket_id, func_name, func_args, options)

    -- This is a workaround, until vshard supports telling us where the error happened
    if err ~= nil then
        if type(err) == 'table' and err.type == 'ClientError' and type(err.message) == 'string' then
            if err.message == string.format("Procedure '%s' is not defined", func_name) then
               err = NotInitializedError:new("crud isn't initialized on replicaset")
            end
        end

        local replicaset, _ = vshard.router.route(bucket_id)
        if replicaset == nil then
            return nil, CallError:new("Function returned an error, but we couldn't figure out the replicaset: %s",
                                      err)
        end

        return nil, CallError:new(utils.format_replicaset_error(
                                     replicaset.uuid, "Function returned an error: %s", err
                                 ))
    end

    if res == box.NULL then
        return nil
    end

    return res
end

return call
