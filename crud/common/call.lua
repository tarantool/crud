local vshard = require('vshard')
local errors = require('errors')

local dev_checks = require('crud.common.dev_checks')
local utils = require('crud.common.utils')
local fiber_clock = require('fiber').clock

local CallError = errors.new_class('Call')
local NotInitializedError = errors.new_class('NotInitialized')

local call = {}

local DEFAULT_VSHARD_CALL_TIMEOUT = 2

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

    local futures_by_replicasets = {}
    local call_opts = {is_async = true}
    for _, replicaset in pairs(replicasets) do
        local future = replicaset[vshard_call](replicaset, func_name, func_args, call_opts)
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
            if err.type == 'ClientError' and type(err.message) == 'string' then
                if err.message == string.format("Procedure '%s' is not defined", func_name) then
                    if func_name:startswith('_crud.') then
                        err = NotInitializedError:new("crud isn't initialized on replicaset: %q", replicaset_uuid)
                    else
                        err = NotInitializedError:new("Function %s is not registered", func_name)
                    end
                end
            end
            err = errors.wrap(err)
            return nil, CallError:new(utils.format_replicaset_error(
                replicaset_uuid, "Function returned an error: %s", err
            ))
        end
        results[replicaset_uuid] = result[1]
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
            return nil, CallError:new(
                "Function returned an error, but we couldn't figure out the replicaset: %s", err
            )
        end

        err = errors.wrap(err)

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
