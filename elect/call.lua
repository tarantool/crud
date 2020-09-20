local checks = require('checks')
local fiber = require('fiber')
local vshard = require('vshard')
local errors = require('errors')

local registry = require('elect.registry')
local utils = require('elect.utils')

local CallError = errors.new_class('Call')
local NotInitializedError = errors.new_class('NotInitialized')

local call = {}

local CALL_FUNC_NAME = '__elect_call'

local DEFAULT_VSHARD_CALL_TIMEOUT = 2

--- Initializes call on node
--
-- Wrapper function is registered to call functions remotely.
--
-- @function init
--
function call.init()
    local function elect_call(opts)
        checks({
            func_name = 'string',
            func_args = '?table',
        })

        local func = registry.get(opts.func_name) or rawget(_G, opts.func_name)

        if type(func) ~= 'function' then
            return nil, CallError:new('Function %s is not registered', opts.func_name)
        end

        return func(unpack(opts.func_args or {}))
    end

    -- register global function
    rawset(_G, CALL_FUNC_NAME, elect_call)
end

local function call_on_replicaset(replicaset, channel, vshard_call, func_name, func_args, opts)
    local elect_call_arg = {
        func_name = func_name,
        func_args = func_args,
    }

    -- replicaset:<vshard_call>(func_name,...)
    local func_ret, err = replicaset[vshard_call](replicaset, CALL_FUNC_NAME, {elect_call_arg}, opts)
    if type(err) == 'table' and err.type == 'ClientError' and type(err.message) == 'string' then
        if err.message == string.format("Procedure '%s' is not defined", CALL_FUNC_NAME) then
            err = NotInitializedError:new("elect isn't initialized on replicaset")
        end
    end

    channel:put({
        replicaset_uuid = replicaset.uuid,
        func_ret = func_ret,
        err = err,
    })
end

local function call_impl(vshard_call, opts)
    checks('string', {
        func_name = 'string',
        func_args = '?table',
        timeout = '?number',
        replicasets = '?table',
    })

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
            call_on_replicaset, replicaset, channel, vshard_call, opts.func_name, opts.func_args, {
                timeout = timeout,
            }
        )
    end

    local results = {}

    for _ = 1, channel:size() do
        local res = channel:get()

        if res == nil then
            if channel:is_closed() then
                return nil, CallError:new(utils.format_replicaset_error(
                    res.replicaset_uuid, "Channel is closed"
                ))
            end

            return nil, CallError:new(utils.format_replicaset_error(
                res.replicaset_uuid, "Timeout was reached"
            ))
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
-- Allowed functions to call can be specified by `elect.register` call.
-- If function with specified `opts.func_name` isn't registered,
-- global function with this name is called.
--
-- Uses vshard `replicaset:callrw`
--
-- @function rw
--
-- @tparam table opts Available options are:
--
-- @tparam string opts.func_name
--  A function name
--
-- @tparam ?table opts.func_args
--  Array of arguments to passed to the function
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
function call.rw(opts)
    return call_impl('callrw', opts)
end

--- Calls specified function on all cluster storages.
--
-- The same as `rw`, but uses vshard `replicaset:callro`
--
-- @function ro
--
function call.ro(opts)
    return call_impl('callro', opts)
end

return call
