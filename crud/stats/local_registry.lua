---- Internal module used to store statistics.
-- @module crud.stats.local_registry
--

local dev_checks = require('crud.common.dev_checks')
local stash = require('crud.common.stash')
local registry_utils = require('crud.stats.registry_utils')

local registry = {}
local internal = stash.get(stash.name.stats_local_registry)

--- Initialize local metrics registry.
--
--  Registries are not meant to used explicitly
--  by users, init is not guaranteed to be idempotent.
--
-- @function init
--
-- @treturn boolean Returns true.
--
function registry.init()
    internal.registry = {}
    internal.registry.spaces = {}

    return true
end

--- Destroy local metrics registry.
--
--  Registries are not meant to used explicitly
--  by users, destroy is not guaranteed to be idempotent.
--
-- @function destroy
--
-- @treturn boolean Returns `true`.
--
function registry.destroy()
    internal.registry = nil

    return true
end

--- Get copy of local metrics registry.
--
--  Registries are not meant to used explicitly
--  by users, get is not guaranteed to work without init.
--
-- @function get
--
-- @string[opt] space_name
--  If specified, returns table with statistics
--  of operations on table, separated by operation type and
--  execution status. If there wasn't any requests for table,
--  returns `{}`. If not specified, returns table with statistics
--  about all observed spaces.
--
-- @treturn table Returns copy of metrics registry (or registry section).
--
function registry.get(space_name)
    dev_checks('?string')

    if space_name ~= nil then
        return table.deepcopy(internal.registry.spaces[space_name]) or {}
    end

    return table.deepcopy(internal.registry)
end

--- Increase requests count and update latency info.
--
-- @function observe
--
-- @string space_name
--  Name of space.
--
-- @number latency
--  Time of call execution.
--
-- @string op
--  Label of registry collectors.
--  Use `require('crud.stats').op` to pick one.
--
-- @string success
--  `'ok'` if no errors on execution, `'error'` otherwise.
--
-- @treturn boolean Returns `true`.
--
function registry.observe(latency, space_name, op, status)
    dev_checks('number', 'string', 'string', 'string')

    registry_utils.init_collectors_if_required(internal.registry.spaces, space_name, op)
    local collectors = internal.registry.spaces[space_name][op][status]

    collectors.count = collectors.count + 1
    collectors.time = collectors.time + latency
    collectors.latency = collectors.time / collectors.count

    return true
end

return registry
