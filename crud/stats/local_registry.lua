---- Internal module used to store statistics.
-- @module crud.stats.local_registry
--

local errors = require('errors')

local dev_checks = require('crud.common.dev_checks')
local stash = require('crud.common.stash')
local op_module = require('crud.stats.operation')
local registry_utils = require('crud.stats.registry_utils')

local registry = {}
local internal = stash.get(stash.name.stats_local_registry)
local StatsLocalError = errors.new_class('StatsLocalError', {capture_stack = false})

--- Initialize local metrics registry.
--
--  Registries are not meant to used explicitly
--  by users, init is not guaranteed to be idempotent.
--
-- @function init
--
-- @tab opts
--
-- @bool opts.quantiles
--  Quantiles is not supported for local, only `false` is valid.
--
-- @number opts.quantile_tolerated_error
--  Quantiles is not supported for local, so the value is ignored.
--
-- @number opts.quantile_age_buckets_count
--  Quantiles is not supported for local, so the value is ignored.
--
-- @number opts.quantile_max_age_time
--  Quantiles is not supported for local, so the value is ignored.
--
-- @treturn boolean Returns `true`.
--
function registry.init(opts)
    dev_checks({
        quantiles = 'boolean',
        quantile_tolerated_error = 'number',
        quantile_age_buckets_count = 'number',
        quantile_max_age_time = 'number',
    })

    StatsLocalError:assert(opts.quantiles == false,
        "Quantiles are not supported for 'local' statistics registry")

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
    collectors.latency_average = collectors.time / collectors.count
    collectors.latency = collectors.latency_average

    return true
end

--- Increase statistics of storage select/pairs calls
--
-- @function observe_fetch
--
-- @string space_name
--  Name of space.
--
-- @number tuples_fetched
--  Count of tuples fetched during storage call.
--
-- @number tuples_lookup
--  Count of tuples looked up on storages while collecting response.
--
-- @treturn boolean Returns true.
--
function registry.observe_fetch(tuples_fetched, tuples_lookup, space_name)
    dev_checks('number', 'number', 'string')

    local op = op_module.SELECT
    registry_utils.init_collectors_if_required(internal.registry.spaces, space_name, op)
    local collectors = internal.registry.spaces[space_name][op].details

    collectors.tuples_fetched = collectors.tuples_fetched + tuples_fetched
    collectors.tuples_lookup = collectors.tuples_lookup + tuples_lookup

    return true
end

--- Increase statistics of planned map reduces during select/pairs
--
-- @function observe_map_reduces
--
-- @number count
--  Count of map reduces planned.
--
-- @string space_name
--  Name of space.
--
-- @treturn boolean Returns true.
--
function registry.observe_map_reduces(count, space_name)
    dev_checks('number', 'string')

    local op = op_module.SELECT
    registry_utils.init_collectors_if_required(internal.registry.spaces, space_name, op)
    local collectors = internal.registry.spaces[space_name][op].details

    collectors.map_reduces = collectors.map_reduces + count

    return true
end

return registry
