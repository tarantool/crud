---- Internal module used to store statistics in `metrics` registry.
-- @module crud.stats.metrics_registry
--

local is_package, metrics = pcall(require, 'metrics')

local dev_checks = require('crud.common.dev_checks')
local op_module = require('crud.stats.operation')
local stash = require('crud.common.stash')
local registry_utils = require('crud.stats.registry_utils')

local registry = {}
-- Used to cache collectors.
local internal = stash.get(stash.name.stats_metrics_registry)

local metric_name = {
    -- Summary collector for all operations.
    stats = 'tnt_crud_stats',
    -- `*_count` and `*_sum` are automatically created
    -- by summary collector.
    stats_count = 'tnt_crud_stats_count',
    stats_sum = 'tnt_crud_stats_sum',

    -- Counter collectors for select/pairs details.
    details = {
        tuples_fetched = 'tnt_crud_tuples_fetched',
        tuples_lookup = 'tnt_crud_tuples_lookup',
        map_reduces = 'tnt_crud_map_reduces',
    }
}

local LATENCY_QUANTILE = 0.99

--- Check if application supports metrics rock for registry
--
--  `metrics >= 0.10.0` is required.
--  `metrics >= 0.9.0` is required to use summary quantiles with
--  age buckets. `metrics >= 0.5.0, < 0.9.0` is unsupported
--  due to quantile overflow bug
--  (https://github.com/tarantool/metrics/issues/235).
--  `metrics == 0.9.0` has bug that do not permits
--  to create summary collector without quantiles
--  (https://github.com/tarantool/metrics/issues/262).
--  In fact, user may use `metrics >= 0.5.0`, `metrics != 0.9.0`
--  if he wants to use metrics without quantiles, and `metrics >= 0.9.0`
--  if he wants to use metrics with quantiles. But this is confusing,
--  so we use a single restriction solving both cases.
--
-- @function is_supported
--
-- @treturn boolean Returns `true` if `metrics >= 0.10.0` found, `false` otherwise.
--
function registry.is_supported()
    if is_package == false then
        return false
    end

    -- Only metrics >= 0.10.0 supported.
    if metrics.unregister_callback == nil then
        return false
    end

    return true
end

--- Initialize collectors in global metrics registry
--
--  Registries are not meant to used explicitly
--  by users, init is not guaranteed to be idempotent.
--  Destroy collectors only through this registry methods.
--
-- @function init
--
-- @tab opts
--
-- @bool opts.quantiles
--  If `true`, computes latency as 0.99 quantile with aging.
--
-- @number[opt=1e-3] opts.quantile_tolerated_error
--  See metrics summary API for details:
--  https://www.tarantool.io/ru/doc/latest/book/monitoring/api_reference/#summary
--  If quantile value is -Inf, try to decrease quantile tolerated error.
--  See https://github.com/tarantool/metrics/issues/189 for issue details.
--
-- @number[opt=2] opts.quantile_age_buckets_count
--  Count of summary quantile buckets.
--  See tarantool/metrics summary API for details:
--  https://www.tarantool.io/ru/doc/latest/book/monitoring/api_reference/#summary
--  Increasing the value smoothes time window move,
--  but consumes additional memory and CPU.
--
-- @number[opt=60] opts.quantile_max_age_time
--  Duration of each bucket’s lifetime in seconds.
--  See tarantool/metrics summary API for details:
--  https://www.tarantool.io/ru/doc/latest/book/monitoring/api_reference/#summary
--  Smaller bucket lifetime results in smaller time window for quantiles,
--  but more CPU is spent on bucket rotation. If your application has low request
--  frequency, increase the value to reduce the amount of `-nan` gaps in quantile values.
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

    local quantile_params = nil
    local age_params = nil
    if opts.quantiles == true then
        quantile_params = {[LATENCY_QUANTILE] = opts.quantile_tolerated_error}
        age_params = {
            age_buckets_count = opts.quantile_age_buckets_count,
            max_age_time = opts.quantile_max_age_time,
        }
    end

    internal.registry = {}
    internal.registry[metric_name.stats] = metrics.summary(
        metric_name.stats,
        'CRUD router calls statistics',
        quantile_params,
        age_params)

    internal.registry[metric_name.details.tuples_fetched] = metrics.counter(
        metric_name.details.tuples_fetched,
        'Tuples fetched from CRUD storages during select/pairs')

    internal.registry[metric_name.details.tuples_lookup] = metrics.counter(
        metric_name.details.tuples_lookup,
        'Tuples looked up on CRUD storages while collecting response during select/pairs')

    internal.registry[metric_name.details.map_reduces] = metrics.counter(
        metric_name.details.map_reduces,
        'Map reduces planned during CRUD select/pairs')

    internal.opts = table.deepcopy(opts)

    return true
end

--- Unregister collectors in global metrics registry.
--
--  Registries are not meant to used explicitly
--  by users, destroy is not guaranteed to be idempotent.
--  Destroy collectors only through this registry methods.
--
-- @function destroy
--
-- @treturn boolean Returns `true`.
--
function registry.destroy()
    for _, c in pairs(internal.registry) do
        metrics.registry:unregister(c)
    end

    internal.registry = nil
    internal.opts = nil

    return true
end

--- Compute `latency_average` and set `latency` fields of each observation.
--
--  `latency` is `latency_average` if quantiles disabled
--  and `latency_quantile` otherwise.
--
-- @function compute_aggregates
-- @local
--
-- @tab stats
--  Object from registry_utils stats.
--
local function compute_aggregates(stats)
    for _, space_stats in pairs(stats.spaces) do
        for _, op_stats in pairs(space_stats) do
            for _, obs in pairs(op_stats) do
                -- There are no count in `details`.
                if obs.count ~= nil then
                    if obs.count == 0 then
                        obs.latency_average = 0
                    else
                        obs.latency_average = obs.time / obs.count
                    end

                    if obs.latency_quantile_recent ~= nil then
                        obs.latency = obs.latency_quantile_recent
                    else
                        obs.latency = obs.latency_average
                    end
                end
            end
        end
    end
end

--- Get copy of global metrics registry.
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
--  about all existing spaces, count of calls to spaces
--  that wasn't found and count of schema reloads.
--
-- @treturn table Returns copy of metrics registry.
function registry.get(space_name)
    dev_checks('?string')

    local stats = {
        spaces = {},
    }

    -- Fill operation basic statistics values.
    for _, obs in ipairs(internal.registry[metric_name.stats]:collect()) do
        local op = obs.label_pairs.operation
        local status = obs.label_pairs.status
        local name = obs.label_pairs.name

        if space_name ~= nil and name ~= space_name then
            goto stats_continue
        end

        registry_utils.init_collectors_if_required(stats.spaces, name, op)
        local space_stats = stats.spaces[name]

        -- metric_name.stats presents only if quantiles enabled.
        if obs.metric_name == metric_name.stats then
            if obs.label_pairs.quantile == LATENCY_QUANTILE then
                space_stats[op][status].latency_quantile_recent = obs.value
            end
        elseif obs.metric_name == metric_name.stats_sum then
            space_stats[op][status].time = obs.value
        elseif obs.metric_name == metric_name.stats_count then
            space_stats[op][status].count = obs.value
        end

        :: stats_continue ::
    end

    compute_aggregates(stats)

    -- Fill select/pairs detail statistics values.
    for stat_name, metric_name in pairs(metric_name.details) do
        for _, obs in ipairs(internal.registry[metric_name]:collect()) do
            local name = obs.label_pairs.name
            local op = obs.label_pairs.operation

            if space_name ~= nil and name ~= space_name then
                goto details_continue
            end

            registry_utils.init_collectors_if_required(stats.spaces, name, op)
            stats.spaces[name][op].details[stat_name] = obs.value

            :: details_continue ::
        end
    end

    if space_name ~= nil then
        return stats.spaces[space_name] or {}
    end

    return stats
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

    -- Use `operations` label to be consistent with `tnt_stats_op_*` labels.
    -- Use `name` label to be consistent with `tnt_space_*` labels.
    -- Use `status` label to be consistent with `tnt_vinyl_*` and HTTP metrics labels.
    local label_pairs = { operation = op, name = space_name, status = status }

    internal.registry[metric_name.stats]:observe(latency, label_pairs)

    return true
end

--- Increase statistics of storage select/pairs calls.
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
-- @treturn boolean Returns `true`.
--
function registry.observe_fetch(tuples_fetched, tuples_lookup, space_name)
    dev_checks('number', 'number', 'string')

    local label_pairs = { name = space_name, operation = op_module.SELECT }

    internal.registry[metric_name.details.tuples_fetched]:inc(tuples_fetched, label_pairs)
    internal.registry[metric_name.details.tuples_lookup]:inc(tuples_lookup, label_pairs)

    return true
end

--- Increase statistics of planned map reduces during select/pairs.
--
-- @function observe_map_reduces
--
-- @number count
--  Count of map reduces planned.
--
-- @string space_name
--  Name of space.
--
-- @treturn boolean Returns `true`.
--
function registry.observe_map_reduces(count, space_name)
    dev_checks('number', 'string')

    local label_pairs = { name = space_name, operation = op_module.SELECT }
    internal.registry[metric_name.details.map_reduces]:inc(count, label_pairs)

    return true
end

-- Workaround for https://github.com/tarantool/metrics/issues/334 .
-- This workaround does not prevent observations reset between role reloads,
-- but it fixes collector unlink from registry. Without this workaround,
-- we will continue to use cached collectors that are already cleaned up
-- from registry and changes will not appear in metrics export output.
local function workaround_role_reload()
    if not registry.is_supported() then
        return
    end

    -- Check if this registry was enabled before reload.
    if internal.registry == nil then
        return
    end

    -- Check if base collector is in metrics package registry.
    -- If it's not, then registry has beed cleaned up on role reload.
    if metrics.registry:find('summary', metric_name.stats) == nil then
        registry.init(internal.opts)
    end
end

workaround_role_reload()

return registry