---- Internal module used by statistics registries.
-- @module crud.stats.registry_utils
--

local dev_checks = require('crud.common.dev_checks')
local op_module = require('crud.stats.operation')

local registry_utils = {}

--- Build collectors for local registry.
--
-- @function build_collectors
--
-- @string op
--  Label of registry collectors.
--  Use `require('crud.stats').op` to pick one.
--
-- @treturn table Returns collectors for success and error requests.
--  Collectors store 'count', 'latency', 'latency_average',
--  'latency_quantile_recent' and 'time' values. Also
--  returns additional collectors for select operation.
--
function registry_utils.build_collectors(op)
    dev_checks('string')

    local collectors = {
        ok = {
            count = 0,
            latency = 0,
            latency_average = 0,
            -- latency_quantile_recent presents only if driver
            -- is 'metrics' and quantiles enabled.
            latency_quantile_recent = nil,
            time = 0,
        },
        error = {
            count = 0,
            latency = 0,
            latency_average = 0,
            -- latency_quantile_recent presents only if driver
            -- is 'metrics' and quantiles enabled.
            latency_quantile_recent = nil,
            time = 0,
        },
    }

    if op == op_module.SELECT then
        collectors.details = {
            tuples_fetched = 0,
            tuples_lookup = 0,
            map_reduces = 0,
        }
    end

    return collectors
end

--- Initialize all statistic collectors for a space operation.
--
-- @function init_collectors_if_required
--
-- @tab spaces
--  `spaces` section of registry.
--
-- @string space_name
--  Name of space.
--
-- @string op
--  Label of registry collectors.
--  Use `require('crud.stats').op` to pick one.
--
function registry_utils.init_collectors_if_required(spaces, space_name, op)
    dev_checks('table', 'string', 'string')

    if spaces[space_name] == nil then
        spaces[space_name] = {}
    end

    local space_collectors = spaces[space_name]
    if space_collectors[op] == nil then
        space_collectors[op] = registry_utils.build_collectors(op)
    end
end

--- Compute `latency_average` and set `latency` field of an observation.
--
--  `latency` is `latency_average` if quantiles are disabled
--  and `latency_quantile_recent` otherwise.
--
-- @function compute_observation_aggregates
--
-- @tab obs
--  Observation with `count`, `time` and, optionally,
--  `latency_quantile_recent` fields.
--
function registry_utils.compute_observation_aggregates(obs)
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

return registry_utils
