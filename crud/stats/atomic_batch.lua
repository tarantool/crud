---- Internal module with collectors for `atomic_batch` sub-operation latency.
-- @module crud.stats.atomic_batch
--

local dev_checks = require('crud.common.dev_checks')
local registry_utils = require('crud.stats.registry_utils')

local atomic_batch = {}

--- Section name that stores per-sub-operation latency of `atomic_batch`.
atomic_batch.sub_ops_name = 'atomic_batch_sub_ops'

--- Initialize statistic collectors for an `atomic_batch` sub-operation.
--
-- Sub-operation latency is measured on storage around pure execution
-- of the corresponding DML operation (see `crud.atomic_batch`), so
-- these collectors are not nested into a per-space operation section,
-- but stored under the dedicated `atomic_batch.sub_ops_name` section.
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
--  Label of sub-operation collectors (see `crud.stats.op`).
--
function atomic_batch.init_collectors_if_required(spaces, space_name, op)
    dev_checks('table', 'string', 'string')

    if spaces[space_name] == nil then
        spaces[space_name] = {}
    end

    local space_collectors = spaces[space_name]
    if space_collectors[atomic_batch.sub_ops_name] == nil then
        space_collectors[atomic_batch.sub_ops_name] = {}
    end

    local sub_ops = space_collectors[atomic_batch.sub_ops_name]
    if sub_ops[op] == nil then
        sub_ops[op] = registry_utils.build_collectors(op)
    end
end

--- Compute `latency_average` and set `latency` fields for
-- `atomic_batch` sub-operation observations.
--
-- @function compute_aggregates
-- @local
--
-- @tab stats
--  Object from registry stats.
--
function atomic_batch.compute_aggregates(stats)
    for _, space_stats in pairs(stats.spaces) do
        local sub_ops = space_stats[atomic_batch.sub_ops_name]
        if sub_ops == nil then
            goto sub_ops_continue
        end

        for _, sub_op_stats in pairs(sub_ops) do
            for _, obs in pairs(sub_op_stats) do
                registry_utils.compute_observation_aggregates(obs)
            end
        end

        :: sub_ops_continue ::
    end
end

return atomic_batch
