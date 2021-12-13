---- Internal module used by statistics registries.
-- @module crud.stats.registry_utils
--

local dev_checks = require('crud.common.dev_checks')

local registry_utils = {}

--- Build collectors for local registry.
--
-- @function build_collectors
--
-- @treturn table Returns collectors for success and error requests.
--  Collectors store 'count', 'latency' and 'time' values.
--
function registry_utils.build_collectors()
    local collectors = {
        ok = {
            count = 0,
            latency = 0,
            time = 0,
        },
        error = {
            count = 0,
            latency = 0,
            time = 0,
        },
    }

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
        space_collectors[op] = registry_utils.build_collectors()
    end
end

return registry_utils
