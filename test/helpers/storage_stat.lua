local checks = require('checks')
local helpers = require('test.helper')

local storage_stat = {}

-- Wrap crud's select_on_storage()/count_on_storage()
-- function to count selects/counts and add storage_stat()
-- function that returns resulting statistics.
--
-- Call it after crud's initialization.
function storage_stat.init_on_storage(method_on_storage_name)
    assert(_G._crud[method_on_storage_name] ~= nil)

    -- Here we count requests.
    local storage_stat_table = {
        requests = 0,
    }

    -- Wrap method on storage function.
    local requests_on_storage_saved = _G._crud[method_on_storage_name]
    _G._crud[method_on_storage_name] = function(...)
        local requests = storage_stat_table.requests
        storage_stat_table.requests = requests + 1
        return requests_on_storage_saved(...)
    end

    -- Accessor for the statistics.
    rawset(_G, 'storage_stat', function()
        return storage_stat_table
    end)
end

function storage_stat.init_on_storage_for_select()
    storage_stat.init_on_storage('select_on_storage')
end

function storage_stat.init_on_storage_for_count()
    storage_stat.init_on_storage('count_on_storage')
end

-- Accumulate statistics from storages.
--
-- The statistics is grouped by replicasets.
--
-- Example of a return value:
--
--  | {
--  |     ['s-1'] = {
--  |         select_requests = 1,
--  |     },
--  |     ['s-2'] = {
--  |         select_requests = 0,
--  |     },
--  | }
function storage_stat.collect(cluster)
    checks('table')

    local res = {}

    helpers.call_on_storages(cluster, function(server, replicaset)
        checks('table', 'table')

        -- Collect the statistics.
        local storage_stat = server.net_box:call('storage_stat')

        -- Initialize if needed.
        if res[replicaset.alias] == nil then
            res[replicaset.alias] = {}
        end

        -- Accumulate the collected statistics.
        for key, val in pairs(storage_stat) do
            local old = res[replicaset.alias][key] or 0
            res[replicaset.alias][key] = old + val
        end
    end)

    return res
end

-- Difference between 'a' and 'b' storage statistics.
--
-- The return value structure is the same as for
-- storage_stat.collect().
function storage_stat.diff(a, b)
    checks('table', 'table')

    local diff = table.deepcopy(a)

    for replicaset_alias, stat_b in pairs(b) do
        -- Initialize if needed.
        if diff[replicaset_alias] == nil then
            diff[replicaset_alias] = {}
        end

        -- Substract 'b' statistics from 'a'.
        for key, val in pairs(stat_b) do
            local old = diff[replicaset_alias][key] or 0
            diff[replicaset_alias][key] = old - val
        end
    end

    return diff
end

-- Accepts collect (or diff) return value and returns
-- total number of select/count requests across all storages.
function storage_stat.total(stats)
    local total = 0

    for _, stat in pairs(stats) do
        total = total + (stat.requests or 0)
    end

    return total
end

return storage_stat
