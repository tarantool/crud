local t = require('luatest')

local helpers = require('test.helper')

local fiber = require("fiber")

local pgroup = t.group('storage_info', helpers.backend_matrix({
    {engine = 'memtx'}
}))

-- Waits for storages to initialize.
-- This is a workaround for "peer closed" errors for some connections right after the cluster start.
-- Retry is required to give a small timeout to reconnect.
local function wait_storages_init(g)
    local storages_initialized = false
    local attempts_left = 5
    local wait_for_init_timeout = 1
    while (attempts_left > 0 and not storages_initialized) do
        local results, err = g.router:call("crud.storage_info", {})
        t.assert_equals(err, nil, "Error getting storage status")
        storages_initialized = true
        local count = 0
        for _, v in pairs(results) do
            count = count + 1
            if v.status ~= "running" then
                storages_initialized = false
            end
        end
        if count ~= 4 then -- Make sure the results count is equal to the cluster instances count.
            return false
        end
        if not storages_initialized then
            fiber.sleep(wait_for_init_timeout)
            attempts_left = attempts_left - 1
        end
    end
    return storages_initialized
end

pgroup.before_all(function(g)
    helpers.start_default_cluster(g, 'srv_select')
end)

pgroup.before_each(function(g)
    t.assert_equals(wait_storages_init(g), true)
end)

pgroup.after_all(function(g)
    helpers.stop_cluster(g.cluster, g.params.backend)
end)

local function build_storage_info(g, array_info)
    local is_vshard = g.params.backend == 'vshard'
    local is_config = g.params.backend == 'config'

    local name_as_key = is_vshard and (
        type(g.params.backend_cfg) == 'table'
        and g.params.backend_cfg.identification_mode == 'name_as_key'
    ) or is_config

    local keys
    if name_as_key then
        keys = {
            's1-master',
            's1-replica',
            's2-master',
            's2-replica',
        }
    else
        keys = {
            helpers.uuid('b', 1),
            helpers.uuid('b', 10),
            helpers.uuid('c', 1),
            helpers.uuid('c', 10),
        }
    end

    local res = {}
    for i, v in ipairs(array_info) do
        res[keys[i]] = v
    end

    return res
end

local function ordered_keys_for_results(g)
    local is_vshard = g.params.backend == 'vshard'
    local is_config = g.params.backend == 'config'

    local name_as_key = is_vshard and (
        type(g.params.backend_cfg) == 'table'
        and g.params.backend_cfg.identification_mode == 'name_as_key'
    ) or is_config

    if name_as_key then
        return {
            's1-master',
            's1-replica',
            's2-master',
            's2-replica',
        }
    end

    return {
        helpers.uuid('b', 1),
        helpers.uuid('b', 10),
        helpers.uuid('c', 1),
        helpers.uuid('c', 10),
    }
end

pgroup.test_crud_storage_status_of_stopped_servers = function(g)
    g.cluster:server("s2-replica"):stop()

    local results, err = g.router:call("crud.storage_info", {})
    t.assert_equals(err, nil, "Error getting storages info")

    local keys = ordered_keys_for_results(g)

    local expected = {
        [keys[1]] = { status = "running", is_master = true  },
        [keys[2]] = { status = "running", is_master = false },
        [keys[3]] = { status = "running", is_master = true  },
        [keys[4]] = { status = "error",   is_master = false },
    }

    for _, k in ipairs(keys) do
        local got = results[k]
        t.assert_not_equals(got, nil, ("No result for key %s"):format(k))
        t.assert_equals(got.status,   expected[k].status,   ("status mismatch for %s"):format(k))
        t.assert_equals(got.is_master, expected[k].is_master, ("is_master mismatch for %s"):format(k))
    end
end
pgroup.after_test('test_crud_storage_status_of_stopped_servers', function(g)
    helpers.stop_cluster(g.cluster, g.params.backend)
    g.cluster = nil
    helpers.start_default_cluster(g, 'srv_select')
end)

pgroup.test_disabled_storage_role = function(g)
    helpers.skip_not_cartridge_backend(g.params.backend)

    -- stop crud storage role on one replica
    local server = g.cluster:server("s1-replica")
    local results = server.net_box:eval([[
        local serviceregistry = require("cartridge.service-registry")
        serviceregistry.get("crud-storage").stop()
        return true
    ]])

    t.assert_not_equals(results, nil, "Failed to disable storage role")

    local results, err = g.router:call("crud.storage_info", {})
    t.assert_equals(err, nil, "Error getting storages info")

    t.assert_equals(results, build_storage_info(g, {
        {
            status = "running",
            is_master = true
        },
        {
            status = "uninitialized",
            is_master = false
        },
        {
            status = "running",
            is_master = true
        },
        {
            status = "running",
            is_master = false,
        },
    }))
end

pgroup.after_test('test_disabled_storage_role', function(g)
    g.cluster:server("s1-replica").net_box:eval([[
        local serviceregistry = require("cartridge.service-registry")
        serviceregistry.get("crud-storage").init()
        return true
    ]])
end)

pgroup.test_storage_call_failure = function(g)
    -- stop crud storage role on one replica
    local server = g.cluster:server("s2-replica")
    local results = server.net_box:eval([[
        _G.saved_storage_info_on_storage = _crud.storage_info_on_storage
        _crud.storage_info_on_storage = {}
        return true
    ]])

    t.assert_not_equals(results, nil, "Eval failed")

    local results, err = g.router:call("crud.storage_info", {})
    t.assert_equals(err, nil, "Error getting storages info")
    t.assert_equals(results, build_storage_info(g, {
        {
            status = "running",
            is_master = true
        },
        {
            status = "running",
            is_master = false
        },
        {
            status = "running",
            is_master = true
        },
        {
            status = "error",
            is_master = false,
            message = "attempt to call a table value"
        },
    }))
end

pgroup.after_test('test_storage_call_failure', function(g)
    g.cluster:server("s2-replica").net_box:eval([[
        _crud.storage_info_on_storage = _G.saved_storage_info_on_storage
        _G.saved_storage_info_on_storage = nil
        return true
    ]])
end)
