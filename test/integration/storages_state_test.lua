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
        local results, err = g.cluster.main_server.net_box:call("crud.storage_info", {})
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

pgroup.test_crud_storage_status_of_stopped_servers = function(g)
    g.cluster:server("s2-replica"):stop()

    local results, err = g.cluster.main_server.net_box:call("crud.storage_info", {})
    t.assert_equals(err, nil, "Error getting storages info")
    t.assert_equals(results, {
        [helpers.uuid('b', 1)] = {
            status = "running",
            is_master = true
        },
        [helpers.uuid('b', 10)] = {
            status = "running",
            is_master = false
        },
        [helpers.uuid('c', 1)] = {
            status = "running",
            is_master = true
        },
        [helpers.uuid('c', 10)] = {
            status = "error",
            is_master = false,
            message = "Peer closed"
        }
    })
end

pgroup.after_test('test_crud_storage_status_of_stopped_servers', function(g)
    g.cluster:server("s2-replica"):start()
    g.cluster:server("s2-replica"):exec(function()
        require('crud').init_storage()
    end)
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

    local results, err = g.cluster.main_server.net_box:call("crud.storage_info", {})
    t.assert_equals(err, nil, "Error getting storages info")

    t.assert_equals(results, {
        [helpers.uuid('b', 1)] = {
            status = "running",
            is_master = true
        },
        [helpers.uuid('b', 10)] = {
            status = "uninitialized",
            is_master = false
        },
        [helpers.uuid('c', 1)] = {
            status = "running",
            is_master = true
        },
        [helpers.uuid('c', 10)] = {
            status = "running",
            is_master = false
        }
    })
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

    local results, err = g.cluster.main_server.net_box:call("crud.storage_info", {})
    t.assert_equals(err, nil, "Error getting storages info")

    t.assert_equals(results, {
        [helpers.uuid('b', 1)] = {
            status = "running",
            is_master = true
        },
        [helpers.uuid('b', 10)] = {
            status = "running",
            is_master = false
        },
        [helpers.uuid('c', 1)] = {
            status = "running",
            is_master = true
        },
        [helpers.uuid('c', 10)] = {
            status = "error",
            is_master = false,
            message = "attempt to call a table value"
        }
    })
end

pgroup.after_test('test_storage_call_failure', function(g)
    g.cluster:server("s2-replica").net_box:eval([[
        _crud.storage_info_on_storage = _G.saved_storage_info_on_storage
        _G.saved_storage_info_on_storage = nil
        return true
    ]])
end)
