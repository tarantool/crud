local fiber = require('fiber')
local helpers = require('test.helper')
local t = require('luatest')

local pgroup = t.group('metrics_integration', helpers.backend_matrix({
    {engine = 'memtx'},
}))

local function before_all(g)
    helpers.start_default_cluster(g, 'srv_stats')
end

local function after_all(g)
    helpers.stop_cluster(g.cluster, g.params.backend)
end

local function before_each(g)
    g.router:eval("crud = require('crud')")
    helpers.call_on_storages(g.cluster, function(server)
        server:call('_crud.rebalance_safe_mode_disable')
    end)
end

pgroup.before_all(before_all)

pgroup.after_all(after_all)

pgroup.before_each(before_each)

pgroup.test_safe_mode_storage_metrics = function(g)
    local has_metrics_module = require('metrics')
    t.skip_if(not has_metrics_module, 'No metrics module in current version')

    -- Check no safe mode metric on router
    local observed = g.router:eval("return require('metrics').collect({ invoke_callbacks = true })")
    for _, m in pairs(observed) do
        if m.metric_name == 'tnt_crud_storage_safe_mode_enabled' then
            t.fail('tnt_crud_storage_safe_mode_enabled metric found on router')
        end
    end

    -- Check safe mode metric on storage
    helpers.call_on_storages(g.cluster, function(server)
        observed = server:eval("return require('metrics').collect({ invoke_callbacks = true })")
        local has_metric = false
        for _, m in pairs(observed) do
            if m.metric_name == 'tnt_crud_storage_safe_mode_enabled' then
                t.assert_equals(m.value, 0, 'Metric must show safe mode disabled')
                has_metric = true
                break
            end
        end
        if not has_metric then
            t.fail('No tnt_crud_storage_safe_mode_enabled metric found on storage')
        end
    end)

    -- Enable safe mode
    helpers.call_on_storages(g.cluster, function(server)
        server:call('_crud.rebalance_safe_mode_enable')
    end)

    -- Check no safe mode metric on router after changing safe mode
    observed = g.router:eval("return require('metrics').collect({ invoke_callbacks = true })")
    for _, m in pairs(observed) do
        if m.metric_name == 'tnt_crud_storage_safe_mode_enabled' then
            t.fail('tnt_crud_storage_safe_mode_enabled metric found on router')
        end
    end

    -- Check that metric value has changed
    helpers.call_on_storages(g.cluster, function(server)
        observed = server:eval("return require('metrics').collect({ invoke_callbacks = true })")
        local has_metric = false
        for _, m in pairs(observed) do
            if m.metric_name == 'tnt_crud_storage_safe_mode_enabled' then
                t.assert_equals(m.value, 1, 'Metric must show safe mode enabled')
                has_metric = true
                break
            end
        end
        if not has_metric then
            t.fail('No tnt_crud_storage_safe_mode_enabled metric found on storage')
        end
    end)
end

pgroup.test_router_cache_metrics = function(g)
    local has_metrics_module = require('metrics')
    t.skip_if(not has_metrics_module, 'No metrics module in current version')

    -- Check router cache metric initial value on router
    local observed = g.router:eval("return require('metrics').collect({ invoke_callbacks = true })")
    local has_metric = false
    for _, m in pairs(observed) do
        if m.metric_name == 'tnt_crud_router_cache_clear_ts' then
            t.assert_equals(m.value, 0, 'Cache never cleared')
            has_metric = true
            break
        end
    end
    if not has_metric then
        t.fail('No tnt_crud_router_cache_clear_ts metric found on router')
    end

    -- Check no router cache metric on storage
    helpers.call_on_storages(g.cluster, function(server)
        observed = server:eval("return require('metrics').collect({ invoke_callbacks = true })")
        for _, m in pairs(observed) do
            if m.metric_name == 'tnt_crud_router_cache_clear_ts' then
                t.fail('tnt_crud_router_cache_clear_ts metric found on storage')
            end
        end
    end)

    -- Clear router cache
    local expected_ts = fiber.time()
    g.router:call("crud.rebalance.router_cache_clear")

    -- Check router cache metric new value on router
    observed = g.router:eval("return require('metrics').collect({ invoke_callbacks = true })")
    has_metric = false
    for _, m in pairs(observed) do
        if m.metric_name == 'tnt_crud_router_cache_clear_ts' then
            t.assert_almost_equals(m.value, expected_ts, 5, 'Cache never cleared')
            has_metric = true
            break
        end
    end
    if not has_metric then
        t.fail('No tnt_crud_router_cache_clear_ts metric found on router')
    end

    -- Check no router cache metric appeared on storage
    helpers.call_on_storages(g.cluster, function(server)
        observed = server:eval("return require('metrics').collect({ invoke_callbacks = true })")
        for _, m in pairs(observed) do
            if m.metric_name == 'tnt_crud_router_cache_clear_ts' then
                t.fail('tnt_crud_router_cache_clear_ts metric found on storage')
            end
        end
    end)
end
