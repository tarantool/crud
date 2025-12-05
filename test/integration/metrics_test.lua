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

pgroup.test_safe_mode_metrics = function(g)
    local has_metrics_module = require('metrics')
    t.skip_if(not has_metrics_module, 'No metrics module in current version')

    -- Check safe mode metric on storage
    helpers.call_on_storages(g.cluster, function(server)
        local observed = server:eval("return require('metrics').collect({ invoke_callbacks = true })")
        local has_metric = false
        for _, m in pairs(observed) do
            if m.metric_name == 'tnt_crud_storage_safe_mode_enabled' then
                t.assert_equals(m.value, 0, 'Metric shows safe mode disabled')
                has_metric = true
                break
            end
        end
        if not has_metric then
            t.fail('No tnt_crud_storage_safe_mode_enabled metric found')
        end
    end)

    -- Enable safe mode
    helpers.call_on_storages(g.cluster, function(server)
        server:call('_crud.rebalance_safe_mode_enable')
    end)

    -- Check that metric value has changed
    helpers.call_on_storages(g.cluster, function(server)
        local observed = server:eval("return require('metrics').collect({ invoke_callbacks = true })")
        local has_metric = false
        for _, m in pairs(observed) do
            if m.metric_name == 'tnt_crud_storage_safe_mode_enabled' then
                t.assert_equals(m.value, 1, 'Metric shows safe mode enabled')
                has_metric = true
                break
            end
        end
        if not has_metric then
            t.fail('No tnt_crud_storage_safe_mode_enabled metric found')
        end
    end)

    -- Check router cache metric
    local observed = g.router:eval("return require('metrics').collect({ invoke_callbacks = true })")
    local first_ts = 0
    for _, m in pairs(observed) do
        if m.metric_name == 'tnt_crud_router_cache_last_clear_ts' then
            first_ts = m.value
            break
        end
    end
    t.assert_gt(first_ts, 0, 'Last cache clear TS is greater than zero')

    -- Clear router cache
    g.router:eval("crud.rebalance.router_cache_clear()")

    -- Check that last_clear_ts has changed
    observed = g.router:eval("return require('metrics').collect({ invoke_callbacks = true })")
    local new_ts = 0
    for _, m in pairs(observed) do
        if m.metric_name == 'tnt_crud_router_cache_last_clear_ts' then
            new_ts = m.value
            break
        end
    end
    t.assert_gt(new_ts, first_ts, 'Last cache clear TS is greater than the first one')
end
