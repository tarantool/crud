local fiber = require('fiber')
local helpers = require('test.helper')
local t = require('luatest')

local pgroup = t.group('metrics_integration', helpers.backend_matrix({
    {engine = 'memtx'},
}))

local function before_all(g)
    -- Disable checks to avoid 'fiber is not registered' error.
    g.old_dev_checks_value = os.getenv('TARANTOOL_CRUD_ENABLE_INTERNAL_CHECKS')
    helpers.disable_dev_checks()
    helpers.start_default_cluster(g, 'srv_stats')
end

local function after_all(g)
    helpers.stop_cluster(g.cluster, g.params.backend)
    os.setenv('TARANTOOL_CRUD_ENABLE_INTERNAL_CHECKS', g.old_dev_checks_value)
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

--- Perform operations with nil bucket_id (emulating old router < 1.7.0).
pgroup.test_nil_bucket_id_compat_metric = function(g)
    local has_metrics_module = pcall(require, 'metrics')
    t.skip_if(not has_metrics_module, 'No metrics module in current version')

    local storage = g.cluster:server('s1-master')

    local common_opts = {bucket_id = nil, skip_sharding_hash_check = true}
    local ops = {
        {
            name = 'get',
            args = {'customers', 1, nil, common_opts}
        },
        {
            name = 'update',
            args = {'customers', 1, {{'=', 'name', 'Test'}}, common_opts}
        },
        {
            name = 'delete',
            args = {'customers', 1, common_opts}
        },
    }

    for _, op in ipairs(ops) do
        storage:call('_crud.' .. op.name .. '_on_storage', op.args)
    end

    local observed = storage:eval([[
        return require('metrics').collect({ invoke_callbacks = true })
    ]])

    local found_metrics = {}
    for _, m in pairs(observed) do
        if m.metric_name == 'tnt_crud_storage_nil_bucket_id_compat_total' then
            table.insert(found_metrics, m)
        end
    end

    local function get_metric_value_by_label(metrics_list, op_name, engine_name)
        for _, m in ipairs(metrics_list) do
            if m.label_pairs.operation == op_name and m.label_pairs.engine == engine_name then
                return m.value
            end
        end
        return nil
    end

    local engine = 'memtx'

    local get_val = get_metric_value_by_label(found_metrics, 'get', engine)
    local update_val = get_metric_value_by_label(found_metrics, 'update', engine)
    local delete_val = get_metric_value_by_label(found_metrics, 'delete', engine)

    t.assert_equals(get_val, 1)
    t.assert_equals(update_val, 1)
    t.assert_equals(delete_val, 1)
end
