local fio = require('fio')

local t = require('luatest')

local helpers = require('test.helper')

local ok = pcall(require, 'ddl')
if not ok then
    t.skip('Lua module ddl is required to run test')
end

local pgroup = t.group('ddl_sharding_key', {
    {engine = 'memtx'},
    {engine = 'vinyl'},
})

pgroup.before_all(function(g)
    g.cluster = helpers.Cluster:new({
        datadir = fio.tempdir(),
        server_command = helpers.entrypoint('srv_ddl'),
        use_vshard = true,
        replicasets = helpers.get_test_replicasets(),
        env = {
            ['ENGINE'] = g.params.engine,
        },
    })
    g.cluster:start()
    local result, err = g.cluster.main_server.net_box:eval([[
        local ddl = require('ddl')

        local ok, err = ddl.get_schema()
        return ok, err
    ]])
    t.assert_equals(type(result), 'table')
    t.assert_equals(err, nil)
end)

pgroup.after_all(function(g) helpers.stop_cluster(g.cluster) end)

pgroup.before_each(function(g)
    helpers.truncate_space_on_cluster(g.cluster, 'customers_name_key')
    helpers.truncate_space_on_cluster(g.cluster, 'customers_name_key_uniq_index')
    helpers.truncate_space_on_cluster(g.cluster, 'customers_name_key_non_uniq_index')
    helpers.truncate_space_on_cluster(g.cluster, 'customers_secondary_idx_name_key')
end)

pgroup.test_select = function(g)
    local tuple = {2, box.NULL, 'Ivan', 20}

    -- insert tuple
    local result, err = g.cluster.main_server.net_box:call('crud.insert', {
        'customers_name_key', tuple
    })

    t.assert_equals(err, nil)
    t.assert_not_equals(result, nil)
    t.assert_equals(#result.rows, 1)

    local conditions = {{'==', 'id', 2}}

    local result, err = g.cluster.main_server.net_box:call('crud.select', {
        'customers_name_key', conditions,
    })

    t.assert_equals(err, nil)
    t.assert_not_equals(result, nil)
    t.assert_equals(#result.rows, 1)
end

-- Right now CRUD's plan for select doesn't support sharding key and it leads
-- to map reduce (select on all replicasets). To avoid map-reduce one need to
-- add a separate index by field name, used in select's condition. We plan to
-- fix this in scope of https://github.com/tarantool/crud/issues/213
pgroup.test_select_wont_lead_map_reduce = function(g)
    local space_name = 'customers_name_key_uniq_index'
    local customers = helpers.insert_objects(g, space_name, {
        {id = 1, name = 'Viktor Pelevin', age = 58},
        {id = 2, name = 'Isaac Asimov', age = 72},
        {id = 3, name = 'Aleksandr Solzhenitsyn', age = 89},
        {id = 4, name = 'James Joyce', age = 59},
        {id = 5, name = 'Oscar Wilde', age = 46},
        {id = 6, name = 'Ivan Bunin', age = 83},
        {id = 7, name = 'Ivan Turgenev', age = 64},
        {id = 8, name = 'Alexander Ostrovsky', age = 63},
        {id = 9, name = 'Anton Chekhov', age = 44},
    })
    t.assert_equals(#customers, 9)

    -- Disable vshard's rebalancer and account current statistics of SELECT
    -- calls on storages before calling CRUD select. Rebalancer may screw up
    -- statistics of SELECT calls, so we will disable it.
    local servers = g.cluster.servers
    local select_total_counter_before = 0
    for n, _ in ipairs(servers) do
        local c = g.cluster.servers[n].net_box:eval([[
            local vshard = require('vshard')
            vshard.storage.rebalancer_disable()
            assert(vshard.storage.sync(2) == true)
            assert(vshard.storage.rebalancing_is_in_progress() == false)

            return box.stat().SELECT.total
        ]])
        select_total_counter_before = select_total_counter_before + c
    end

    -- Make a CRUD's SELECT.
    local result, err = g.cluster.main_server.net_box:call('crud.select', {
        space_name, {{'==', 'name', 'Anton Chekhov'}}
    })
    t.assert_equals(err, nil)
    t.assert_not_equals(result, nil)
    t.assert_equals(#result.rows, 1)

    -- Enable vshard's rebalancer and account current statistics of SELECT
    -- calls on storages after calling CRUD select.
    local select_total_counter_after = 0
    for n, _ in ipairs(servers) do
        local c = g.cluster.servers[n].net_box:eval([[
            local vshard = require('vshard')
            local stat = box.stat().SELECT.total
            vshard.storage.rebalancer_enable()

            return stat
        ]])
        select_total_counter_after = select_total_counter_after + c
    end

    -- Compare total counters of SELECT calls on cluster's storages before and
    -- after calling SELECT on router. Make sure no more than 1 storage changed
    -- SELECT counter. Otherwise we lead map reduce.
    local diff = select_total_counter_after - select_total_counter_before
    t.assert_le(diff, 4)
    t.assert_ge(diff, 2)
end

pgroup.test_select_secondary_idx = function(g)
    local tuple = {2, box.NULL, 'Ivan', 20}

    -- insert tuple
    local result, err = g.cluster.main_server.net_box:call('crud.insert', {
        'customers_secondary_idx_name_key', tuple
    })

    t.assert_equals(err, nil)
    t.assert_not_equals(result, nil)
    t.assert_equals(#result.rows, 1)

    local conditions = {{'==', 'name', 'Ivan'}}

    local result, err = g.cluster.main_server.net_box:call('crud.select', {
        'customers_secondary_idx_name_key', conditions,
    })

    t.assert_equals(err, nil)
    t.assert_not_equals(result, nil)
    t.assert_equals(#result.rows, 1)
end

pgroup.test_non_unique_index = function(g)
    local space_name = 'customers_name_key_non_uniq_index'
    local customers = helpers.insert_objects(g, space_name, {
        {id = 1, name = 'Viktor Pelevin', age = 58},
        {id = 2, name = 'Isaac Asimov', age = 72},
        {id = 3, name = 'Aleksandr Solzhenitsyn', age = 89},
        {id = 4, name = 'James Joyce', age = 59},
        {id = 5, name = 'Oscar Wilde', age = 46},
        {id = 6, name = 'Ivan Bunin', age = 83},
        {id = 7, name = 'Ivan Turgenev', age = 64},
        {id = 8, name = 'Alexander Ostrovsky', age = 63},
        {id = 9, name = 'Anton Chekhov', age = 44},
        {id = 10, name = 'Ivan Bunin', age = 83},
    })
    t.assert_equals(#customers, 10)

    local result, err = g.cluster.main_server.net_box:call('crud.select', {
        space_name, {{'==', 'name', 'Ivan Bunin'}}
    })
    t.assert_equals(err, nil)
    t.assert_not_equals(result, nil)
    t.assert_equals(#result.rows, 2)
end
