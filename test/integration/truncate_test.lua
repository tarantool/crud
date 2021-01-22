local fio = require('fio')

local t = require('luatest')

local helpers = require('test.helper')

local pgroup = helpers.pgroup.new('truncate', {
    engine = {'memtx', 'vinyl'},
})

pgroup:set_before_all(function(g)
    g.cluster = helpers.Cluster:new({
        datadir = fio.tempdir(),
        server_command = helpers.entrypoint('srv_select'),
        use_vshard = true,
        replicasets = helpers.get_test_replicasets(),
        env = {
            ['ENGINE'] = g.params.engine,
        },
    })

    g.cluster:start()

    g.space_format = g.cluster.servers[2].net_box.space.customers:format()
end)

pgroup:set_after_all(function(g) helpers.stop_cluster(g.cluster) end)

pgroup:set_before_each(function(g)
    helpers.truncate_space_on_cluster(g.cluster, 'customers')
end)


pgroup:add('test_non_existent_space', function(g)
    -- insert
    local obj, err = g.cluster.main_server.net_box:call(
       'crud.truncate', {'non_existent_space'}
    )

    t.assert_equals(obj, nil)
    t.assert_str_contains(err.err, "Space \"non_existent_space\" doesn't exist")
end)

pgroup:add('test_truncate', function(g)
    local customers = helpers.insert_objects(g, 'customers', {
        {
            id = 1, name = "Elizabeth", last_name = "Jackson",
            age = 12, city = "New York",
        }, {
            id = 2, name = "Mary", last_name = "Brown",
            age = 46, city = "Los Angeles",
        }, {
            id = 3, name = "David", last_name = "Smith",
            age = 33, city = "Los Angeles",
        }, {
            id = 4, name = "William", last_name = "White",
            age = 81, city = "Chicago",
        },
    })

    table.sort(customers, function(obj1, obj2) return obj1.id < obj2.id end)

    local result, err = g.cluster.main_server.net_box:call('crud.select', {'customers', nil})
    t.assert_equals(err, nil)
    t.assert(#result.rows > 0)

    local result, err = g.cluster.main_server.net_box:call('crud.truncate', {'customers'})
    t.assert_equals(err, nil)
    t.assert_equals(result, true)

    local result, err = g.cluster.main_server.net_box:call('crud.select', {'customers', nil})
    t.assert_equals(err, nil)
    t.assert(#result.rows == 0)
end)
