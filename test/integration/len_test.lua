local fio = require('fio')

local t = require('luatest')

local helpers = require('test.helper')

local pgroup = helpers.pgroup.new('len', {
    engine = {'memtx'},
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
end)

pgroup:set_after_all(function(g) helpers.stop_cluster(g.cluster) end)

pgroup:set_before_each(function(g)
    helpers.truncate_space_on_cluster(g.cluster, 'customers')
end)

pgroup:add('test_len_non_existent_space', function(g)
    local result, err = g.cluster.main_server.net_box:call('crud.len', {'non_existent_space'})

    t.assert_equals(result, nil)
    t.assert_str_contains(err.err, "Space \"non_existent_space\" doesn't exist")
end)

pgroup:add('test_len', function(g)
    local bucket_id = 1
    local other_bucket_id, err = helpers.get_other_storage_bucket_id(g.cluster, bucket_id)
    t.assert(other_bucket_id ~= nil, err)

    -- let's insert five tuples on different replicasets
    -- (two tuples on one replica and three on the other)
    -- to check that  the total length will be calculated on the router
    helpers.insert_objects(g, 'customers', {
        {
            id = 1, name = "Elizabeth", last_name = "Jackson",
            age = 33, city = "New York",
            bucket_id = bucket_id,
        }, {
            id = 2, name = "Mary", last_name = "Brown",
            age = 33, city = "Los Angeles",
            bucket_id = other_bucket_id,
        },  {
            id = 3, name = "David", last_name = "Smith",
            age = 33, city = "Los Angeles",
            bucket_id = bucket_id
        }, {
            id = 4, name = "William", last_name = "White",
            age = 81, city = "Chicago",
            bucket_id = bucket_id
        }, {
            id = 5, name = "John", last_name = "May",
            age = 38, city = "New York",
            bucket_id = other_bucket_id
        },
    })

    local result, err = g.cluster.main_server.net_box:call('crud.len', {'customers'})

    t.assert_equals(err, nil)
    t.assert_equals(result, 5)
end)

pgroup:add('test_len_empty_space', function(g)
    local result, err = g.cluster.main_server.net_box:call('crud.len', {'customers'})

    t.assert_equals(err, nil)
    t.assert_equals(result, 0)
end)
