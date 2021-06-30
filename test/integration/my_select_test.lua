local fio = require('fio')

local t = require('luatest')

local crud = require('crud')
local crud_utils = require('crud.common.utils')

local helpers = require('test.helper')

local pgroup = helpers.pgroup.new('select', {
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
    helpers.truncate_space_on_cluster(g.cluster, 'developers')
    helpers.truncate_space_on_cluster(g.cluster, 'cars')
end)

pgroup:add('test_multipart_primary_index', function(g)
    local coords = helpers.insert_objects(g, 'coord', {
        { x = 0, y = 0 }, -- 1
        { x = 0, y = 1 }, -- 2
        { x = 0, y = 2 }, -- 3
        { x = 1, y = 3 }, -- 4
        { x = 1, y = 4 }, -- 5
    })

    local conditions = {{'=', 'primary', 0}}
    local result_0, err = g.cluster.main_server.net_box:call('crud.select', {'coord', conditions})
    t.assert_equals(err, nil)
    local objects = crud.unflatten_rows(result_0.rows, result_0.metadata)
    t.assert_equals(objects, helpers.get_objects_by_idxs(coords, {1, 2, 3}))

    local result, err = g.cluster.main_server.net_box:call('crud.select', {'coord', conditions,
                                                                           {after = result_0.rows[1]}})
    t.assert_equals(err, nil)
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, helpers.get_objects_by_idxs(coords, {2, 3}))

    local result, err = g.cluster.main_server.net_box:call('crud.select', {'coord', conditions,
                                                                           {after = result_0.rows[3], first = -2}})
    t.assert_equals(err, nil)
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, helpers.get_objects_by_idxs(coords, {1, 2}))

    local conditions = {{'=', 'primary', {0, 2}}}
    local result, err = g.cluster.main_server.net_box:call('crud.select', {'coord', conditions})
    t.assert_equals(err, nil)
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, helpers.get_objects_by_idxs(coords, {3}))

    local conditions_ge = {{'>=', 'primary', 0}}
    local result_ge_0, err = g.cluster.main_server.net_box:call('crud.select', {'coord', conditions_ge})
    t.assert_equals(err, nil)
    local objects = crud.unflatten_rows(result_ge_0.rows, result_ge_0.metadata)
    t.assert_equals(objects, helpers.get_objects_by_idxs(coords, {1, 2, 3, 4, 5}))

    local result, err = g.cluster.main_server.net_box:call('crud.select', {'coord', conditions_ge,
                                                                           {after = result_ge_0.rows[1]}})
    t.assert_equals(err, nil)
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, helpers.get_objects_by_idxs(coords, {2, 3, 4, 5}))

    local result, err = g.cluster.main_server.net_box:call('crud.select', {'coord', conditions_ge,
                                                                           {after = result_ge_0.rows[3], first = -3}})
    t.assert_equals(err, nil)
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, helpers.get_objects_by_idxs(coords, {1, 2}))
end)