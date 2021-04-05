local fio = require('fio')

local t = require('luatest')

local helpers = require('test.helper')

local pgroup = helpers.pgroup.new('135_issue_test', {
    engine = {'memtx', 'vinyl'},
})

pgroup:set_before_all(function(g)
    g.cluster = helpers.Cluster:new({
        datadir = fio.tempdir(),
        server_command = helpers.entrypoint('135_issue_server'),
        use_vshard = true,
        replicasets = {
            {
                uuid = helpers.uuid('a'),
                alias = 'router',
                roles = { 'crud-router' },
                servers = {
                    { instance_uuid = helpers.uuid('a', 1), alias = 'router1' },
                    { instance_uuid = helpers.uuid('a', 2), alias = 'router2' },
                },
            },
            {
                uuid = helpers.uuid('b'),
                alias = 's-1',
                roles = { 'customers-storage', 'crud-storage' },
                servers = {
                    { instance_uuid = helpers.uuid('b', 1), alias = 's1-master' },
                    { instance_uuid = helpers.uuid('b', 2), alias = 's1-replica' },
                },
            },
            {
                uuid = helpers.uuid('c'),
                alias = 's-2',
                roles = { 'customers-storage', 'crud-storage' },
                servers = {
                    { instance_uuid = helpers.uuid('c', 1), alias = 's2-master' },
                    { instance_uuid = helpers.uuid('c', 2), alias = 's2-replica' },
                },
            },
            {
                uuid = helpers.uuid('d'),
                alias = 'router2',
                roles = { 'crud-router' },
                servers = {
                    { instance_uuid = helpers.uuid('d', 1), alias = 'another-router' },
                },
            },
        },
        env = {
            ['ENGINE'] = g.params.engine,
        },
    })

    g.cluster:start()
end)

pgroup:set_after_all(function(g) helpers.stop_cluster(g.cluster) end)


pgroup:add('test_auth_templates', function(g)
    local templates = g.cluster.main_server.net_box:call('crud.insert', {'authTemplates',
        {"79774120882","app4t2","671418",3,1716591611,1716592211}})
    t.assert_not_equals(templates, nil)

    local router1 = g.cluster:server('router1')
    local router2 = g.cluster:server('router2')
    for i=1,1e4 do
    local obj, err = router1.net_box:call(
       'crud.select', {'authTemplates', {{'==', 'authTemplates_msisdn_channel_idx', {'79774120882', 'app4t2'}}}})
    t.assert_not_equals(obj, nil, err)
    t.assert_not_equals(#obj['rows'], 0, obj)
    t.assert_equals(err, nil, err)

    local obj, err = router1.net_box:call(
       'crud.select', {'authTemplates', {{'==', 'authTemplates_msisdn_channel_idx', {'79774120882'}}}})
    t.assert_not_equals(obj, nil, err)
    t.assert_not_equals(#obj['rows'], 0, obj)
    t.assert_equals(err, nil, err)


    local obj, err = router2.net_box:call(
       'crud.select', {'authTemplates', {{'==', 'authTemplates_msisdn_channel_idx', {'79774120882', 'app4t2'}}}})
    t.assert_not_equals(obj, nil, err)
    t.assert_not_equals(#obj['rows'], 0, obj)
    t.assert_equals(err, nil, err)

    local obj, err = router2.net_box:call(
       'crud.select', {'authTemplates', {{'==', 'authTemplates_msisdn_channel_idx', {'79774120882'}}}})
    t.assert_not_equals(obj, nil)
    t.assert_not_equals(#obj['rows'], 0, obj)
    t.assert_equals(err, nil, err)
    end
end)
