local fio = require('fio')

local t = require('luatest')

local helpers = require('test.helper')

local pgroup = t.group('readview_not_supported', {
    {engine = 'memtx'},
})


pgroup.before_all(function(g)
    if helpers.tarantool_version_at_least(2, 11, 0)
    and require('luatest.tarantool').is_enterprise_package() then
        t.skip("Readview is supported")
    end
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

pgroup.after_all(function(g) helpers.stop_cluster(g.cluster) end)

pgroup.test_open = function(g)
    local obj, err = g.cluster.main_server.net_box:eval([[
        local crud = require('crud')
        local foo, err = crud.readview({name = 'foo'})
        return foo, err
    ]])

    t.assert_equals(obj, nil)
    t.assert_str_contains(err.str, 'Tarantool does not support readview')

end
