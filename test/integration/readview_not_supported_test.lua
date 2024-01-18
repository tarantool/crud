local t = require('luatest')

local helpers = require('test.helper')

local pgroup = t.group('readview_not_supported', helpers.backend_matrix({
    {engine = 'memtx'},
}))


pgroup.before_all(function(g)
    if helpers.tarantool_version_at_least(2, 11, 0)
    and require('luatest.tarantool').is_enterprise_package() then
        t.skip("Readview is supported")
    end

    helpers.start_default_cluster(g, 'srv_select')

    g.space_format = g.cluster.servers[2].net_box.space.customers:format()
end)

pgroup.after_all(function(g)
    helpers.stop_cluster(g.cluster, g.params.backend)
end)

pgroup.test_open = function(g)
    local obj, err = g.router:eval([[
        local crud = require('crud')
        local foo, err = crud.readview({name = 'foo'})
        return foo, err
    ]])

    t.assert_equals(obj, nil)
    t.assert_str_contains(err.str, 'Tarantool does not support readview')

end
