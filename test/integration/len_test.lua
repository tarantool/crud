local fio = require('fio')

local t = require('luatest')

local helpers = require('test.helper')

local pgroup = t.group('len', {
    {engine = 'memtx'},
})

pgroup.before_all(function(g)
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

pgroup.after_all(function(g) helpers.stop_cluster(g.cluster) end)

pgroup.before_each(function(g)
    helpers.truncate_space_on_cluster(g.cluster, 'customers')
end)

pgroup.test_len_non_existent_space = function(g)
    local result, err = g.cluster.main_server.net_box:call('crud.len', {'non_existent_space'})

    t.assert_equals(result, nil)
    t.assert_str_contains(err.err, "Space \"non_existent_space\" doesn't exist")
end

pgroup.test_len = function(g)
    local customers = {}
    local expected_len = 100

    -- let's insert a large number of tuples in a simple loop that gives
    -- really high probability that there is at least one tuple on each storage
    for i = 1, expected_len do
        table.insert(customers, {
            id = i, name = tostring(i), last_name = tostring(i),
            age = i, city = tostring(i),
        })
    end

    helpers.insert_objects(g, 'customers', customers)

    local result, err = g.cluster.main_server.net_box:call('crud.len', {'customers'})

    t.assert_equals(err, nil)
    t.assert_equals(result, expected_len)
end

pgroup.test_len_empty_space = function(g)
    local result, err = g.cluster.main_server.net_box:call('crud.len', {'customers'})

    t.assert_equals(err, nil)
    t.assert_equals(result, 0)
end

pgroup.test_opts_not_damaged = function(g)
    local len_opts = {timeout = 1}
    local new_len_opts, err = g.cluster.main_server.net_box:eval([[
        local crud = require('crud')

        local len_opts = ...

        local _, err = crud.len('customers', len_opts)

        return len_opts, err
    ]], {len_opts})

    t.assert_equals(err, nil)
    t.assert_equals(new_len_opts, len_opts)
end
