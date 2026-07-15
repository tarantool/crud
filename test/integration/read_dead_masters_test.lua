local t = require('luatest')

local helpers = require('test.helper')

local pgroup = t.group('read_only_with_dead_masters', helpers.backend_matrix({
    {engine = 'memtx'},
}))

pgroup.before_all(function(g)
    helpers.start_default_cluster(g, 'srv_simple_operations')
end)

pgroup.after_all(function(g)
    helpers.stop_cluster(g.cluster, g.params.backend)
end)

pgroup.test_read_operations_when_masters_are_down = function(g)
    local insert_res, err = g.router:call('crud.insert', {'customers', {1, box.NULL, 'Elizabeth', 12}})
    t.assert_equals(err, nil)
    t.assert_not_equals(insert_res, nil)

    insert_res, err = g.router:call('crud.insert', {'customers', {5, box.NULL, 'Jack', 35}})
    t.assert_equals(err, nil)
    t.assert_not_equals(insert_res, nil)

    -- Stop all masters in the cluster to simulate total master outage.
    local s1_master = g.cluster:server('s1-master')
    local s2_master = g.cluster:server('s2-master')

    s1_master:stop()
    s2_master:stop()

    local pairs_tuples = g.router:exec(function()
        local crud = require('crud')
        local tuples = {}
        for _, tuple in crud.pairs('customers') do
            table.insert(tuples, tuple)
        end
        return tuples
    end)
    t.assert_equals(#pairs_tuples, 2)

    local len_res, err = g.router:call('crud.len', {'customers', {mode = 'read'}})
    t.assert_equals(err, nil)
    t.assert_equals(len_res, 2)

    local select_res, err = g.router:call('crud.select', {'customers', {{'<=', 'age', 35}}})
    t.assert_equals(err, nil)
    t.assert_not_equals(select_res, nil)
    t.assert_equals(#select_res.rows, 2)

    local get_res, err = g.router:call('crud.get', {'customers', 1})
    t.assert_equals(err, nil)
    t.assert_not_equals(get_res, nil)
    t.assert_equals(get_res.rows[1][3], 'Elizabeth')

    local count_res, err = g.router:call('crud.count', {'customers', {{'==', 'age', 35}}})
    t.assert_equals(err, nil)
    t.assert_equals(count_res, 1)

    local min_res, err = g.router:call('crud.min', {'customers'})
    t.assert_equals(err, nil)
    t.assert_not_equals(min_res, nil)
    t.assert_equals(min_res.rows[1][1], 1)

    local max_res, err = g.router:call('crud.max', {'customers'})
    t.assert_equals(err, nil)
    t.assert_not_equals(max_res, nil)
    t.assert_equals(max_res.rows[1][1], 5)
end
