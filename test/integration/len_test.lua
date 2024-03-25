local t = require('luatest')

local helpers = require('test.helper')

local pgroup = t.group('len', helpers.backend_matrix({
    {engine = 'memtx'},
}))

pgroup.before_all(function(g)
    helpers.start_default_cluster(g, 'srv_select')
end)

pgroup.after_all(function(g)
    helpers.stop_cluster(g.cluster, g.params.backend)
end)

pgroup.before_each(function(g)
    helpers.truncate_space_on_cluster(g.cluster, 'customers')
end)

pgroup.test_len_non_existent_space = function(g)
    local result, err = g.router:call('crud.len', {'non_existent_space'})

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

    local result, err = g.router:call('crud.len', {'customers'})

    t.assert_equals(err, nil)
    t.assert_equals(result, expected_len)
end

pgroup.test_len_empty_space = function(g)
    local result, err = g.router:call('crud.len', {'customers'})

    t.assert_equals(err, nil)
    t.assert_equals(result, 0)
end

pgroup.test_opts_not_damaged = function(g)
    local len_opts = {timeout = 1}
    local new_len_opts, err = g.router:eval([[
        local crud = require('crud')

        local len_opts = ...

        local _, err = crud.len('customers', len_opts)

        return len_opts, err
    ]], {len_opts})

    t.assert_equals(err, nil)
    t.assert_equals(new_len_opts, len_opts)
end
