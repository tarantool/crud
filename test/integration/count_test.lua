local clock = require('clock')

local t = require('luatest')

local helpers = require('test.helper')
local read_scenario = require('test.integration.read_scenario')

local pgroup = t.group('count', helpers.backend_matrix({
    {engine = 'memtx'},
}))

pgroup.before_all(function(g)
    helpers.start_default_cluster(g, 'srv_select')

    g.router.net_box:eval([[
        require('crud').cfg{ stats = true }
    ]])
    g.router.net_box:eval([[
        require('crud.ratelimit').disable()
    ]])
end)

pgroup.after_all(function(g)
    helpers.stop_cluster(g.cluster, g.params.backend)
end)

pgroup.before_each(function(g)
    helpers.truncate_space_on_cluster(g.cluster, 'customers')
    helpers.truncate_space_on_cluster(g.cluster, 'coord')
    helpers.truncate_space_on_cluster(g.cluster, 'book_translation')
    helpers.truncate_space_on_cluster(g.cluster, 'interval')
end)

pgroup.test_count_non_existent_space = function(g)
    local result, err = g.router:call('crud.count', {
        'non_existent_space',
        nil,
        {fullscan = true, mode = 'write'},
    })

    t.assert_equals(result, nil)
    t.assert_str_contains(err.err, "Space \"non_existent_space\" doesn't exist")
end

pgroup.test_count_empty_space = function(g)
    local result, err = g.router:call('crud.count', {
        'customers',
        nil,
        {fullscan = true, mode = 'write'},
    })

    t.assert_equals(err, nil)
    t.assert_equals(result, 0)
end

pgroup.test_not_valid_value_type = function(g)
    local conditions = {
        {'==', 'id', 'not_number'}
    }

    local result, err = g.router:call('crud.count', {
        'customers',
        conditions,
        {mode = 'write'},
    })

    t.assert_equals(result, nil)
    t.assert_str_contains(err.err, "Supplied key type of part 0 does not match index part type: expected unsigned")
end

pgroup.test_not_valid_operation = function(g)
    local conditions = {
        {{}, 'id', 5}
    }

    local result, err = g.router:call('crud.count', {
        'customers',
        conditions,
        {fullscan = true, mode = 'write'},
    })

    t.assert_equals(result, nil)
    t.assert_str_contains(err.err, "Failed to parse conditions")
end

pgroup.test_conditions_with_non_existed_field = function(g)
    local conditions = {
        {'==', 'non_existed_field', 'value'}
    }

    local result, err = g.router:call('crud.count', {
        'customers',
        conditions,
        {mode = 'write'},
    })

    t.assert_equals(err, nil)
    t.assert_equals(result, 0)
end


local count_safety_cases = {
    nil_and_nil_opts = {
        has_crit = true,
        user_conditions = nil,
        opts = {mode = 'write'},
    },
    fullscan_false = {
        has_crit = true,
        user_conditions = nil,
        opts = {fullscan = false, mode = 'write'},
    },
    fullscan_true = {
        has_crit = false,
        user_conditions = nil,
        opts = {fullscan = true, mode = 'write'},
    },
    non_equal_conditions = {
        has_crit = true,
        user_conditions = {
            {'>=', 'last_name', 'A'},
            {'<=', 'last_name', 'Z'},
            {'>', 'age', 20},
            {'<', 'age', 30},
        },
        opts = {mode = 'write'},
    },
    equal_condition = {
        has_crit = false,
        user_conditions = {
            {'>=', 'last_name', 'A'},
            {'<=', 'last_name', 'Z'},
            {'=', 'age', 25},
        },
        opts = {mode = 'write'},
    },
    equal_condition2 = {
        has_crit = false,
        user_conditions = {
            {'>=', 'last_name', 'A'},
            {'<=', 'last_name', 'Z'},
            {'==', 'age', 25},
        },
        opts = {mode = 'write'},
    },
}

for name, case in pairs(count_safety_cases) do
    local space = 'customers'
    local crit_log = "C> Potentially long count from space '" .. space .. "'"
    local test_name = ('test_count_safety_%s'):format(name)

    pgroup[test_name] = function(g)
        local uc = case.user_conditions
        local opts = case.opts
        local captured, err = helpers.get_command_log(g.router,
            'crud.count', {space, uc, opts})

        t.assert_equals(err, nil)
        if case.has_crit then
            t.assert_str_contains(captured, crit_log)
        else
            t.assert_equals(string.find(captured, crit_log, 1, true), nil)
        end
    end
end

pgroup.test_count_all = function(g)
    -- let's insert five tuples on different replicasets
    -- (two tuples on one replica and three on the other)
    -- to check that  the total count will be calculated on the router
    helpers.insert_objects(g, 'customers', {
        {
            -- bucket_id is 477, storage is s-2
            id = 1, name = "Elizabeth", last_name = "Jackson",
            age = 31, city = "New York",
        }, {
            -- bucket_id is 401, storage is s-2
            id = 2, name = "Mary", last_name = "Brown",
            age = 33, city = "Los Angeles",
        },  {
            -- bucket_id is 2804, storage is s-1
            id = 3, name = "Elizabeth", last_name = "Smith",
            age = 33, city = "Los Angeles",
        }, {
            -- bucket_id is 1161, storage is s-2
            id = 4, name = "William", last_name = "White",
            age = 81, city = "Chicago",
        }, {
            -- bucket_id is 1172, storage is s-1
            id = 5, name = "Elizabeth", last_name = "May",
            age = 28, city = "New York",
        },
    })

    local result, err = g.router:call('crud.count', {
        'customers', nil, {fullscan = true, mode = 'write'},
    })

    t.assert_equals(err, nil)
    t.assert_equals(result, 5)
end

pgroup.test_count_all_with_yield_every = function(g)
    -- let's insert five tuples on different replicasets
    -- (two tuples on one replica and three on the other)
    -- to check that  the total count will be calculated on the router
    helpers.insert_objects(g, 'customers', {
        {
            -- bucket_id is 477, storage is s-2
            id = 1, name = "Elizabeth", last_name = "Jackson",
            age = 31, city = "New York",
        }, {
            -- bucket_id is 401, storage is s-2
            id = 2, name = "Mary", last_name = "Brown",
            age = 33, city = "Los Angeles",
        },  {
            -- bucket_id is 2804, storage is s-1
            id = 3, name = "Elizabeth", last_name = "Smith",
            age = 33, city = "Los Angeles",
        }, {
            -- bucket_id is 1161, storage is s-2
            id = 4, name = "William", last_name = "White",
            age = 81, city = "Chicago",
        }, {
            -- bucket_id is 1172, storage is s-1
            id = 5, name = "Elizabeth", last_name = "May",
            age = 28, city = "New York",
        },
    })

    local result, err = g.router:call('crud.count', {
        'customers', nil, {yield_every = 1, fullscan = true, mode = 'write'},
    })

    t.assert_equals(err, nil)
    t.assert_equals(result, 5)
end

pgroup.test_count_all_with_yield_every_0 = function(g)
    -- let's insert five tuples on different replicasets
    -- (two tuples on one replica and three on the other)
    -- to check that  the total count will be calculated on the router
    helpers.insert_objects(g, 'customers', {
        {
            -- bucket_id is 477, storage is s-2
            id = 1, name = "Elizabeth", last_name = "Jackson",
            age = 31, city = "New York",
        }, {
            -- bucket_id is 401, storage is s-2
            id = 2, name = "Mary", last_name = "Brown",
            age = 33, city = "Los Angeles",
        },  {
            -- bucket_id is 2804, storage is s-1
            id = 3, name = "Elizabeth", last_name = "Smith",
            age = 33, city = "Los Angeles",
        }, {
            -- bucket_id is 1161, storage is s-2
            id = 4, name = "William", last_name = "White",
            age = 81, city = "Chicago",
        }, {
            -- bucket_id is 1172, storage is s-1
            id = 5, name = "Elizabeth", last_name = "May",
            age = 28, city = "New York",
        },
    })

    local result, err = g.router:call('crud.count', {
        'customers', nil, {yield_every = 0, fullscan = true, mode = 'write'}
    })

    t.assert_equals(result, nil)
    t.assert_str_contains(err.err, "yield_every should be > 0")
end

pgroup.test_count_by_primary_index = function(g)
    -- let's insert five tuples on different replicasets
    -- (two tuples on one replica and three on the other)
    -- to check that  the total count will be calculated on the router
    helpers.insert_objects(g, 'customers', {
        {
            -- bucket_id is 477, storage is s-2
            id = 1, name = "Elizabeth", last_name = "Jackson",
            age = 31, city = "New York",
        }, {
            -- bucket_id is 401, storage is s-2
            id = 2, name = "Mary", last_name = "Brown",
            age = 33, city = "Los Angeles",
        },  {
            -- bucket_id is 2804, storage is s-1
            id = 3, name = "Elizabeth", last_name = "Smith",
            age = 33, city = "Los Angeles",
        }, {
            -- bucket_id is 1161, storage is s-2
            id = 4, name = "William", last_name = "White",
            age = 81, city = "Chicago",
        }, {
            -- bucket_id is 1172, storage is s-1
            id = 5, name = "Elizabeth", last_name = "May",
            age = 28, city = "New York",
        },
    })

    local conditions = {{'==', 'id_index', 3}}

    local result, err = g.router:call('crud.count', {
        'customers', conditions, {mode = 'write'},
    })

    t.assert_equals(err, nil)
    t.assert_equals(result, 1)
end

pgroup.test_eq_condition_with_index = function(g)
    -- let's insert five tuples on different replicasets
    -- (two tuples on one replica and three on the other)
    -- to check that the total count will be calculated on the router
    helpers.insert_objects(g, 'customers', {
        {
            -- bucket_id is 477, storage is s-2
            id = 1, name = "Elizabeth", last_name = "Jackson",
            age = 31, city = "New York",
        }, {
            -- bucket_id is 401, storage is s-2
            id = 2, name = "Mary", last_name = "Brown",
            age = 33, city = "Los Angeles",
        },  {
            -- bucket_id is 2804, storage is s-1
            id = 3, name = "Elizabeth", last_name = "Smith",
            age = 33, city = "Los Angeles",
        }, {
            -- bucket_id is 1161, storage is s-2
            id = 4, name = "William", last_name = "White",
            age = 81, city = "Chicago",
        }, {
            -- bucket_id is 1172, storage is s-1
            id = 5, name = "Elizabeth", last_name = "May",
            age = 28, city = "New York",
        },
    })

    local conditions = {
        {'==', 'age_index', 33},
    }

    local expected_len = 2

    local result, err = g.router:call('crud.count', {
        'customers', conditions, {mode = 'write'},
    })

    t.assert_equals(err, nil)
    t.assert_equals(result, expected_len)
end

pgroup.test_ge_condition_with_index = function(g)
    -- let's insert five tuples on different replicasets
    -- (two tuples on one replica and three on the other)
    -- to check that  the total count will be calculated on the router
    helpers.insert_objects(g, 'customers', {
        {
            -- bucket_id is 477, storage is s-2
            id = 1, name = "Elizabeth", last_name = "Jackson",
            age = 31, city = "New York",
        }, {
            -- bucket_id is 401, storage is s-2
            id = 2, name = "Mary", last_name = "Brown",
            age = 33, city = "Los Angeles",
        },  {
            -- bucket_id is 2804, storage is s-1
            id = 3, name = "Elizabeth", last_name = "Smith",
            age = 33, city = "Los Angeles",
        }, {
            -- bucket_id is 1161, storage is s-2
            id = 4, name = "William", last_name = "White",
            age = 81, city = "Chicago",
        }, {
            -- bucket_id is 1172, storage is s-1
            id = 5, name = "Elizabeth", last_name = "May",
            age = 28, city = "New York",
        },
    })

    local conditions = {
        {'>=', 'age_index', 33},
    }

    local expected_len = 3

    local result, err = g.router:call('crud.count', {
        'customers', conditions, {fullscan = true, mode = 'write'},
    })

    t.assert_equals(err, nil)
    t.assert_equals(result, expected_len)
end

pgroup.test_gt_condition_with_index = function(g)
    -- let's insert five tuples on different replicasets
    -- (two tuples on one replica and three on the other)
    -- to check that  the total count will be calculated on the router
    helpers.insert_objects(g, 'customers', {
        {
            -- bucket_id is 477, storage is s-2
            id = 1, name = "Elizabeth", last_name = "Jackson",
            age = 31, city = "New York",
        }, {
            -- bucket_id is 401, storage is s-2
            id = 2, name = "Mary", last_name = "Brown",
            age = 33, city = "Los Angeles",
        },  {
            -- bucket_id is 2804, storage is s-1
            id = 3, name = "Elizabeth", last_name = "Smith",
            age = 33, city = "Los Angeles",
        }, {
            -- bucket_id is 1161, storage is s-2
            id = 4, name = "William", last_name = "White",
            age = 81, city = "Chicago",
        }, {
            -- bucket_id is 1172, storage is s-1
            id = 5, name = "Elizabeth", last_name = "May",
            age = 28, city = "New York",
        },
    })

    local conditions = {
        {'>', 'age_index', 33},
    }

    local expected_len = 1

    local result, err = g.router:call('crud.count', {
        'customers', conditions, {fullscan = true, mode = 'write'},
    })

    t.assert_equals(err, nil)
    t.assert_equals(result, expected_len)
end

pgroup.test_le_condition_with_index = function(g)
    -- let's insert five tuples on different replicasets
    -- (two tuples on one replica and three on the other)
    -- to check that  the total count will be calculated on the router
    helpers.insert_objects(g, 'customers', {
        {
            -- bucket_id is 477, storage is s-2
            id = 1, name = "Elizabeth", last_name = "Jackson",
            age = 31, city = "New York",
        }, {
            -- bucket_id is 401, storage is s-2
            id = 2, name = "Mary", last_name = "Brown",
            age = 33, city = "Los Angeles",
        },  {
            -- bucket_id is 2804, storage is s-1
            id = 3, name = "Elizabeth", last_name = "Smith",
            age = 33, city = "Los Angeles",
        }, {
            -- bucket_id is 1161, storage is s-2
            id = 4, name = "William", last_name = "White",
            age = 81, city = "Chicago",
        }, {
            -- bucket_id is 1172, storage is s-1
            id = 5, name = "Elizabeth", last_name = "May",
            age = 28, city = "New York",
        },
    })

    local conditions = {
        {'<=', 'age_index', 33},
    }

    local expected_len = 4

    local result, err = g.router:call('crud.count', {
        'customers', conditions, {fullscan = true, mode = 'write'},
    })

    t.assert_equals(err, nil)
    t.assert_equals(result, expected_len)
end

pgroup.test_lt_condition_with_index = function(g)
    -- let's insert five tuples on different replicasets
    -- (two tuples on one replica and three on the other)
    -- to check that  the total count will be calculated on the router
    helpers.insert_objects(g, 'customers', {
        {
            -- bucket_id is 477, storage is s-2
            id = 1, name = "Elizabeth", last_name = "Jackson",
            age = 31, city = "New York",
        }, {
            -- bucket_id is 401, storage is s-2
            id = 2, name = "Mary", last_name = "Brown",
            age = 33, city = "Los Angeles",
        },  {
            -- bucket_id is 2804, storage is s-1
            id = 3, name = "Elizabeth", last_name = "Smith",
            age = 33, city = "Los Angeles",
        }, {
            -- bucket_id is 1161, storage is s-2
            id = 4, name = "William", last_name = "White",
            age = 81, city = "Chicago",
        }, {
            -- bucket_id is 1172, storage is s-1
            id = 5, name = "Elizabeth", last_name = "May",
            age = 28, city = "New York",
        },
    })

    local conditions = {
        {'<', 'age_index', 33},
    }

    local expected_len = 2

    local result, err = g.router:call('crud.count', {
        'customers', conditions, {fullscan = true, mode = 'write'},
    })

    t.assert_equals(err, nil)
    t.assert_equals(result, expected_len)
end

pgroup.test_multiple_conditions = function(g)
    -- let's insert five tuples on different replicasets
    -- (two tuples on one replica and three on the other)
    -- to check that  the total count will be calculated on the router
    helpers.insert_objects(g, 'customers', {
        {
            -- bucket_id is 477, storage is s-2
            id = 1, name = "Elizabeth", last_name = "Jackson",
            age = 31, city = "New York",
        }, {
            -- bucket_id is 401, storage is s-2
            id = 2, name = "Mary", last_name = "Brown",
            age = 33, city = "Los Angeles",
        },  {
            -- bucket_id is 2804, storage is s-1
            id = 3, name = "Elizabeth", last_name = "Smith",
            age = 33, city = "Los Angeles",
        }, {
            -- bucket_id is 1161, storage is s-2
            id = 4, name = "William", last_name = "White",
            age = 81, city = "Chicago",
        }, {
            -- bucket_id is 1172, storage is s-1
            id = 5, name = "Elizabeth", last_name = "May",
            age = 28, city = "New York",
        },
    })

    local conditions = {
        {'>', 'age', 25},
        {'==', 'name', 'Elizabeth'},
        {'==', 'city', 'New York'},
    }

    local expected_len = 2

    local result, err = g.router:call('crud.count', {
        'customers', conditions, {mode = 'write'},
    })

    t.assert_equals(err, nil)
    t.assert_equals(result, expected_len)
end

pgroup.test_multipart_primary_index = function(g)
    local bucket_id = 1
    local other_bucket_id, err = helpers.get_other_storage_bucket_id(g.cluster, bucket_id)
    t.assert(other_bucket_id ~= nil, err)

    helpers.insert_objects(g, 'coord', {
        { x = 0, y = 0, bucket_id = bucket_id },       -- 1
        { x = 0, y = 1, bucket_id = other_bucket_id }, -- 2
        { x = 0, y = 2, bucket_id = bucket_id },       -- 3
        { x = 1, y = 3, bucket_id = other_bucket_id }, -- 4
        { x = 1, y = 4, bucket_id = bucket_id },       -- 5
    })

    local conditions = {{'=', 'primary', 0}}
    local result, err = g.router:call('crud.count', {
        'coord', conditions, {mode = 'write'},
    })

    t.assert_equals(err, nil)
    t.assert_equals(result, 3)

    local conditions = {{'=', 'primary', {0, 2}}}
    local result, err = g.router:call('crud.count', {
        'coord', conditions, {mode = 'write'},
    })

    t.assert_equals(err, nil)
    t.assert_equals(result, 1)
end

pgroup.test_opts_not_damaged = function(g)
    helpers.insert_objects(g, 'customers', {
        {
            id = 1, name = "Elizabeth", last_name = "Jackson",
            age = 12, city = "Los Angeles",
        }, {
            id = 2, name = "Mary", last_name = "Brown",
            age = 46, city = "London",
        }, {
            id = 3, name = "David", last_name = "Smith",
            age = 33, city = "Los Angeles",
        }, {
            id = 4, name = "William", last_name = "White",
            age = 46, city = "Chicago",
        },
    })

    local count_opts = {
        timeout = 1,
        bucket_id = 1161,
        yield_every = 105,
        mode = 'read',
        prefer_replica = false,
        balance = false,
        fullscan = true
    }
    local new_count_opts, err = g.router:eval([[
         local crud = require('crud')

         local count_opts = ...

         local _, err = crud.count('customers', nil, count_opts)

         return count_opts, err
     ]], {count_opts})

    t.assert_equals(err, nil)
    t.assert_equals(new_count_opts, count_opts)
end

pgroup.test_count_no_map_reduce = function(g)
    helpers.insert_objects(g, 'customers', {
        {
            -- bucket_id is 477, storage is s-2
            id = 1, name = 'Elizabeth', last_name = 'Jackson',
            age = 12, city = 'New York',
        }, {
            -- bucket_id is 401, storage is s-2
            id = 2, name = 'Mary', last_name = 'Brown',
            age = 46, city = 'Los Angeles',
        }, {
            -- bucket_id is 2804, storage is s-1
            id = 3, name = 'David', last_name = 'Smith',
            age = 33, city = 'Los Angeles',
        }, {
            -- bucket_id is 1161, storage is s-2
            id = 4, name = 'William', last_name = 'White',
            age = 81, city = 'Chicago',
        },
    })

    local router = g.router.net_box
    local map_reduces_before = helpers.get_map_reduces_stat(router, 'customers')

    -- Case: no conditions, just bucket id.
    local result, err = g.router:call('crud.count', {
        'customers',
        nil,
        {bucket_id = 2804, timeout = 1, fullscan = true, mode = 'write'},
    })
    t.assert_equals(err, nil)
    t.assert_equals(result, 1)

    local map_reduces_after_1 = helpers.get_map_reduces_stat(router, 'customers')
    local diff_1 = map_reduces_after_1 - map_reduces_before
    t.assert_equals(diff_1, 0, 'Count request was not a map reduce')

    -- Case: EQ on secondary index, which is not in the sharding
    -- index (primary index in the case).
    local result, err = g.router:call('crud.count', {
        'customers',
        {{'==', 'age', 81}},
        {bucket_id = 1161, timeout = 1, mode = 'write'},
    })
    t.assert_equals(err, nil)
    t.assert_equals(result, 1)

    local map_reduces_after_2 = helpers.get_map_reduces_stat(router, 'customers')
    local diff_2 = map_reduces_after_2 - map_reduces_after_1
    t.assert_equals(diff_2, 0, 'Count request was not a map reduce')
end

pgroup.test_count_timeout = function(g)
    -- let's insert five tuples on different replicasets
    -- (two tuples on one replica and three on the other)
    -- to check that  the total count will be calculated on the router
    helpers.insert_objects(g, 'customers', {
        {
            -- bucket_id is 477, storage is s-2
            id = 1, name = "Elizabeth", last_name = "Jackson",
            age = 31, city = "New York",
        }, {
            -- bucket_id is 401, storage is s-2
            id = 2, name = "Mary", last_name = "Brown",
            age = 33, city = "Los Angeles",
        },  {
            -- bucket_id is 2804, storage is s-1
            id = 3, name = "Elizabeth", last_name = "Smith",
            age = 33, city = "Los Angeles",
        }, {
            -- bucket_id is 1161, storage is s-2
            id = 4, name = "William", last_name = "White",
            age = 81, city = "Chicago",
        }, {
            -- bucket_id is 1172, storage is s-1
            id = 5, name = "Elizabeth", last_name = "May",
            age = 28, city = "New York",
        },
    })

    local conditions = {
        {'<', 'age_index', 33},
    }

    local expected_len = 2
    local timeout = 4
    local begin = clock.proc()

    local result, err = g.router:call('crud.count', {
        'customers', conditions, {timeout = timeout, fullscan = true, mode = 'write'},
    })

    t.assert_equals(err, nil)
    t.assert_equals(result, expected_len)
    t.assert_lt(clock.proc() - begin, timeout)
end

pgroup.test_composite_index = function(g)
    -- let's insert five tuples on different replicasets
    -- (two tuples on one replica and three on the other)
    -- to check that  the total count will be calculated on the router
    helpers.insert_objects(g, 'customers', {
        {
            -- bucket_id is 477, storage is s-2
            id = 1, name = "Elizabeth", last_name = "Rodriguez",
            age = 31, city = "New York",
        }, {
            -- bucket_id is 401, storage is s-2
            id = 2, name = "Elizabeth", last_name = "Johnson",
            age = 33, city = "Los Angeles",
        },  {
            -- bucket_id is 2804, storage is s-1
            id = 3, name = "David", last_name = "Brown",
            age = 33, city = "Los Angeles",
        }, {
            -- bucket_id is 1161, storage is s-2
            id = 4, name = "Jessica", last_name = "Jones",
            age = 81, city = "Chicago",
        }, {
            -- bucket_id is 1172, storage is s-1
            id = 5, name = "Elizabeth", last_name = "May",
            age = 28, city = "New York",
        },
    })

    local conditions = {
        {'>=', 'full_name', {"Elizabeth", "Jo"}},
    }

    -- no after
    local result, err = g.router:call('crud.count', {
        'customers', conditions, {fullscan = true, mode = 'write'},
    })

    t.assert_equals(err, nil)
    t.assert_equals(result, 4)

    -- partial value in conditions
    local conditions = {
        {'==', 'full_name', "Elizabeth"},
    }

    local result, err = g.router:call('crud.count', {
        'customers', conditions, {mode = 'write'},
    })

    t.assert_equals(err, nil)
    t.assert_equals(result, 3)
end

pgroup.test_composite_primary_index = function(g)
    local book_translation = helpers.insert_objects(g, 'book_translation', {
        {
            id = 5,
            language = 'Ukrainian',
            edition = 55,
            translator = 'Mitro Dmitrienko',
            comments = 'Translation 55',
        }
    })
    t.assert_equals(#book_translation, 1)

    local conditions = {{'=', 'id', {5, 'Ukrainian', 55}}}

    local result, err = g.router:call('crud.count', {
        'book_translation', conditions, {mode = 'write'},
    })
    t.assert_equals(err, nil)
    t.assert_equals(result, 1)
end

pgroup.test_count_by_full_sharding_key = function(g)
    -- let's insert five tuples on different replicasets
    -- (two tuples on one replica and three on the other)
    -- to check that  the total count will be calculated on the router
    helpers.insert_objects(g, 'customers', {
        {
            -- bucket_id is 477, storage is s-2
            id = 1, name = "Elizabeth", last_name = "Rodriguez",
            age = 31, city = "New York",
        }, {
            -- bucket_id is 401, storage is s-2
            id = 2, name = "Elizabeth", last_name = "Johnson",
            age = 33, city = "Los Angeles",
        },  {
            -- bucket_id is 2804, storage is s-1
            id = 3, name = "David", last_name = "Brown",
            age = 33, city = "Los Angeles",
        }, {
            -- bucket_id is 1161, storage is s-2
            id = 4, name = "Jessica", last_name = "Jones",
            age = 81, city = "Chicago",
        }, {
            -- bucket_id is 1172, storage is s-1
            id = 5, name = "Elizabeth", last_name = "May",
            age = 28, city = "New York",
        },
    })

    local conditions = {{'==', 'id', 3}}
    local result, err = g.router:call('crud.count', {
        'customers', conditions, {mode = 'write'},
    })

    t.assert_equals(err, nil)
    t.assert_equals(result, 1)
end

pgroup.test_count_force_map_call = function(g)
    local key = 1

    local first_bucket_id = g.router:eval([[
        local vshard = require('vshard')

        local key = ...
        return vshard.router.bucket_id_strcrc32(key)
    ]], {key})

    local second_bucket_id, err = helpers.get_other_storage_bucket_id(g.cluster, first_bucket_id)

    t.assert_equals(err, nil)

    helpers.insert_objects(g, 'customers', {
        {
            id = key, bucket_id = first_bucket_id, name = "Elizabeth", last_name = "Jackson",
            age = 12, city = "New York",
        }, {
            id = key, bucket_id = second_bucket_id, name = "Mary", last_name = "Brown",
            age = 46, city = "Los Angeles",
        },
    })

    local result, err = g.router:call('crud.count', {
        'customers', {{'==', 'id', key}}, {mode = 'write'},
    })

    t.assert_equals(err, nil)
    t.assert_equals(result, 1)

    result, err = g.router:call('crud.count', {
        'customers', {{'==', 'id', key}}, {force_map_call = true, mode = 'write'},
    })

    t.assert_equals(err, nil)
    t.assert_equals(result, 2)
end

local read_impl = function(cg, space, conditions, opts)
    opts = table.deepcopy(opts) or {}
    opts.mode = 'write'

    return cg.router:call('crud.count', {space, conditions, opts})
end

pgroup.test_gh_418_count_with_secondary_noneq_index_condition = function(g)
    read_scenario.gh_418_read_with_secondary_noneq_index_condition(g, read_impl)
end

local gh_373_types_cases = helpers.merge_tables(
    read_scenario.gh_373_read_with_decimal_condition_cases,
    read_scenario.gh_373_read_with_datetime_condition_cases,
    read_scenario.gh_373_read_with_interval_condition_cases
)

for case_name_template, case in pairs(gh_373_types_cases) do
    local case_name = 'test_' .. case_name_template:format('count')

    pgroup[case_name] = function(g)
        case(g, read_impl)
    end
end

for case_name_template, case in pairs(read_scenario.gh_422_nullability_cases) do
    local case_name = 'test_' .. case_name_template:format('count')

    pgroup[case_name] = function(g)
        case(g, read_impl)
    end
end
