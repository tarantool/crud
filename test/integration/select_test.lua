local t = require('luatest')

local crud = require('crud')
local crud_utils = require('crud.common.utils')

local helpers = require('test.helper')
local read_scenario = require('test.integration.read_scenario')

local pgroup = t.group('select', helpers.backend_matrix({
    {engine = 'memtx'},
}))

pgroup.before_all(function(g)
    helpers.start_default_cluster(g, 'srv_select')

    g.space_format = g.cluster:server('s1-master').net_box.space.customers:format()

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
    helpers.reset_call_cache(g.cluster)
    helpers.truncate_space_on_cluster(g.cluster, 'customers')
    helpers.truncate_space_on_cluster(g.cluster, 'developers')
    helpers.truncate_space_on_cluster(g.cluster, 'cars')
    helpers.truncate_space_on_cluster(g.cluster, 'logins')
    helpers.truncate_space_on_cluster(g.cluster, 'interval')
end)


pgroup.test_non_existent_space = function(g)
    -- insert
    local obj, err = g.router:call('crud.select', {
        'non_existent_space',
        nil,
        {fullscan = true, mode = 'write'},
    })

    t.assert_equals(obj, nil)
    t.assert_str_contains(err.err, "Space \"non_existent_space\" doesn't exist")
end

pgroup.test_select_no_index = function(g)
    local obj, err = g.router:call('crud.select', {
        'no_index_space',
        nil,
        {fullscan = true, mode = 'write'},
    })

    t.assert_equals(obj, nil)
    t.assert_str_contains(err.err, "Space \"no_index_space\" has no indexes, space should have primary index")
end

pgroup.test_not_valid_value_type = function(g)
    local conditions = {
        {'=', 'id', 'not_number'}
    }

    local obj, err = g.router:eval([[
        local crud = require('crud')
        local conditions = ...

        local result, err = crud.select('customers', conditions, {mode = 'write'})
        return result, err
    ]], {conditions})

    t.assert_equals(obj, nil)
    t.assert_str_contains(err.err, "Supplied key type of part 0 does not match index part type: expected unsigned")
end

local select_safety_cases = {
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
    limit = {
        has_crit = false,
        user_conditions = nil,
        opts = {first = 123, mode = 'write'},
    },
    max_negative_limit = {
        has_crit = false,
        user_conditions = nil,
        opts = {first = -1000, mode = 'write'},
    },
    max_limit = {
        has_crit = false,
        user_conditions = nil,
        opts = {first = 1000, mode = 'write'},
    },
    too_big_limit = {
        has_crit = true,
        user_conditions = nil,
        opts = {first = 1001, mode = 'write'},
    },
    too_low_limit = {
        has_crit = true,
        user_conditions = nil,
        opts = {first = -1001, mode = 'write'},
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

for name, case in pairs(select_safety_cases) do
    local space = 'customers'
    local crit_log = "C> Potentially long select from space '" .. space .. "'"
    local test_name = ('test_select_safety_%s'):format(name)

    pgroup[test_name] = function(g)
        local uc = case.user_conditions
        local opts = case.opts

        if opts ~= nil and opts.first ~= nil and opts.first < 0 then
            local after_tuple = {
                id = 1, name = "Elizabeth", last_name = "Jackson",
                age = 12, city = "New York",
            }
            opts.after = crud_utils.flatten(after_tuple, g.space_format)
        end

        local captured, err = helpers.get_command_log(g.router,
            'crud.select', {space, uc, opts})
        t.assert_equals(err, nil)

        if case.has_crit then
            t.assert_str_contains(captured, crit_log)
        else
            t.assert_equals(string.find(captured, crit_log, 1, true), nil)
        end
    end
end

pgroup.test_select_all = function(g)
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

    local result, err = g.router:call('crud.select', {
        'customers', nil, {fullscan = true, mode = 'write'},
    })

    t.assert_equals(err, nil)
    t.assert_equals(result.rows, {
        {1, 477, "Elizabeth", "Jackson", 12, "New York"},
        {2, 401, "Mary", "Brown", 46, "Los Angeles"},
        {3, 2804, "David", "Smith", 33, "Los Angeles"},
        {4, 1161, "William", "White", 81, "Chicago"},
    })
    t.assert_equals(result.metadata, {
        {name = 'id', type = 'unsigned'},
        {name = 'bucket_id', type = 'unsigned'},
        {name = 'name', type = 'string'},
        {name = 'last_name', type = 'string'},
        {name = 'age', type = 'number'},
        {name = 'city', type = 'string'},
    })

    -- no after
    local result, err = g.router:call('crud.select', {
        'customers', nil, {fullscan = true, mode = 'write'},
    })

    t.assert_equals(err, nil)
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, customers)

    -- after obj 2
    local after = crud_utils.flatten(customers[2], g.space_format)
    local result, err = g.router:call('crud.select', {
        'customers', nil, {after = after, fullscan = true, mode = 'write'},
    })

    t.assert_equals(err, nil)
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, helpers.get_objects_by_idxs(customers, {3, 4}))

    -- after obj 4 (last)
    local after = crud_utils.flatten(customers[4], g.space_format)
    local result, err = g.router:call('crud.select', {
        'customers', nil, {after = after, fullscan = true, mode = 'write'},
    })

    t.assert_equals(err, nil)
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(#objects, 0)
end

pgroup.test_select_all_with_first = function(g)
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

    -- first 2
    local first = 2
    local result, err = g.router:call('crud.select', {
        'customers', nil, {first = first, mode = 'write'},
    })

    t.assert_equals(err, nil)
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, helpers.get_objects_by_idxs(customers, {1, 2}))

    -- first 0
    local first = 0
    local result, err = g.router:call('crud.select', {
        'customers', nil, {first = first, mode = 'write'},
    })

    t.assert_equals(err, nil)
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(#objects, 0)
end

pgroup.test_negative_first = function(g)
    local customers = helpers.insert_objects(g, 'customers', {
        {
            id = 1, name = "Elizabeth", last_name = "Jackson",
            age = 11, city = "New York",
        }, {
            id = 2, name = "Mary", last_name = "Brown",
            age = 22, city = "Los Angeles",
        }, {
            id = 3, name = "David", last_name = "Smith",
            age = 33, city = "Los Angeles",
        }, {
            id = 4, name = "William", last_name = "White",
            age = 44, city = "Chicago",
        }, {
            id = 5, name = "Jack", last_name = "Sparrow",
            age = 55, city = "London",
        }, {
            id = 6, name = "William", last_name = "Terner",
            age = 66, city = "Oxford",
        }, {
            id = 7, name = "Elizabeth", last_name = "Swan",
            age = 77, city = "Cambridge",
        }, {
            id = 8, name = "Hector", last_name = "Barbossa",
            age = 88, city = "London",
        },
    })

    table.sort(customers, function(obj1, obj2) return obj1.id < obj2.id end)

    -- no conditions
    -- first -4 after 6
    local first = -4
    local after = crud_utils.flatten(customers[6], g.space_format)
    local result, err = g.router:call('crud.select', {
        'customers', nil, {first = first, after = after, mode = 'write'},
    })

    t.assert_equals(err, nil)
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, helpers.get_objects_by_idxs(customers, {2, 3, 4, 5}))

    -- id >= 2
    -- first -2 after 5
    local conditions = {
        {'>=', 'id', 2},
    }
    local first = -2
    local after = crud_utils.flatten(customers[5], g.space_format)
    local result, err = g.router:call('crud.select', {
        'customers', conditions, {first = first, after = after, mode = 'write'},
    })

    t.assert_equals(err, nil)
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, helpers.get_objects_by_idxs(customers, {3, 4}))

    -- age >= 22
    -- first -2 after 5
    local conditions = {
        {'>=', 'age', 22},
    }
    local first = -2
    local after = crud_utils.flatten(customers[5], g.space_format)
    local result, err = g.router:call('crud.select', {
        'customers', conditions, {first = first, after = after, mode = 'write'},
    })

    t.assert_equals(err, nil)
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, helpers.get_objects_by_idxs(customers, {3, 4}))

    -- id <= 6
    -- first -2 after 5
    local conditions = {
        {'<=', 'id', 6},
    }
    local first = -2
    local after = crud_utils.flatten(customers[5], g.space_format)
    local result, err = g.router:call('crud.select', {
        'customers', conditions, {first = first, after = after, mode = 'write'},
    })

    t.assert_equals(err, nil)
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, helpers.get_objects_by_idxs(customers, {6}))

    -- age <= 66
    -- first -2 after 5
    local conditions = {
        {'<=', 'age', 66},
    }
    local first = -2
    local after = crud_utils.flatten(customers[5], g.space_format)
    local result, err = g.router:call('crud.select', {
        'customers', conditions, {first = first, after = after, mode = 'write'},
    })

    t.assert_equals(err, nil)
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, helpers.get_objects_by_idxs(customers, {6}))
end

pgroup.test_positive_first = function(g)
    local customers = helpers.insert_objects(g, 'customers', {
        {
            id = 1, name = "Elizabeth", last_name = "Jackson",
            age = 11, city = "New York",
        }, {
            id = 2, name = "Mary", last_name = "Brown",
            age = 22, city = "Los Angeles",
        }, {
            id = 3, name = "David", last_name = "Smith",
            age = 33, city = "Los Angeles",
        }, {
            id = 4, name = "William", last_name = "White",
            age = 44, city = "Chicago",
        }, {
            id = 5, name = "Jack", last_name = "Sparrow",
            age = 55, city = "London",
        }, {
            id = 6, name = "William", last_name = "Terner",
            age = 66, city = "Oxford",
        }, {
            id = 7, name = "Elizabeth", last_name = "Swan",
            age = 77, city = "Cambridge",
        }, {
            id = 8, name = "Hector", last_name = "Barbossa",
            age = 88, city = "London",
        },
    })

    table.sort(customers, function(obj1, obj2) return obj1.id < obj2.id end)

    -- id >= 3
    -- first 10 after 5
    local conditions = {
        {'>=', 'id', 3},
    }
    local first = 10
    local after = crud_utils.flatten(customers[5], g.space_format)
    local result, err = g.router:call('crud.select', {
        'customers', conditions, {first = first, after = after, mode = 'write'},
    })

    t.assert_equals(err, nil)
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, helpers.get_objects_by_idxs(customers, {6, 7, 8}))

    -- id <= 3
    -- first 10 after 5
    local conditions = {
        {'<=', 'id', 3},
    }
    local first = 10
    local after = crud_utils.flatten(customers[5], g.space_format)
    local result, err = g.router:call('crud.select', {
        'customers', conditions, {first = first, after = after, mode = 'write'},
    })

    t.assert_equals(err, nil)
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, helpers.get_objects_by_idxs(customers, {3, 2, 1}))

    -- id <= 6
    -- first 10 after 5
    local conditions = {
        {'<=', 'id', 6},
    }
    local first = 10
    local after = crud_utils.flatten(customers[5], g.space_format)
    local result, err = g.router:call('crud.select', {
        'customers', conditions, {first = first, after = after, mode = 'write'},
    })

    t.assert_equals(err, nil)
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, helpers.get_objects_by_idxs(customers, {4, 3, 2, 1}))

    -- id >= 6
    -- first 10 after 2
    local conditions = {
        {'>=', 'id', 6},
    }
    local first = 10
    local after = crud_utils.flatten(customers[2], g.space_format)
    local result, err = g.router:call('crud.select', {
        'customers', conditions, {first = first, after = after, mode = 'write'},
    })

    t.assert_equals(err, nil)
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, helpers.get_objects_by_idxs(customers, {6, 7, 8}))
end

pgroup.test_negative_first_with_batch_size = function(g)
    local customers = helpers.insert_objects(g, 'customers', {
        {
            id = 1, name = "Elizabeth", last_name = "Jackson",
            age = 11, city = "New York",
        }, {
            id = 2, name = "Mary", last_name = "Brown",
            age = 22, city = "Los Angeles",
        }, {
            id = 3, name = "David", last_name = "Smith",
            age = 33, city = "Los Angeles",
        }, {
            id = 4, name = "William", last_name = "White",
            age = 44, city = "Chicago",
        }, {
            id = 5, name = "Jack", last_name = "Sparrow",
            age = 55, city = "London",
        }, {
            id = 6, name = "William", last_name = "Terner",
            age = 66, city = "Oxford",
        }, {
            id = 7, name = "Elizabeth", last_name = "Swan",
            age = 77, city = "Cambridge",
        }, {
            id = 8, name = "Hector", last_name = "Barbossa",
            age = 88, city = "London",
        },
    })

    table.sort(customers, function(obj1, obj2) return obj1.id < obj2.id end)

    -- negative first w/o after
    local first = -10
    local result, err = g.router:call('crud.select', {
        'customers', nil, {first = first, mode = 'write'},
    })

    t.assert_equals(result, nil)
    t.assert_str_contains(err.err, "Negative first should be specified only with after option")

    -- no conditions
    -- first -3 after 5 (batch_size is 1)
    local first = -3
    local after = crud_utils.flatten(customers[5], g.space_format)
    local batch_size = 1
    local result, err = g.router:call('crud.select', {
        'customers', nil, {first = first, after = after, batch_size = batch_size, mode = 'write'},
    })

    t.assert_equals(err, nil)
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, helpers.get_objects_by_idxs(customers, {2, 3, 4}))

    -- id >= 2
    -- first -2 after 5 (batch_size is 1)
    local conditions = {
        {'>=', 'id', 2},
    }
    local first = -2
    local after = crud_utils.flatten(customers[5], g.space_format)
    local batch_size = 1
    local result, err = g.router:call('crud.select', {
        'customers', conditions, {first = first, after = after, batch_size = batch_size, mode = 'write'},
    })

    t.assert_equals(err, nil)
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, helpers.get_objects_by_idxs(customers, {3, 4}))

    -- age >= 22
    -- first -2 after 5 (batch_size is 1)
    local conditions = {
        {'>=', 'age', 22},
    }
    local first = -2
    local after = crud_utils.flatten(customers[5], g.space_format)
    local batch_size = 1
    local result, err = g.router:call('crud.select', {
        'customers', conditions, {first = first, after = after, batch_size = batch_size, mode = 'write'},
    })

    t.assert_equals(err, nil)
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, helpers.get_objects_by_idxs(customers, {3, 4}))

    -- id <= 6
    -- first -2 after 5 (batch_size is 1)
    local conditions = {
        {'<=', 'id', 6},
    }
    local first = -2
    local after = crud_utils.flatten(customers[5], g.space_format)
    local batch_size = 1
    local result, err = g.router:call('crud.select', {
        'customers', conditions, {first = first, after = after, batch_size = batch_size, mode = 'write'},
    })

    t.assert_equals(err, nil)
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, helpers.get_objects_by_idxs(customers, {6}))

    -- age <= 66
    -- first -2 after 5 (batch_size is 1)
    local conditions = {
        {'<=', 'age', 66},
    }
    local first = -2
    local after = crud_utils.flatten(customers[5], g.space_format)
    local batch_size = 1
    local result, err = g.router:call('crud.select', {
       'customers', conditions, {first = first, after = after, batch_size = batch_size, mode = 'write'},
    })

    t.assert_equals(err, nil)
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, helpers.get_objects_by_idxs(customers, {6}))
end

pgroup.test_select_all_with_batch_size = function(g)
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
        }, {
            id = 5, name = "Jack", last_name = "Sparrow",
            age = 35, city = "London",
        }, {
            id = 6, name = "William", last_name = "Terner",
            age = 25, city = "Oxford",
        }, {
            id = 7, name = "Elizabeth", last_name = "Swan",
            age = 18, city = "Cambridge",
        }, {
            id = 8, name = "Hector", last_name = "Barbossa",
            age = 45, city = "London",
        },
    })

    table.sort(customers, function(obj1, obj2) return obj1.id < obj2.id end)

    -- batch size 1
    local result, err = g.router:call('crud.select', {
        'customers', nil, {batch_size = 1, fullscan = true, mode = 'write'},
    })

    t.assert_equals(err, nil)
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, customers)

    -- batch size 3
    local result, err = g.router:call('crud.select', {
        'customers', nil, {batch_size = 3, fullscan = true, mode = 'write'},
    })

    t.assert_equals(err, nil)
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, customers)

    -- batch size 3 and first 6
    local result, err = g.router:call('crud.select', {
        'customers', nil, {batch_size = 3, first = 6, mode = 'write'},
    })

    t.assert_equals(err, nil)
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, helpers.get_objects_by_idxs(customers, {1, 2, 3, 4, 5, 6}))
end

pgroup.test_select_by_primary_index = function(g)
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
        },
    })

    table.sort(customers, function(obj1, obj2) return obj1.id < obj2.id end)

    local conditions = {{'==', 'id_index', 3}}
    local result, err = g.router:eval([[
        local crud = require('crud')

        local conditions = ...

        local result, err = crud.select('customers', conditions, {mode = 'write'})
        return result, err
    ]], {conditions})

    t.assert_equals(err, nil)
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, helpers.get_objects_by_idxs(customers, {3}))
end

pgroup.test_eq_condition_with_index = function(g)
    local customers = helpers.insert_objects(g, 'customers', {
        {
            id = 1, name = "Elizabeth", last_name = "Jackson",
            age = 33, city = "New York",
        }, {
            id = 2, name = "Mary", last_name = "Brown",
            age = 46, city = "Los Angeles",
        }, {
            id = 3, name = "David", last_name = "Smith",
            age = 33, city = "Los Angeles",
        }, {
            id = 4, name = "William", last_name = "Smith",
            age = 81, city = "Chicago",
        },{
            id = 5, name = "Hector", last_name = "Barbossa",
            age = 33, city = "Chicago",
        },{
            id = 6, name = "William", last_name = "White",
            age = 81, city = "Chicago",
        },{
            id = 7, name = "Jack", last_name = "Sparrow",
            age = 33, city = "Chicago",
        }, {
            id = 8, name = "Nick", last_name = "Smith",
            age = 20, city = "London",
        }
    })

    table.sort(customers, function(obj1, obj2) return obj1.id < obj2.id end)

    local conditions = {
        {'==', 'age_index', 33},
    }

    -- no after
    local result, err = g.router:call('crud.select', {
        'customers', conditions, {mode = 'write'},
    })

    t.assert_equals(err, nil)
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, helpers.get_objects_by_idxs(customers, {1, 3, 5, 7})) -- in id order

    -- after obj 3
    local after = crud_utils.flatten(customers[3], g.space_format)
    local result, err = g.router:call('crud.select', {
        'customers', conditions, {after = after, mode = 'write'},
    })

    t.assert_equals(err, nil)
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, helpers.get_objects_by_idxs(customers, {5, 7})) -- in id order

    -- after obj 5 with negative first
    local after = crud_utils.flatten(customers[5], g.space_format)
    local result, err = g.router:call('crud.select', {
        'customers', conditions, {after = after, first = -10, mode = 'write'},
    })

    t.assert_equals(err, nil)
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, helpers.get_objects_by_idxs(customers, {1, 3})) -- in id order

    -- after obj 8
    local after = crud_utils.flatten(customers[8], g.space_format)
    local result, err = g.router:call('crud.select', {
        'customers', conditions, {after = after, first = 10, mode = 'write'},
    })

    t.assert_equals(err, nil)
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, helpers.get_objects_by_idxs(customers, {1, 3, 5, 7})) -- in id order

    -- after obj 8 with negative first
    local after = crud_utils.flatten(customers[8], g.space_format)
    local result, err = g.router:call('crud.select', {
        'customers', conditions, {after = after, first = -10, mode = 'write'},
    })

    t.assert_equals(err, nil)
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, helpers.get_objects_by_idxs(customers, {}))

    -- after obj 2
    local after = crud_utils.flatten(customers[2], g.space_format)
    local result, err = g.router:call('crud.select', {
        'customers', conditions, {after = after, first = 10, mode = 'write'},
    })

    t.assert_equals(err, nil)
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, helpers.get_objects_by_idxs(customers, {}))

    -- after obj 2 with negative first
    local after = crud_utils.flatten(customers[2], g.space_format)
    local result, err = g.router:call('crud.select', {
        'customers', conditions, {after = after, first = -10, mode = 'write'},
    })

    t.assert_equals(err, nil)
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, helpers.get_objects_by_idxs(customers, {1, 3, 5, 7})) -- in id order
end

pgroup.test_ge_condition_with_index = function(g)
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

    local conditions = {
        {'>=', 'age_index', 33},
    }

    -- no after
    local result, err = g.router:call('crud.select', {
        'customers', conditions, {fullscan = true, mode = 'write'},
    })

    t.assert_equals(err, nil)
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, helpers.get_objects_by_idxs(customers, {3, 2, 4})) -- in age order

    -- after obj 3
    local after = crud_utils.flatten(customers[3], g.space_format)
    local result, err = g.router:call('crud.select', {
        'customers', conditions, {after = after, fullscan = true, mode = 'write'},
    })

    t.assert_equals(err, nil)
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, helpers.get_objects_by_idxs(customers, {2, 4})) -- in age order
end

pgroup.test_le_condition_with_index = function(g)
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

    local conditions = {
        {'<=', 'age_index', 33},
    }

    -- no after
    local result, err = g.router:call('crud.select', {
        'customers', conditions, {fullscan = true, mode = 'write'},
    })

    t.assert_equals(err, nil)
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, helpers.get_objects_by_idxs(customers, {3, 1})) -- in age order

    -- after obj 3
    local after = crud_utils.flatten(customers[3], g.space_format)
    local result, err = g.router:call('crud.select', {
        'customers', conditions, {after = after, fullscan = true, mode = 'write'},
    })

    t.assert_equals(err, nil)
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, helpers.get_objects_by_idxs(customers, {1})) -- in age order

    -- after obj 2
    local after = crud_utils.flatten(customers[2], g.space_format)
    local result, err = g.router:call('crud.select', {
        'customers', conditions, {after = after, mode = 'write'},
    })

    t.assert_equals(err, nil)
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, helpers.get_objects_by_idxs(customers, {3, 1})) -- in age order
end

pgroup.test_lt_condition_with_index = function(g)
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

    local conditions = {
        {'<', 'age_index', 33},
    }

    -- no after
    local result, err = g.router:call('crud.select', {
        'customers', conditions, {fullscan = true, mode = 'write'},
    })

    t.assert_equals(err, nil)
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, helpers.get_objects_by_idxs(customers, {1})) -- in age order

    -- after obj 1
    local after = crud_utils.flatten(customers[1], g.space_format)
    local result, err = g.router:call('crud.select', {
        'customers', conditions, {after = after, fullscan = true, mode = 'write'},
    })

    t.assert_equals(err, nil)
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, helpers.get_objects_by_idxs(customers, {})) -- in age order
end

pgroup.test_multiple_conditions = function(g)
    local customers = helpers.insert_objects(g, 'customers', {
        {
            id = 1, name = "Elizabeth", last_name = "Rodriguez",
            age = 20, city = "Los Angeles",
        }, {
            id = 2, name = "Elizabeth", last_name = "Rodriguez",
            age = 44, city = "Chicago",
        }, {
            id = 3, name = "Elizabeth", last_name = "Rodriguez",
            age = 22, city = "New York",
        }, {
            id = 4, name = "David", last_name = "Brown",
            age = 23, city = "Los Angeles",
        }, {
            id = 5, name = "Elizabeth", last_name = "Rodriguez",
            age = 39, city = "Chicago",
        }
    })

    table.sort(customers, function(obj1, obj2) return obj1.id < obj2.id end)

    local conditions = {
        {'>', 'age', 20},
        {'==', 'name', 'Elizabeth'},
        {'==', 'city', 'Chicago'},
    }

    -- no after
    local result, err = g.router:call('crud.select', {
        'customers', conditions, {mode = 'write'},
    })

    t.assert_equals(err, nil)
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, helpers.get_objects_by_idxs(customers, {5, 2})) -- in age order

    -- after obj 5
    local after = crud_utils.flatten(customers[5], g.space_format)
    local result, err = g.router:call('crud.select', {
        'customers', conditions, {after = after, fullscan = true, mode = 'write'},
    })

    t.assert_equals(err, nil)
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, helpers.get_objects_by_idxs(customers, {2})) -- in age order
end

pgroup.test_composite_index = function(g)
    local customers = helpers.insert_objects(g, 'customers', {
        {
            id = 1, name = "Elizabeth", last_name = "Rodriguez",
            age = 20, city = "Los Angeles",
        }, {
            id = 2, name = "Elizabeth", last_name = "Johnson",
            age = 44, city = "Los Angeles",
        }, {
            id = 3, name = "David", last_name = "Brown",
            age = 23, city = "Chicago",
        }, {
            id = 4, name = "Jessica", last_name = "Jones",
            age = 22, city = "New York",
        }
    })

    table.sort(customers, function(obj1, obj2) return obj1.id < obj2.id end)

    local conditions = {
        {'>=', 'full_name', {"Elizabeth", "Jo"}},
    }

    -- no after
    local result, err = g.router:call('crud.select', {
        'customers', conditions, {fullscan = true, mode = 'write'},
    })

    t.assert_equals(err, nil)
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, helpers.get_objects_by_idxs(customers, {2, 1, 4})) -- in full_name order

    -- after obj 2
    local after = crud_utils.flatten(customers[2], g.space_format)
    local result, err = g.router:call('crud.select', {
        'customers', conditions, {after = after, fullscan = true, mode = 'write'},
    })

    t.assert_equals(err, nil)
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, helpers.get_objects_by_idxs(customers, {1, 4})) -- in full_name order

    -- partial value in conditions
    local conditions = {
        {'==', 'full_name', "Elizabeth"},
    }

    local result, err = g.router:call('crud.select', {
        'customers', conditions, {mode = 'write'},
    })

    t.assert_equals(err, nil)
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, helpers.get_objects_by_idxs(customers, {2, 1})) -- in full_name order

    -- first 1
    local result, err = g.router:call('crud.select', {
        'customers', conditions, {first = 1, mode = 'write'},
    })
    t.assert_equals(err, nil)
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, helpers.get_objects_by_idxs(customers, {2})) -- in full_name order

    -- first 1 with full specified key
    local result, err = g.router:call('crud.select', {
        'customers', {{'==', 'full_name', {'Elizabeth', 'Johnson'}}}, {first = 1,mode = 'write'},
    })
    t.assert_equals(err, nil)
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, helpers.get_objects_by_idxs(customers, {2})) -- in full_name order
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

    local result, err = g.router:call('crud.select', {
        'book_translation', conditions, {mode = 'write'},
    })
    t.assert_equals(err, nil)
    t.assert_equals(#result.rows, 1)

    local result, err = g.router:call('crud.select', {
        'book_translation', conditions, {first = 2, mode = 'write'},
    })
    t.assert_equals(err, nil)
    t.assert_equals(#result.rows, 1)

    local result, err = g.router:call('crud.select', {
        'book_translation', conditions, {first = 1, mode = 'write'},
    })
    t.assert_equals(err, nil)
    t.assert_equals(#result.rows, 1)

    local result, err = g.router:call('crud.select', {
        'book_translation', conditions, {first = 1, after = result.rows[1], mode = 'write'},
    })
    t.assert_equals(err, nil)
    t.assert_equals(#result.rows, 0)
end

pgroup.test_select_with_batch_size_1 = function(g)
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
        }, {
            id = 5, name = "Jack", last_name = "Sparrow",
            age = 35, city = "London",
        }, {
            id = 6, name = "William", last_name = "Terner",
            age = 25, city = "Oxford",
        }, {
            id = 7, name = "Elizabeth", last_name = "Swan",
            age = 18, city = "Cambridge",
        }, {
            id = 8, name = "Hector", last_name = "Barbossa",
            age = 45, city = "London",
        },
    })

    table.sort(customers, function(obj1, obj2) return obj1.id < obj2.id end)

    -- LE
    local conditions = {{'<=', 'age', 35}}
    local result, err = g.router:call('crud.select', {
        'customers', conditions, {batch_size = 1, fullscan = true, mode = 'write'},
    })

    t.assert_equals(err, nil)
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, helpers.get_objects_by_idxs(customers, {5, 3, 6, 7, 1}))

    -- LT
    local conditions = {{'<', 'age', 35}}
    local result, err = g.router:call('crud.select', {
        'customers', conditions, {batch_size = 1, fullscan = true, mode = 'write'},
    })

    t.assert_equals(err, nil)
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, helpers.get_objects_by_idxs(customers, {3, 6, 7, 1}))

    -- GE
    local conditions = {{'>=', 'age', 35}}
    local result, err = g.router:call('crud.select', {
        'customers', conditions, {batch_size = 1, fullscan = true, mode = 'write'},
    })

    t.assert_equals(err, nil)
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, helpers.get_objects_by_idxs(customers, {5, 8, 2, 4}))

    -- GT
    local conditions = {{'>', 'age', 35}}
    local result, err = g.router:call('crud.select', {
        'customers', conditions, {batch_size = 1, fullscan = true, mode = 'write'},
    })

    t.assert_equals(err, nil)
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, helpers.get_objects_by_idxs(customers, {8, 2, 4}))
end

pgroup.test_select_by_full_sharding_key = function(g)
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
        },
    })

    table.sort(customers, function(obj1, obj2) return obj1.id < obj2.id end)

    local conditions = {{'==', 'id', 3}}
    local result, err = g.router:call('crud.select', {
        'customers', conditions, {mode = 'write'},
    })

    t.assert_equals(err, nil)
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, helpers.get_objects_by_idxs(customers, {3}))
end

pgroup.test_select_with_collations = function(g)
    local customers = helpers.insert_objects(g, 'customers', {
        {
            id = 1, name = "Elizabeth", last_name = "Jackson",
            age = 12, city = "Oxford",
        }, {
            id = 2, name = "Mary", last_name = "Brown",
            age = 46, city = "oxford",
        }, {
            id = 3, name = "elizabeth", last_name = "brown",
            age = 46, city = "Oxford",
        }, {
            id = 4, name = "Jack", last_name = "Sparrow",
            age = 35, city = "oxford",
        }, {
            id = 5, name = "William", last_name = "Terner",
            age = 25, city = "Oxford",
        }, {
            id = 6, name = "elizabeth", last_name = "Brown",
            age = 33, city = "Los Angeles",
        },
    })

    table.sort(customers, function(obj1, obj2) return obj1.id < obj2.id end)

    -- full name index - unicode ci collation (case-insensitive)
    local conditions = {{'==', 'name', "Elizabeth"}}
    local result, err = g.router:call('crud.select', {
        'customers', conditions, {mode = 'write'},
    })

    t.assert_equals(err, nil)
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, helpers.get_objects_by_idxs(customers, {3, 6, 1}))

    -- city - no collation (case-sensitive)
    local conditions = {{'==', 'city', "oxford"}}
    local result, err = g.router:call('crud.select', {
        'customers', conditions, {mode = 'write'},
    })

    t.assert_equals(err, nil)
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, helpers.get_objects_by_idxs(customers, {2, 4}))
end

pgroup.test_multipart_primary_index = function(g)
    local coords = helpers.insert_objects(g, 'coord', {
        { x = 0, y = 0 }, -- 1
        { x = 0, y = 1 }, -- 2
        { x = 0, y = 2 }, -- 3
        { x = 1, y = 3 }, -- 4
        { x = 1, y = 4 }, -- 5
    })

    local conditions = {{'=', 'primary', 0}}
    local result_0, err = g.router:call('crud.select', {
        'coord', conditions, {mode = 'write'},
    })
    t.assert_equals(err, nil)
    local objects = crud.unflatten_rows(result_0.rows, result_0.metadata)
    t.assert_equals(objects, helpers.get_objects_by_idxs(coords, {1, 2, 3}))

    local result, err = g.router:call('crud.select', {
        'coord', conditions, {after = result_0.rows[1], mode = 'write'},
    })
    t.assert_equals(err, nil)
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, helpers.get_objects_by_idxs(coords, {2, 3}))

    local result, err = g.router:call('crud.select', {
        'coord', conditions, {after = result_0.rows[3], first = -2, mode = 'write'},
    })
    t.assert_equals(err, nil)
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, helpers.get_objects_by_idxs(coords, {1, 2}))

    local new_conditions = {{'=', 'y', 1}, {'=', 'primary', 0}}
    local result, err = g.router:call('crud.select', {
        'coord', new_conditions, {after = result_0.rows[3], first = -2, mode = 'write'},
    })
    t.assert_equals(err, nil)
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, helpers.get_objects_by_idxs(coords, {2}))

    local conditions = {{'=', 'primary', {0, 2}}}
    local result, err = g.router:call('crud.select', {
        'coord', conditions, {mode = 'write'},
    })
    t.assert_equals(err, nil)
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, helpers.get_objects_by_idxs(coords, {3}))

    local conditions_ge = {{'>=', 'primary', 0}}
    local result_ge_0, err = g.router:call('crud.select', {
        'coord', conditions_ge, {fullscan = true, mode = 'write'},
    })
    t.assert_equals(err, nil)
    local objects = crud.unflatten_rows(result_ge_0.rows, result_ge_0.metadata)
    t.assert_equals(objects, helpers.get_objects_by_idxs(coords, {1, 2, 3, 4, 5}))

    local result, err = g.router:call('crud.select', {
        'coord', conditions_ge, {after = result_ge_0.rows[1], fullscan = true, mode = 'write'},
    })
    t.assert_equals(err, nil)
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, helpers.get_objects_by_idxs(coords, {2, 3, 4, 5}))

    local result, err = g.router:call('crud.select', {
        'coord', conditions_ge, {after = result_ge_0.rows[3], first = -3, mode = 'write'},
    })
    t.assert_equals(err, nil)
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, helpers.get_objects_by_idxs(coords, {1, 2}))
end

pgroup.test_select_partial_result_bad_input = function(g)
    helpers.insert_objects(g, 'customers', {
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

    local conditions = {{'>=', 'age', 33}}
    local result, err = g.router:call('crud.select', {
        'customers', conditions, {fields = {'id', 'mame'}, fullscan = true, mode = 'write'},
    })

    t.assert_equals(result, nil)
    t.assert_str_contains(err.err, 'Space format doesn\'t contain field named "mame"')
end

pgroup.test_select_partial_result = function(g)
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

    -- condition by indexed non-unique non-primary field (age):
    local conditions = {{'>=', 'age', 33}}

    -- condition field is not in opts.fields
    local fields = {'name', 'city'}

    -- result doesn't contain primary key, result tuples are sorted by field+primary
    -- in age + id order
    local expected_customers = {
        {id = 3, age = 33, name = "David", city = "Los Angeles"},
        {id = 2, age = 46, name = "Mary", city = "London"},
        {id = 4, age = 46, name = "William", city = "Chicago"},
    }

    local result, err = g.router:call('crud.select', {
        'customers', conditions, {fields = fields, fullscan = true, mode = 'write'},
    })

    t.assert_equals(err, nil)
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, expected_customers)

    -- same case with after option
    expected_customers = {
        {id = 2, age = 46, name = "Mary", city = "London"},
        {id = 4, age = 46, name = "William", city = "Chicago"},
    }

    result, err = g.router:call('crud.select', {
        'customers', conditions, {after = result.rows[1], fields = fields, fullscan = true, mode = 'write'},
    })

    t.assert_equals(err, nil)
    objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, expected_customers)

    -- condition field is in opts.fields
    fields = {'name', 'age'}

    -- result doesn't contain primary key, result tuples are sorted by field+primary
    -- in age + id order
    expected_customers = {
        {id = 3, age = 33, name = "David"},
        {id = 2, age = 46, name = "Mary"},
        {id = 4, age = 46, name = "William"},
    }

    result, err = g.router:call('crud.select', {
        'customers', conditions, {fields = fields, fullscan = true, mode = 'write'},
    })

    t.assert_equals(err, nil)
    objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, expected_customers)

    -- same case with after option
    expected_customers = {
        {id = 2, age = 46, name = "Mary"},
        {id = 4, age = 46, name = "William"},
    }

    result, err = g.router:call('crud.select', {
        'customers', conditions, {after = result.rows[1], fields = fields, fullscan = true, mode = 'write'},
    })

    t.assert_equals(err, nil)
    objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, expected_customers)

    -- condition by non-indexed non-unique non-primary field (city):
    conditions = {{'>=', 'city', 'Lo'}}

    -- condition field is not in opts.fields
    fields = {'name', 'age'}

    -- result doesn't contain primary key, result tuples are sorted by primary
    -- in id order
    expected_customers = {
        {id = 1, name = "Elizabeth", age = 12},
        {id = 2, name = "Mary", age = 46},
        {id = 3, name = "David", age = 33},
    }

    result, err = g.router:call('crud.select', {
        'customers', conditions, {fields = fields, fullscan = true, mode = 'write'},
    })

    t.assert_equals(err, nil)
    objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, expected_customers)

    -- same case with after option
    expected_customers = {
        {id = 2, name = "Mary", age = 46},
        {id = 3, name = "David", age = 33},
    }

    result, err = g.router:call('crud.select', {
        'customers', conditions, {after = result.rows[1], fields = fields, fullscan = true, mode = 'write'},
    })

    t.assert_equals(err, nil)
    objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, expected_customers)

    -- condition field is in opts.fields
    fields = {'name', 'city'}

    -- result doesn't contain primary key, result tuples are sorted by primary
    -- in id order
    expected_customers = {
        {id = 1, name = "Elizabeth", city = "Los Angeles"},
        {id = 2, name = "Mary", city = "London"},
        {id = 3, name = "David", city = "Los Angeles"},
    }

    result, err = g.router:call('crud.select', {
        'customers', conditions, {fields = fields, fullscan = true, mode = 'write'},
    })

    t.assert_equals(err, nil)
    objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, expected_customers)

    -- same case with after option
    expected_customers = {
        {id = 2, name = "Mary", city = "London"},
        {id = 3, name = "David", city = "Los Angeles"},
    }

    result, err = g.router:call('crud.select', {
        'customers', conditions, {after = result.rows[1], fields = fields, fullscan = true, mode = 'write'},
    })

    t.assert_equals(err, nil)
    objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, expected_customers)
end

pgroup.test_cut_selected_rows = function(g)
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

    -- condition by indexed non-unique non-primary field (age):
    local conditions = {{'>=', 'age', 33}}

    -- condition field is not in opts.fields
    local fields = {'name', 'city'}

    local expected_customers = {
        {name = "David", city = "Los Angeles"},
        {name = "Mary", city = "London"},
        {name = "William", city = "Chicago"},
    }

    -- with fields option
    local result, err = g.router:call('crud.select', {
        'customers', conditions, {fields = fields, fullscan = true, mode = 'write'},
    })

    t.assert_equals(err, nil)

    result, err = g.router:call('crud.cut_rows', {result.rows, result.metadata, fields})

    t.assert_equals(err, nil)
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, expected_customers)

    -- without fields option

    -- fields should be in metadata order if we want to work with cut_rows
    fields = {'id', 'bucket_id', 'name'}

    expected_customers = {
        {bucket_id = 2804, id = 3, name = "David"},
        {bucket_id = 401, id = 2, name = "Mary"},
        {bucket_id = 1161, id = 4, name = "William"},
    }

    local result, err = g.router:call('crud.select', {
        'customers', conditions, {fullscan = true, mode = 'write'},
    })

    t.assert_equals(err, nil)

    result, err = g.router:call('crud.cut_rows', {result.rows, result.metadata, fields})

    t.assert_equals(err, nil)
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, expected_customers)
end

pgroup.test_select_force_map_call = function(g)
    local key = 1

    local first_bucket_id = g.router:eval([[
        local vshard = require('vshard')

        local key = ...
        return vshard.router.bucket_id_strcrc32(key)
    ]], {key})

    local second_bucket_id, err = helpers.get_other_storage_bucket_id(g.cluster, first_bucket_id)

    t.assert_equals(err, nil)

    local customers = helpers.insert_objects(g, 'customers', {
        {
            id = key, bucket_id = first_bucket_id, name = "Elizabeth", last_name = "Jackson",
            age = 12, city = "New York",
        }, {
            id = key, bucket_id = second_bucket_id, name = "Mary", last_name = "Brown",
            age = 46, city = "Los Angeles",
        },
    })

    table.sort(customers, function(obj1, obj2) return obj1.bucket_id < obj2.bucket_id end)

    local result, err = g.router:call('crud.select', {
        'customers', {{'==', 'id', key}}, {mode = 'write'},
    })

    t.assert_equals(err, nil)
    t.assert_equals(#result.rows, 1)

    result, err = g.router:call('crud.select', {
        'customers', {{'==', 'id', key}}, {force_map_call = true, mode = 'write'},
    })

    t.assert_equals(err, nil)

    local objects = crud.unflatten_rows(result.rows, result.metadata)
    table.sort(objects, function(obj1, obj2) return obj1.bucket_id < obj2.bucket_id end)
    t.assert_equals(objects, customers)
end

pgroup.test_jsonpath = function(g)
    helpers.insert_objects(g, 'developers', {
        {
            id = 1, name = "Alexey", last_name = "Smith",
            age = 20, additional = { a = { b = 140 } },
        }, {
            id = 2, name = "Sergey", last_name = "Choppa",
            age = 21, additional = { a = { b = 120 } },
        }, {
            id = 3, name = "Mikhail", last_name = "Crossman",
            age = 42, additional = {},
        }, {
            id = 4, name = "Pavel", last_name = "White",
            age = 51, additional = { a = { b = 50 } },
        }, {
            id = 5, name = "Tatyana", last_name = "May",
            age = 17, additional = { a = 55 },
        },
    })

    local result, err = g.router:call('crud.select', {
        'developers', {{'>=', '[5]', 40}}, {fields = {'name', 'last_name'}, fullscan = true, mode = 'write'},
    })
    t.assert_equals(err, nil)

    local objects = crud.unflatten_rows(result.rows, result.metadata)
    local expected_objects = {
        {id = 3, name = "Mikhail", last_name = "Crossman"},
        {id = 4, name = "Pavel", last_name = "White"},
    }
    t.assert_equals(objects, expected_objects)

    local result, err = g.router:call('crud.select', {
        'developers', {{'<', '["age"]', 21}}, {fields = {'name', 'last_name'}, fullscan = true, mode = 'write'},
    })
    t.assert_equals(err, nil)

    local objects = crud.unflatten_rows(result.rows, result.metadata)
    local expected_objects = {
        {id = 1, name = "Alexey", last_name = "Smith"},
        {id = 5, name = "Tatyana", last_name = "May"},
    }
    t.assert_equals(objects, expected_objects)

    local result, err = g.router:call('crud.select', {
        'developers', {{'>=', '[6].a.b', 55}}, {fields = {'name', 'last_name'}, fullscan = true, mode = 'write'},
    })
    t.assert_equals(err, nil)

    local objects = crud.unflatten_rows(result.rows, result.metadata)
    local expected_objects = {
        {id = 1, name = "Alexey", last_name = "Smith"},
        {id = 2, name = "Sergey", last_name = "Choppa"},
    }
    t.assert_equals(objects, expected_objects)
end

pgroup.test_jsonpath_index_field = function(g)
    t.skip_if(
        not crud_utils.tarantool_supports_jsonpath_indexes(),
        "Jsonpath indexes supported since 2.6.3/2.7.2/2.8.1"
    )

    helpers.insert_objects(g, 'cars', {
        {
            id = {car_id = {signed = 1}},
            age = 2,
            manufacturer = 'VAG',
            data = {car = { model = 'BMW', color = 'Black' }},
        },
        {
            id = {car_id = {signed = 2}},
            age = 5,
            manufacturer = 'FIAT',
            data = {car = { model = 'Cadillac', color = 'White' }},
        },
        {
            id = {car_id = {signed = 3}},
            age = 17,
            manufacturer = 'Ford',
            data = {car = { model = 'BMW', color = 'Yellow' }},
        },
        {
            id = {car_id = {signed = 4}},
            age = 3,
            manufacturer = 'General Motors',
            data = {car = { model = 'Mercedes', color = 'Yellow' }},
        },
    })

    -- PK jsonpath index
    local result, err = g.router:call('crud.select', {
        'cars', {{'<=', 'id_ind', 3}, {'<=', 'age', 5}}, {fields = {'id', 'age'}, fullscan = true, mode = 'write'},
    })
    t.assert_equals(err, nil)

    local objects = crud.unflatten_rows(result.rows, result.metadata)
    local expected_objects = {
    {
        id = {car_id = {signed = 2}},
        age = 5,
    },
    {
        id = {car_id = {signed = 1}},
        age = 2,
    }}

    t.assert_equals(objects, expected_objects)

    -- Secondary jsonpath index (partial)
    local result, err = g.router:call('crud.select', {
        'cars', {{'==', 'data_index', 'Yellow'}}, {fields = {'id', 'age'}, mode = 'write'},
    })
    t.assert_equals(err, nil)

    local objects = crud.unflatten_rows(result.rows, result.metadata)
    local expected_objects = {
    {
        id = {car_id = {signed = 3}},
        age = 17,
        data = {car = { model = 'BMW', color = 'Yellow' }},
    },
    {
        id = {car_id = {signed = 4}},
        age = 3,
        data = {car = { model = 'Mercedes', color = 'Yellow' }}
    }}

    t.assert_equals(objects, expected_objects)

    -- Secondary jsonpath index (full)
    local result, err = g.router:call('crud.select', {
        'cars', {{'==', 'data_index', {'Yellow', 'Mercedes'}}}, {fields = {'id', 'age'}, mode = 'write'},
    })
    t.assert_equals(err, nil)

    local objects = crud.unflatten_rows(result.rows, result.metadata)
    local expected_objects = {
    {
        id = {car_id = {signed = 4}},
        age = 3,
        data = {car = { model = 'Mercedes', color = 'Yellow' }}
    }}

    t.assert_equals(objects, expected_objects)
end

pgroup.test_jsonpath_index_field_pagination = function(g)
    t.skip_if(
        not crud_utils.tarantool_supports_jsonpath_indexes(),
        "Jsonpath indexes supported since 2.6.3/2.7.2/2.8.1"
    )

    local cars = helpers.insert_objects(g, 'cars', {
        {
            id = {car_id = {signed = 1}},
            age = 5,
            manufacturer = 'VAG',
            data = {car = { model = 'A', color = 'Yellow' }},
        },
        {
            id = {car_id = {signed = 2}},
            age = 17,
            manufacturer = 'FIAT',
            data = {car = { model = 'B', color = 'Yellow' }},
        },
        {
            id = {car_id = {signed = 3}},
            age = 5,
            manufacturer = 'Ford',
            data = {car = { model = 'C', color = 'Yellow' }},
        },
        {
            id = {car_id = {signed = 4}},
            age = 3,
            manufacturer = 'General Motors',
            data = {car = { model = 'D', color = 'Yellow' }},
        },
        {
            id = {car_id = {signed = 5}},
            age = 3,
            manufacturer = 'General Motors',
            data = {car = { model = 'E', color = 'Yellow' }},
        },
        {
            id = {car_id = {signed = 6}},
            age = 3,
            manufacturer = 'General Motors',
            data = {car = { model = 'F', color = 'Yellow' }},
        },
        {
            id = {car_id = {signed = 7}},
            age = 3,
            manufacturer = 'General Motors',
            data = {car = { model = 'G', color = 'Yellow' }},
        },
        {
            id = {car_id = {signed = 8}},
            age = 3,
            manufacturer = 'General Motors',
            data = {car = { model = 'H', color = 'Yellow' }},
        },
    })


    -- Pagination (primary index)
    local result, err = g.router:call('crud.select', {
        'cars', nil, {first = 2, mode = 'write'},
    })
    t.assert_equals(err, nil)

    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, helpers.get_objects_by_idxs(cars, {1, 2}))

    local result, err = g.router:call('crud.select', {
        'cars', nil, {first = 2, after = result.rows[2], mode = 'write'},
    })
    t.assert_equals(err, nil)

    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, helpers.get_objects_by_idxs(cars, {3, 4}))

    -- Reverse pagination (primary index)
    local result, err = g.router:call('crud.select', {
        'cars', nil, {first = -2, after = result.rows[1], mode = 'write'},
    })
    t.assert_equals(err, nil)

    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, helpers.get_objects_by_idxs(cars, {1, 2}))

    -- Pagination (secondary index - 1 field)
    local result, err = g.router:call('crud.select', {
        'cars', {{'==', 'data_index', 'Yellow'}}, {first = 2, mode = 'write'},
    })
    t.assert_equals(err, nil)

    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, helpers.get_objects_by_idxs(cars, {1, 2}))

    local result, err = g.router:call('crud.select', {
        'cars', {{'==', 'data_index', 'Yellow'}}, {first = 2, after = result.rows[2], mode = 'write'},
    })
    t.assert_equals(err, nil)

    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, helpers.get_objects_by_idxs(cars, {3, 4}))

    -- Reverse pagination (secondary index - 1 field)
    local result, err = g.router:call('crud.select', {
        'cars', {{'==', 'data_index', 'Yellow'}}, {first = -2, after = result.rows[1], mode = 'write'},
    })
    t.assert_equals(err, nil)

    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, helpers.get_objects_by_idxs(cars, {1, 2}))

    -- Pagination (secondary index - 2 fields)
    local result, err = g.router:call('crud.select', {
        'cars', {{'>=', 'data_index', {'Yellow', 'E'}}}, {first = 2, mode = 'write'},
    })
    t.assert_equals(err, nil)

    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, helpers.get_objects_by_idxs(cars, {5, 6}))

    local result, err = g.router:call('crud.select', {
        'cars', {{'>=', 'data_index', {'Yellow', 'E'}}}, {first = 2, after = result.rows[2], mode = 'write'},
    })
    t.assert_equals(err, nil)

    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, helpers.get_objects_by_idxs(cars, {7, 8}))

    local result, err = g.router:call('crud.select', {
        'cars',
        {{'>=', 'data_index', {'Yellow', 'B'}}, {'<=', 'id_ind', 3}},
        {first = -3, after = result.rows[1], mode = 'write'},
    })
    t.assert_equals(err, nil)

    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, helpers.get_objects_by_idxs(cars, {2, 3}))
end

pgroup.test_select_timeout = function(g)
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

    local result, err = g.router:call('crud.select', {
        'customers', nil, {timeout = 1, fullscan = true, mode = 'write'},
    })

    t.assert_equals(err, nil)
    t.assert_equals(result.rows, {
        {1, 477, "Elizabeth", "Jackson", 12, "New York"},
        {2, 401, "Mary", "Brown", 46, "Los Angeles"},
        {3, 2804, "David", "Smith", 33, "Los Angeles"},
        {4, 1161, "William", "White", 81, "Chicago"},
    })
end

pgroup.test_opts_not_damaged = function(g)
    local customers = helpers.insert_objects(g, 'customers', {
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

    table.sort(customers, function(obj1, obj2) return obj1.id < obj2.id end)

    -- after tuple should be in `fields` format + primary key
    local fields = {'name', 'age'}
    local after = {"Mary", 46, 2}

    local select_opts = {
        timeout = 1, bucket_id = 1161,
        batch_size = 105, first = 2, after = after,
        fields = fields, mode = 'read', prefer_replica = false,
        balance = false, force_map_call = false,
    }
    local new_select_opts, err = g.router:eval([[
         local crud = require('crud')

         local select_opts = ...

         local _, err = crud.select('customers', nil, select_opts)

         return select_opts, err
     ]], {select_opts})

    t.assert_equals(err, nil)
    t.assert_equals(new_select_opts, select_opts)
end

-- gh-220: bucket_id argument is ignored when it cannot be deduced
-- from provided select/pairs conditions.
pgroup.test_select_no_map_reduce = function(g)
    local customers = helpers.insert_objects(g, 'customers', {
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

    table.sort(customers, function(obj1, obj2) return obj1.id < obj2.id end)

    local router = g.router.net_box
    local map_reduces_before = helpers.get_map_reduces_stat(router, 'customers')

    -- Case: no conditions, just bucket id.
    local result, err = g.router:call('crud.select', {
        'customers',
        nil,
        {bucket_id = 2804, timeout = 1, fullscan = true, mode = 'write'},
    })
    t.assert_equals(err, nil)
    t.assert_equals(result.rows, {
        {3, 2804, 'David', 'Smith', 33, 'Los Angeles'},
    })

    local map_reduces_after_1 = helpers.get_map_reduces_stat(router, 'customers')
    local diff_1 = map_reduces_after_1 - map_reduces_before
    t.assert_equals(diff_1, 0, 'Select request was not a map reduce')

    -- Case: EQ on secondary index, which is not in the sharding
    -- index (primary index in the case).
    local result, err = g.router:call('crud.select', {
        'customers',
        {{'==', 'age', 81}},
        {bucket_id = 1161, timeout = 1, mode = 'write'},
    })
    t.assert_equals(err, nil)
    t.assert_equals(result.rows, {
        {4, 1161, 'William', 'White', 81, 'Chicago'},
    })

    local map_reduces_after_2 = helpers.get_map_reduces_stat(router, 'customers')
    local diff_2 = map_reduces_after_2 - map_reduces_after_1
    t.assert_equals(diff_2, 0, 'Select request was not a map reduce')
end

pgroup.before_test('test_storage_nil_err_is_processed', function(g)
    helpers.call_on_storages(g.cluster, function(server)
        server.net_box:eval([[
            local errors = require('errors')
            local StorageError = errors.new_class("StorageError")

            local _crud = rawget(_G, '_crud')
            rawset(_G, '_real_crud_select', _crud.select_on_storage)

            local return_err = function()
                return nil, StorageError:new('My storage error')
            end
            _crud.select_on_storage = return_err
        ]])
    end)
end)

pgroup.after_test('test_storage_nil_err_is_processed', function(g)
    helpers.call_on_storages(g.cluster, function(server)
        server.net_box:eval([[
            local _crud = rawget(_G, '_crud')
            _crud.select_on_storage = rawget(_G, '_real_crud_select')
            rawset(_G, '_real_crud_select', nil)
        ]])
    end)
end)

pgroup.test_storage_nil_err_is_processed = function(g)
    local obj, err = g.router:call('crud.select', {
        'customers', {{'==', 'age', 101}}
    })
    t.assert_equals(obj, nil)
    t.assert_str_contains(err.str, 'StorageError', 'Storage error class is preserved')
    t.assert_str_contains(err.str, 'My storage error', 'Storage error msg is preserved')
end

pgroup.before_test('test_storage_uninit_select_error_text', function(g)
    helpers.call_on_storages(g.cluster, function(server)
        server.net_box:eval([[
            local _crud = rawget(_G, '_crud')
            rawset(_G, '_real_crud_select', _crud.select_on_storage)
            _crud.select_on_storage = nil
        ]])
    end)
end)

pgroup.after_test('test_storage_uninit_select_error_text', function(g)
    helpers.call_on_storages(g.cluster, function(server)
        server.net_box:eval([[
            local _crud = rawget(_G, '_crud')
            _crud.select_on_storage = rawget(_G, '_real_crud_select')
            rawset(_G, '_real_crud_select', nil)
        ]])
    end)
end)

pgroup.test_storage_uninit_select_error_text = function(g)
    local obj, err = g.router:call('crud.select', {
        'customers', {{'==', 'age', 101}}
    })
    t.assert_equals(obj, nil)
    t.assert_str_contains(err.str, 'SelectError')
    t.assert_str_contains(err.str, 'NotInitialized')
    t.assert_str_contains(err.str, "Function '_crud.select_on_storage' is not registered")
    t.assert_str_contains(err.str, "crud isn't initialized on replicaset")
    t.assert_str_contains(err.str, "or crud module versions mismatch between router and storage")
end

pgroup.before_test('test_storage_uninit_get_error_text', function(g)
    helpers.call_on_storages(g.cluster, function(server)
        server.net_box:eval([[
            local _crud = rawget(_G, '_crud')
            rawset(_G, '_real_crud_get', _crud.get_on_storage)
            _crud.get_on_storage = nil
        ]])
    end)
end)

pgroup.after_test('test_storage_uninit_get_error_text', function(g)
    helpers.call_on_storages(g.cluster, function(server)
        server.net_box:eval([[
            local _crud = rawget(_G, '_crud')
            _crud.get_on_storage = rawget(_G, '_real_crud_get')
            rawset(_G, '_real_crud_get', nil)
        ]])
    end)
end)

pgroup.test_storage_uninit_get_error_text = function(g)
    local obj, err = g.router:call('crud.get', {'customers', 1})
    t.assert_equals(obj, nil)
    t.assert_str_contains(err.str, 'GetError')
    t.assert_str_contains(err.str, 'NotInitialized')
    t.assert_str_contains(err.str, "Function '_crud.get_on_storage' is not registered")
    t.assert_str_contains(err.str, "crud isn't initialized on replicaset")
    t.assert_str_contains(err.str, "or crud module versions mismatch between router and storage")
end

local function prepare_yield_counters_on_storages(g)
    helpers.call_on_storages(
        g.cluster,
        function(server)
            server.net_box:eval([[
                local fiber = require('fiber')
                rawset(_G, '_fiber_yield', fiber.yield)

                rawset(_G, '_yield_count', 0)

                fiber.yield = function()
                    rawset(_G, '_yield_count', rawget(_G, '_yield_count') + 1)
                    rawget(_G, '_fiber_yield')()
                end
            ]])
        end)
end

local function reset_yield_counters_on_storages(g)
    helpers.call_on_storages(
        g.cluster,
        function(server)
            server.net_box:eval([[
                rawset(_G, '_yield_count', 0)
            ]])
        end)
end

local function get_yields_count(g)
    local result = {}
    helpers.call_on_storages(
        g.cluster,
        function(server, replicaset_alias, result)
            local count = server.net_box:eval([[
                return rawget(_G, '_yield_count')
            ]])

            if result[replicaset_alias] == nil then
                result[replicaset_alias] = 0
            end

            result[replicaset_alias] = result[replicaset_alias] + count
        end,
        result)

    local total = 0
    for _, v in pairs(result) do
        total = total + v
    end

    return total
end

local function clean_yield_counters_on_storages(g)
    helpers.call_on_storages(
        g.cluster,
        function(server)
            server.net_box:eval([[
                local fiber = require('fiber')

                rawset(_G, '_yield_count', nil)

                fiber.yield = rawget(_G, '_fiber_yield')

                rawset(_G, '_fiber_yield', nil)
            ]])
        end)
end


local cases = {
    select = {
        eval = [[
            return crud.select(...)
        ]]
    },
    pairs = {
        eval = [[
            local resp = { rows = {} }
            for _, v in crud.pairs(...) do
                table.insert(resp.rows, v)
            end
            return resp
        ]]
    },
}

for op, case in pairs(cases) do
    local name = ('test_%s_yield_every'):format(op)

    pgroup.before_test(name, prepare_yield_counters_on_storages)
    pgroup.after_test(name, clean_yield_counters_on_storages)

    pgroup[name] = function(g)
        local customers_count = 1000
        local yield_margin = 100 -- For corner cases and yields unrelated to the call.

        for id = 1, customers_count do
            helpers.insert_objects(g, 'customers', {
                {
                    id = id, name = 'Masafumi', last_name = 'Gotoh',
                    age = 46, city = 'Shimada',
                }
            })
        end

        local conditions = {{'=', 'age', 46}}
        local opts = {
            first = customers_count,
            batch_size = customers_count,
            mode = 'write',
        }

        if op == 'select' then
            opts.fullscan = true
        end

        opts.yield_every = 1
        reset_yield_counters_on_storages(g)
        local resp, err = g.router:eval(case.eval, {'customers', conditions, opts})
        t.assert_equals(err, nil)
        t.assert_equals(#resp.rows, 1000)
        local yield_count_on_each = get_yields_count(g)

        opts.yield_every = customers_count * 2
        reset_yield_counters_on_storages(g)
        local resp, err = g.router:eval(case.eval, {'customers', conditions, opts})
        t.assert_equals(err, nil)
        t.assert_equals(#resp.rows, 1000)
        local yield_count_none = get_yields_count(g)

        t.assert_almost_equals(
            yield_count_on_each - yield_count_none,
            customers_count,
            yield_margin,
            "Expected yield on each record")
    end
end

pgroup.test_select_yield_every_0 = function(g)
    local resp, err = g.router:call('crud.select', {
        'customers', nil, {yield_every = 0, fullscan = true, mode = 'write'},
    })
    t.assert_equals(resp, nil)
    t.assert_str_contains(err.err, "yield_every should be > 0")
end

pgroup.test_pairs_yield_every_0 = function(g)
    t.assert_error_msg_contains("yield_every should be > 0", function()
        g.router:call('crud.pairs',
            {'customers', nil, {yield_every = 0, mode = 'write'},
        })
    end)
end

local function read_impl(cg, space, conditions, opts)
    opts = table.deepcopy(opts) or {}
    opts.mode = 'write'
    opts.fullscan = true

    local resp, err = cg.router:call('crud.select', {space, conditions, opts})

    if err ~= nil then
        return nil, err
    end

    return crud.unflatten_rows(resp.rows, resp.metadata), nil
end

pgroup.test_gh_418_select_with_secondary_noneq_index_condition = function(g)
    read_scenario.gh_418_read_with_secondary_noneq_index_condition(g, read_impl)
end

local gh_373_types_cases = helpers.merge_tables(
    read_scenario.gh_373_read_with_decimal_condition_cases,
    read_scenario.gh_373_read_with_datetime_condition_cases,
    read_scenario.gh_373_read_with_interval_condition_cases
)

for case_name_template, case in pairs(gh_373_types_cases) do
    local case_name = 'test_' .. case_name_template:format('select')

    pgroup[case_name] = function(g)
        case(g, read_impl)
    end
end

pgroup.before_test(
    'test_select_merger_process_storage_error',
    read_scenario.before_merger_process_storage_error
)

pgroup.test_select_merger_process_storage_error = function(g)
    read_scenario.merger_process_storage_error(g, read_impl)
end

pgroup.after_test(
    'test_select_merger_process_storage_error',
    read_scenario.after_merger_process_storage_error
)

for case_name_template, case in pairs(read_scenario.gh_422_nullability_cases) do
    local case_name = 'test_' .. case_name_template:format('select')

    pgroup[case_name] = function(g)
        case(g, read_impl)
    end
end

pgroup.test_select_invalid_bucket_id = function(g)
    local invalid_opts_list = {
        {bucket_id = "str"},
        {bucket_id = -1},
        {bucket_id = {}},
        {bucket_id = true},
    }

    for _, opts in ipairs(invalid_opts_list) do
        opts.mode = 'write'
        opts.fullscan = true

        local expected_err = string.format(
            "Invalid bucket_id: expected unsigned, got %s",
            type(opts.bucket_id)
        )

        local resp, err = g.router:call('crud.select', {
            'customers', nil, opts
        })
        t.assert_equals(resp, nil)
        t.assert_str_contains(err.err or err.str, expected_err)
    end
end
