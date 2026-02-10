local select_plan = require('crud.compare.plan')
local select_executor = require('crud.select.executor')
local select_filters = require('crud.compare.filters')

local compare_conditions = require('crud.compare.conditions')
local cond_funcs = compare_conditions.funcs

local t = require('luatest')
local g = t.group('select_executor')

local helpers = require('test.helper')

local function insert_customers(customers)
    for _, customer in ipairs(customers) do
        box.space.customers:insert(box.space.customers:frommap(customer))
    end
end

local function get_ids(customers)
    local selected_customer_ids = {}
    for _, customer in ipairs(customers) do
        table.insert(selected_customer_ids, customer.id)
    end
    return selected_customer_ids
end

g.before_all = function()
    helpers.box_cfg()

    local customers_space = box.schema.space.create('customers', {
        format = {
            {name = 'id', type = 'unsigned'},
            {name = 'name', type = 'string'},
            {name = 'last_name', type = 'string'},
            {name = 'age', type = 'number'},
            {name = 'city', type = 'string'},
        },
        if_not_exists = true,
    })
    customers_space:create_index('id', {
        parts = { {field = 'id'} },
        if_not_exists = true,
    })
    customers_space:create_index('age', {
        parts = { {field = 'age'} },
        unique = false,
        if_not_exists = true,
    })
    customers_space:create_index('full_name', {
        parts = {
            { field = 'name', collation = 'unicode_ci' },
            { field = 'last_name', is_nullable = true },
        },
        unique = false,
        if_not_exists = true,
    })
end

g.after_each(function()
    box.space.customers:truncate()
end)

g.after_all(function()
    box.space.customers:drop()
end)

g.test_one_condition_no_index = function()
    local customers = {
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
    }
    insert_customers(customers)

    local conditions = { cond_funcs.eq('city', 'Los Angeles') }
    local space = box.space.customers

    local plan, err = select_plan.new(space, conditions)
    t.assert_equals(err, nil)
    local index = space.index[plan.index_id]

    local filter_func, err = select_filters.gen_func(space, index, conditions, {
        tarantool_iter = plan.tarantool_iter,
        scan_condition_num = plan.scan_condition_num,
    })
    t.assert_equals(err, nil)

    -- no after
    local results = select_executor.execute(space, index, filter_func, {
        scan_value = plan.scan_value,
        tarantool_iter = plan.tarantool_iter,
        scan_condition_num = plan.scan_condition_num,
    })
    t.assert_equals(get_ids(results.tuples), {2, 3})

    -- after tuple 2
    local after_tuple = space:frommap(customers[2]):totable()

    local results = select_executor.execute(space, index, filter_func, {
        scan_value = plan.scan_value,
        after_tuple = after_tuple,
        tarantool_iter = plan.tarantool_iter,
    })
    t.assert_equals(get_ids(results.tuples), {3})

    -- after tuple 3
    local after_tuple = space:frommap(customers[3]):totable()

    local results = select_executor.execute(space, index, filter_func, {
        scan_value = plan.scan_value,
        after_tuple = after_tuple,
        tarantool_iter = plan.tarantool_iter,
    })
    t.assert_equals(#results.tuples, 0)
end

g.test_eq_condition_with_after_on_nonunique_index = function()
    -- Test EQ iterator with after cursor on non-unique secondary index.
    -- When after_tuple key matches scan_value, native after is used (O(1)).
    -- When keys don't match, fallback scroll_to_after_tuple is used (O(N)).
    local customers = {
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
            id = 4, name = "William", last_name = "White",
            age = 33, city = "Chicago",
        }, {
            id = 5, name = "Jack", last_name = "Sparrow",
            age = 33, city = "London",
        },
    }
    insert_customers(customers)

    -- EQ condition on non-unique age index
    local conditions = { cond_funcs.eq('age', 33) }
    local space = box.space.customers

    local plan, err = select_plan.new(space, conditions)
    t.assert_equals(err, nil)
    local index = space.index[plan.index_id]

    local filter_func, err = select_filters.gen_func(space, index, conditions, {
        tarantool_iter = plan.tarantool_iter,
        scan_condition_num = plan.scan_condition_num,
    })
    t.assert_equals(err, nil)

    -- no after - should return all customers with age=33
    local results = select_executor.execute(space, index, filter_func, {
        scan_value = plan.scan_value,
        tarantool_iter = plan.tarantool_iter,
    })
    t.assert_equals(get_ids(results.tuples), {1, 3, 4, 5}) -- in id order

    -- after tuple 1 - should return remaining customers with age=33
    local after_tuple = space:frommap(customers[1]):totable()
    local results = select_executor.execute(space, index, filter_func, {
        scan_value = plan.scan_value,
        after_tuple = after_tuple,
        tarantool_iter = plan.tarantool_iter,
    })
    t.assert_equals(get_ids(results.tuples), {3, 4, 5})

    -- after tuple 3 - should return customers 4 and 5
    local after_tuple = space:frommap(customers[3]):totable()
    local results = select_executor.execute(space, index, filter_func, {
        scan_value = plan.scan_value,
        after_tuple = after_tuple,
        tarantool_iter = plan.tarantool_iter,
    })
    t.assert_equals(get_ids(results.tuples), {4, 5})

    -- after tuple 5 (last with age=33) - should return empty
    local after_tuple = space:frommap(customers[5]):totable()
    local results = select_executor.execute(space, index, filter_func, {
        scan_value = plan.scan_value,
        after_tuple = after_tuple,
        tarantool_iter = plan.tarantool_iter,
    })
    t.assert_equals(#results.tuples, 0)
end

g.test_eq_condition_with_after_mismatched_key = function()
    -- Test EQ iterator when after_tuple key does NOT match scan_value.
    -- This forces fallback to scroll_to_after_tuple (native after would
    -- cause "Iterator position is invalid" error).
    local customers = {
        {
            id = 1, name = "Elizabeth", last_name = "Jackson",
            age = 20, city = "New York",
        }, {
            id = 2, name = "Mary", last_name = "Brown",
            age = 33, city = "Los Angeles",
        }, {
            id = 3, name = "David", last_name = "Smith",
            age = 33, city = "Los Angeles",
        }, {
            id = 4, name = "William", last_name = "White",
            age = 46, city = "Chicago",
        },
    }
    insert_customers(customers)

    -- EQ condition on age=33, but after_tuple has age=20
    local conditions = { cond_funcs.eq('age', 33) }
    local space = box.space.customers

    local plan, err = select_plan.new(space, conditions)
    t.assert_equals(err, nil)
    local index = space.index[plan.index_id]

    local filter_func, err = select_filters.gen_func(space, index, conditions, {
        tarantool_iter = plan.tarantool_iter,
        scan_condition_num = plan.scan_condition_num,
    })
    t.assert_equals(err, nil)

    -- after_tuple with age=20 (mismatched key), should still return age=33 tuples
    local after_tuple = space:frommap(customers[1]):totable()
    local results = select_executor.execute(space, index, filter_func, {
        scan_value = plan.scan_value,
        after_tuple = after_tuple,
        tarantool_iter = plan.tarantool_iter,
    })
    t.assert_equals(get_ids(results.tuples), {2, 3})
end

g.test_one_condition_with_index = function()
    local customers = {
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
    }
    insert_customers(customers)

    local conditions = { cond_funcs.ge('age', 33) }
    local space = box.space.customers

    local plan, err = select_plan.new(space, conditions)
    t.assert_equals(err, nil)
    local index = space.index[plan.index_id]

    local filter_func, err = select_filters.gen_func(space, index, conditions, {
        tarantool_iter = plan.tarantool_iter,
        scan_condition_num = plan.scan_condition_num,
    })
    t.assert_equals(err, nil)

    -- no after
    local results = select_executor.execute(space, index, filter_func, {
        scan_value = plan.scan_value,
        tarantool_iter = plan.tarantool_iter,
    })
    t.assert_equals(get_ids(results.tuples), {3, 2, 4}) -- in age order

    -- after tuple 3
    local after_tuple = space:frommap(customers[3]):totable()

    local results = select_executor.execute(space, index, filter_func, {
        scan_value = plan.scan_value,
        after_tuple = after_tuple,
        tarantool_iter = plan.tarantool_iter,
    })
    t.assert_equals(get_ids(results.tuples), {2, 4}) -- in age order
end

g.test_le_condition_with_after = function()
    -- LE uses LE iterator, native after applies via generate_value path.
    local customers = {
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
    }
    insert_customers(customers)

    local conditions = { cond_funcs.le('age', 46) }
    local space = box.space.customers

    local plan, err = select_plan.new(space, conditions)
    t.assert_equals(err, nil)
    local index = space.index[plan.index_id]

    local filter_func, err = select_filters.gen_func(space, index, conditions, {
        tarantool_iter = plan.tarantool_iter,
        scan_condition_num = plan.scan_condition_num,
    })
    t.assert_equals(err, nil)

    -- no after - returns age <= 46 in reverse order
    local results = select_executor.execute(space, index, filter_func, {
        scan_value = plan.scan_value,
        tarantool_iter = plan.tarantool_iter,
    })
    t.assert_equals(get_ids(results.tuples), {2, 3, 1})

    -- after tuple 2 (age=46) - should return remaining
    local after_tuple = space:frommap(customers[2]):totable()

    local results = select_executor.execute(space, index, filter_func, {
        scan_value = plan.scan_value,
        after_tuple = after_tuple,
        tarantool_iter = plan.tarantool_iter,
    })
    t.assert_equals(get_ids(results.tuples), {3, 1})
end

g.test_lt_condition_with_after = function()
    -- LT uses LT iterator, native after applies via generate_value path.
    local customers = {
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
    }
    insert_customers(customers)

    local conditions = { cond_funcs.lt('age', 81) }
    local space = box.space.customers

    local plan, err = select_plan.new(space, conditions)
    t.assert_equals(err, nil)
    local index = space.index[plan.index_id]

    local filter_func, err = select_filters.gen_func(space, index, conditions, {
        tarantool_iter = plan.tarantool_iter,
        scan_condition_num = plan.scan_condition_num,
    })
    t.assert_equals(err, nil)

    -- no after - returns age < 81 in reverse order
    local results = select_executor.execute(space, index, filter_func, {
        scan_value = plan.scan_value,
        tarantool_iter = plan.tarantool_iter,
    })
    t.assert_equals(get_ids(results.tuples), {2, 3, 1})

    -- after tuple 2 (age=46) - should return remaining
    local after_tuple = space:frommap(customers[2]):totable()

    local results = select_executor.execute(space, index, filter_func, {
        scan_value = plan.scan_value,
        after_tuple = after_tuple,
        tarantool_iter = plan.tarantool_iter,
    })
    t.assert_equals(get_ids(results.tuples), {3, 1})
end

g.test_gt_condition_with_after = function()
    -- GT uses GT iterator, native after applies via generate_value path.
    local customers = {
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
    }
    insert_customers(customers)

    local conditions = { cond_funcs.gt('age', 12) }
    local space = box.space.customers

    local plan, err = select_plan.new(space, conditions)
    t.assert_equals(err, nil)
    local index = space.index[plan.index_id]

    local filter_func, err = select_filters.gen_func(space, index, conditions, {
        tarantool_iter = plan.tarantool_iter,
        scan_condition_num = plan.scan_condition_num,
    })
    t.assert_equals(err, nil)

    -- no after - returns age > 12 in order
    local results = select_executor.execute(space, index, filter_func, {
        scan_value = plan.scan_value,
        tarantool_iter = plan.tarantool_iter,
    })
    t.assert_equals(get_ids(results.tuples), {3, 2, 4})

    -- after tuple 3 (age=33) - should return remaining
    local after_tuple = space:frommap(customers[3]):totable()

    local results = select_executor.execute(space, index, filter_func, {
        scan_value = plan.scan_value,
        after_tuple = after_tuple,
        tarantool_iter = plan.tarantool_iter,
    })
    t.assert_equals(get_ids(results.tuples), {2, 4})
end

g.test_multiple_conditions = function()
    local customers = {
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
    }
    insert_customers(customers)

    local conditions = {
        cond_funcs.gt('age', 20),
        cond_funcs.eq('name', 'Elizabeth'),
        cond_funcs.eq('city', "Chicago"),
    }
    local space = box.space.customers

    local plan, err = select_plan.new(space, conditions)
    t.assert_equals(err, nil)
    local index = space.index[plan.index_id]

    local filter_func, err = select_filters.gen_func(space, index, conditions, {
        tarantool_iter = plan.tarantool_iter,
        scan_condition_num = plan.scan_condition_num,
    })
    t.assert_equals(err, nil)

    -- no after
    local results = select_executor.execute(space, index, filter_func, {
        scan_value = plan.scan_value,
        tarantool_iter = plan.tarantool_iter,
    })
    t.assert_equals(get_ids(results.tuples), {5, 2})  -- in age order

    -- after tuple 5
    local after_tuple = space:frommap(customers[5]):totable()

    local results = select_executor.execute(space, index, filter_func, {
        scan_value = plan.scan_value,
        after_tuple = after_tuple,
        tarantool_iter = plan.tarantool_iter,
    })
    t.assert_equals(get_ids(results.tuples), {2})
end

g.test_composite_index = function()
    local customers = {
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
    }
    insert_customers(customers)

    local conditions = {
        cond_funcs.ge('full_name', {"Elizabeth", "Jo"}),
    }
    local space = box.space.customers

    local plan, err = select_plan.new(space, conditions)
    t.assert_equals(err, nil)
    local index = space.index[plan.index_id]

    local filter_func, err = select_filters.gen_func(space, index, conditions, {
        tarantool_iter = plan.tarantool_iter,
        scan_condition_num = plan.scan_condition_num,
    })
    t.assert_equals(err, nil)

    -- no after
    local results = select_executor.execute(space, index, filter_func, {
        scan_value = plan.scan_value,
        tarantool_iter = plan.tarantool_iter,
    })
    t.assert_equals(get_ids(results.tuples), {2, 1, 4}) -- in full_name order

    -- after tuple 2
    local after_tuple = space:frommap(customers[2]):totable()

    local results = select_executor.execute(space, index, filter_func, {
        scan_value = plan.scan_value,
        after_tuple = after_tuple,
        tarantool_iter = plan.tarantool_iter,
    })
    t.assert_equals(get_ids(results.tuples), {1, 4})
end

g.test_get_by_id = function()
    local customers = {
        {
            id = 1, name = "Elizabeth", last_name = "Rodriguez",
            age = 20, city = "Los Angeles",
        }, {
            id = 2, name = "John", last_name = "Johnson",
            age = 44, city = "Mew York",
        }, {
            id = 3, name = "David", last_name = "Brown",
            age = 23, city = "Chicago",
        }
    }
    insert_customers(customers)

    local conditions = {
        cond_funcs.eq('id', 2),
    }
    local space = box.space.customers

    local plan, err = select_plan.new(space, conditions)
    t.assert_equals(err, nil)
    local index = space.index[plan.index_id]

    local filter_func, err = select_filters.gen_func(space, index, conditions, {
        tarantool_iter = plan.tarantool_iter,
        scan_condition_num = plan.scan_condition_num,
    })
    t.assert_equals(err, nil)

    -- no after
    local results = select_executor.execute(space, index, filter_func, {
        scan_value = plan.scan_value,
        tarantool_iter = plan.tarantool_iter,
    })
    t.assert_equals(get_ids(results.tuples), {2})
end

g.test_early_exit = function()
    insert_customers({
        {
            id = 1, name = "Jessica", last_name = "Rodriguez",
            age = 5, city = "Los Angeles",
        }, {
            id = 2, name = "Elizabeth", last_name = "Johnson",
            age = 53, city = "New York",
        }, {
            id = 3, name = "David", last_name = "Brown",
            age = 64, city = "Chicago",
        }, {
            id = 4, name = "John", last_name = "Smith",
            age = 12, city = "Chicago",
        }
    })

    local conditions = {
        cond_funcs.gt('age', 11),
        cond_funcs.le('age', 53),
    }
    local space = box.space.customers

    local plan, err = select_plan.new(space, conditions)
    t.assert_equals(err, nil)
    local index = space.index[plan.index_id]

    local filter_func, err = select_filters.gen_func(space, index, conditions, {
        tarantool_iter = plan.tarantool_iter,
        scan_condition_num = plan.scan_condition_num,
    })
    t.assert_equals(err, nil)

    -- no after
    local results = select_executor.execute(space, index, filter_func, {
        scan_value = plan.scan_value,
        tarantool_iter = plan.tarantool_iter,
    })
    t.assert_equals(get_ids(results.tuples), {4, 2})
end

g.test_select_all = function()
    insert_customers({
        {
            id = 1, name = "Elizabeth", last_name = "Rodriguez",
            age = 5, city = "Chicago",
        }, {
            id = 2, name = "Elizabeth", last_name = "Johnson",
            age = 53, city = "New York",
        }, {
            id = 3, name = "John", last_name = "Jackson",
            age = 64, city = "Los Angeles",
        }, {
            id = 4, name = "David", last_name = "Brown",
            age = 12, city = "New York",
        }
    })

    local space = box.space.customers

    local plan, err = select_plan.new(space)
    t.assert_equals(err, nil)
    local index = space.index[plan.index_id]

    local filter_func, err = select_filters.gen_func(space, index, nil, {
        tarantool_iter = plan.tarantool_iter,
        scan_condition_num = plan.scan_condition_num,
    })
    t.assert_equals(err, nil)

    -- no after
    local results = select_executor.execute(space, index, filter_func, {
        scan_value = plan.scan_value,
        tarantool_iter = plan.tarantool_iter,
    })
    t.assert_equals(get_ids(results.tuples), {1, 2, 3, 4})
end

g.test_limit = function()
    insert_customers({
        {
            id = 1, name = "Elizabeth", last_name = "Rodriguez",
            age = 5, city = "Chicago",
        }, {
            id = 2, name = "Elizabeth", last_name = "Johnson",
            age = 53, city = "New York",
        }, {
            id = 3, name = "John", last_name = "Jackson",
            age = 64, city = "Los Angeles",
        }, {
            id = 4, name = "David", last_name = "Brown",
            age = 12, city = "New York",
        }
    })

    local space = box.space.customers

    local plan, err = select_plan.new(space)
    t.assert_equals(err, nil)
    local index = space.index[plan.index_id]

    local filter_func, err = select_filters.gen_func(space, index, nil, {
        tarantool_iter = plan.tarantool_iter,
        scan_condition_num = plan.scan_condition_num,
    })
    t.assert_equals(err, nil)

    -- limit 0
    local results = select_executor.execute(space, index, filter_func, {
        scan_value = plan.scan_value,
        tarantool_iter = plan.tarantool_iter,
        limit = 0,
    })
    t.assert_equals(#results.tuples, 0)

    -- limit 2
    local results = select_executor.execute(space, index, filter_func, {
        scan_value = plan.scan_value,
        tarantool_iter = plan.tarantool_iter,
        limit = 2,
    })
    t.assert_equals(get_ids(results.tuples), {1, 2})
end
