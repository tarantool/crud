local crud = require('crud')

local select_plan = require('crud.select.plan')
local select_executor = require('crud.select.executor')
local select_filters = require('crud.select.filters')

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

    crud.init_storage()
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

    local filter_func, err = select_filters.gen_func(space, conditions, {
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
    t.assert_equals(get_ids(results), {2, 3})

    -- after tuple 2
    local after_tuple = space:frommap(customers[2]):totable()

    local results = select_executor.execute(space, index, filter_func, {
        scan_value = plan.scan_value,
        after_tuple = after_tuple,
        tarantool_iter = plan.tarantool_iter,
    })
    t.assert_equals(get_ids(results), {3})

    -- after tuple 3
    local after_tuple = space:frommap(customers[3]):totable()

    local results = select_executor.execute(space, index, filter_func, {
        scan_value = plan.scan_value,
        after_tuple = after_tuple,
        tarantool_iter = plan.tarantool_iter,
    })
    t.assert_equals(#results, 0)
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

    local filter_func, err = select_filters.gen_func(space, conditions, {
        tarantool_iter = plan.tarantool_iter,
        scan_condition_num = plan.scan_condition_num,
    })
    t.assert_equals(err, nil)

    -- no after
    local results = select_executor.execute(space, index, filter_func, {
        scan_value = plan.scan_value,
        tarantool_iter = plan.tarantool_iter,
    })
    t.assert_equals(get_ids(results), {3, 2, 4}) -- in age order

    -- after tuple 3
    local after_tuple = space:frommap(customers[3]):totable()

    local results = select_executor.execute(space, index, filter_func, {
        scan_value = plan.scan_value,
        after_tuple = after_tuple,
        tarantool_iter = plan.tarantool_iter,
    })
    t.assert_equals(get_ids(results), {2, 4}) -- in age order
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

    local filter_func, err = select_filters.gen_func(space, conditions, {
        tarantool_iter = plan.tarantool_iter,
        scan_condition_num = plan.scan_condition_num,
    })
    t.assert_equals(err, nil)

    -- no after
    local results = select_executor.execute(space, index, filter_func, {
        scan_value = plan.scan_value,
        tarantool_iter = plan.tarantool_iter,
    })
    t.assert_equals(get_ids(results), {5, 2})  -- in age order

    -- after tuple 5
    local after_tuple = space:frommap(customers[5]):totable()

    local results = select_executor.execute(space, index, filter_func, {
        scan_value = plan.scan_value,
        after_tuple = after_tuple,
        tarantool_iter = plan.tarantool_iter,
    })
    t.assert_equals(get_ids(results), {2})
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

    local filter_func, err = select_filters.gen_func(space, conditions, {
        tarantool_iter = plan.tarantool_iter,
        scan_condition_num = plan.scan_condition_num,
    })
    t.assert_equals(err, nil)

    -- no after
    local results = select_executor.execute(space, index, filter_func, {
        scan_value = plan.scan_value,
        tarantool_iter = plan.tarantool_iter,
    })
    t.assert_equals(get_ids(results), {2, 1, 4}) -- in full_name order

    -- after tuple 2
    local after_tuple = space:frommap(customers[2]):totable()

    local results = select_executor.execute(space, index, filter_func, {
        scan_value = plan.scan_value,
        after_tuple = after_tuple,
        tarantool_iter = plan.tarantool_iter,
    })
    t.assert_equals(get_ids(results), {1, 4})
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

    local filter_func, err = select_filters.gen_func(space, conditions, {
        tarantool_iter = plan.tarantool_iter,
        scan_condition_num = plan.scan_condition_num,
    })
    t.assert_equals(err, nil)

    -- no after
    local results = select_executor.execute(space, index, filter_func, {
        scan_value = plan.scan_value,
        tarantool_iter = plan.tarantool_iter,
    })
    t.assert_equals(get_ids(results), {2})
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

    local filter_func, err = select_filters.gen_func(space, conditions, {
        tarantool_iter = plan.tarantool_iter,
        scan_condition_num = plan.scan_condition_num,
    })
    t.assert_equals(err, nil)

    -- no after
    local results = select_executor.execute(space, index, filter_func, {
        scan_value = plan.scan_value,
        tarantool_iter = plan.tarantool_iter,
    })
    t.assert_equals(get_ids(results), {4, 2})
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

    local filter_func, err = select_filters.gen_func(space, nil, {
        tarantool_iter = plan.tarantool_iter,
        scan_condition_num = plan.scan_condition_num,
    })
    t.assert_equals(err, nil)

    -- no after
    local results = select_executor.execute(space, index, filter_func, {
        scan_value = plan.scan_value,
        tarantool_iter = plan.tarantool_iter,
    })
    t.assert_equals(get_ids(results), {1, 2, 3, 4})
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

    local filter_func, err = select_filters.gen_func(space, nil, {
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
    t.assert_equals(#results, 0)

    -- limit 2
    local results = select_executor.execute(space, index, filter_func, {
        scan_value = plan.scan_value,
        tarantool_iter = plan.tarantool_iter,
        limit = 2,
    })
    t.assert_equals(get_ids(results), {1, 2})
end
