local select_plan = require('crud.select.plan')
local compare_conditions = require('crud.compare.conditions')
local utils = require('crud.common.utils')
local cond_funcs = compare_conditions.funcs

local t = require('luatest')
local g = t.group('select_plan')

local helpers = require('test.helper')

g.before_all = function()
    helpers.box_cfg()

    local customers_space = box.schema.space.create('customers', {
        format = {
            {'id', 'unsigned'},
            {'bucket_id', 'unsigned'},
            {'name', 'string'},
            {'last_name', 'string'},
            {'age', 'number'},
            {'city', 'string'},
            {'has_a_car', 'boolean'},
        },
        if_not_exists = true,
    })
    customers_space:create_index('id', { -- id: 0
        parts = { {field = 'id'} },
        if_not_exists = true,
    })
    customers_space:create_index('age', { -- id: 1
        parts = { {field = 'age'} },
        unique = false,
        if_not_exists = true,
    })
    customers_space:create_index('full_name', { -- id: 2
        parts = {
            {field = 'name', collation = 'unicode_ci'},
            {field = 'last_name', is_nullable = true},
        },
        unique = false,
        if_not_exists = true,
    })
    customers_space:create_index('name_id', { -- id: 3
        parts = { {field = 'name'}, {field = 'id'} },
        unique = false,
        if_not_exists = true,
    })

    -- is used for multipart primary index tests
    local coord_space = box.schema.space.create('coord', {
        format = {
              {name = 'x', type = 'unsigned'},
              {name = 'y', type = 'unsigned'},
              {name = 'bucket_id', type = 'unsigned'},
        },
        if_not_exists = true,
    })
     -- primary index
    coord_space:create_index('primary', {
        parts = { {field = 'x'}, {field = 'y'} },
        if_not_exists = true,
    })
    coord_space:create_index('bucket_id', {
        parts = { {field = 'bucket_id'} },
        if_not_exists = true,
    })
end

g.after_all(function()
    box.space.customers:drop()
end)

g.test_indexed_field = function()
    -- select by indexed field
    local conditions = { cond_funcs.gt('age', 20) }

    local plan, err = select_plan.new(box.space.customers, conditions)

    t.assert_equals(err, nil)
    t.assert_type(plan, 'table')

    t.assert_equals(plan.conditions, conditions)
    t.assert_equals(plan.space_name, 'customers')
    t.assert_equals(plan.index_id, 1) -- age index
    t.assert_equals(plan.scan_value, {20})
    t.assert_equals(plan.after_tuple, nil)
    t.assert_equals(plan.scan_condition_num, 1)
    t.assert_equals(plan.tarantool_iter, box.index.GT)
    t.assert_equals(plan.total_tuples_count, nil)
    t.assert_equals(plan.sharding_key, nil)
    t.assert_equals(plan.field_names, nil)
end

g.test_non_indexed_field = function()
    local conditions = { cond_funcs.eq('city', 'Moscow') }
    local plan, err = select_plan.new(box.space.customers, conditions)

    t.assert_equals(err, nil)
    t.assert_type(plan, 'table')

    t.assert_equals(plan.conditions, conditions)
    t.assert_equals(plan.space_name, 'customers')
    t.assert_equals(plan.index_id, 0) -- primary index
    t.assert_equals(plan.scan_value, {})
    t.assert_equals(plan.after_tuple, nil)
    t.assert_equals(plan.scan_condition_num, nil)
    t.assert_equals(plan.tarantool_iter, box.index.GE)
    t.assert_equals(plan.total_tuples_count, nil)
    t.assert_equals(plan.sharding_key, nil)
    t.assert_equals(plan.field_names, nil)
end

g.test_partial_indexed_field = function()
    -- select by first part of the index
    local conditions = { cond_funcs.gt('name', 'A'), }
    local plan, err = select_plan.new(box.space.customers, conditions)

    t.assert_equals(err, nil)
    t.assert_type(plan, 'table')

    t.assert_equals(plan.conditions, conditions)
    t.assert_equals(plan.space_name, 'customers')
    t.assert_equals(plan.index_id, 2) -- full_name index
    t.assert_equals(plan.scan_value, {'A'})
    t.assert_equals(plan.after_tuple, nil)
    t.assert_equals(plan.scan_condition_num, 1)
    t.assert_equals(plan.tarantool_iter, box.index.GT)
    t.assert_equals(plan.total_tuples_count, nil)
    t.assert_equals(plan.sharding_key, nil)
    t.assert_equals(plan.field_names, nil)

    -- select by second part of the index
    local conditions = { cond_funcs.gt('last_name', 'A'), }
    local plan, err = select_plan.new(box.space.customers, conditions)

    t.assert_equals(err, nil)
    t.assert_type(plan, 'table')

    t.assert_equals(plan.conditions, conditions)
    t.assert_equals(plan.space_name, 'customers')
    t.assert_equals(plan.index_id, 0) -- primary index
    t.assert_equals(plan.scan_value, {})
    t.assert_equals(plan.after_tuple, nil)
    t.assert_equals(plan.scan_condition_num, nil)
    t.assert_equals(plan.tarantool_iter, box.index.GE)
    t.assert_equals(plan.total_tuples_count, nil)
    t.assert_equals(plan.sharding_key, nil)
    t.assert_equals(plan.field_names, nil)
end

g.test_is_scan_by_full_sharding_key_eq = function()
    -- id eq
    local plan, err = select_plan.new(box.space.customers, {
        cond_funcs.eq('has_a_car', true),
        cond_funcs.eq('id', 15),
        cond_funcs.eq('full_name', {'Ivan', 'Ivanov'}),
        cond_funcs.gt('age', 20),
    })

    t.assert_equals(err, nil)

    t.assert_equals(plan.total_tuples_count, 1)
    t.assert_equals(plan.sharding_key, {15})

    -- id is a part of scan index
    local plan, err = select_plan.new(box.space.customers, {
        cond_funcs.eq('name_id', {'Ivan', 11}),
        cond_funcs.gt('id', 15),
        cond_funcs.gt('age', 20),
    })

    t.assert_equals(err, nil)

    t.assert_equals(plan.index_id, 3) -- index name_id is used
    t.assert_equals(plan.scan_value, {'Ivan', 11})
    t.assert_equals(plan.total_tuples_count, 1)
    t.assert_equals(plan.sharding_key, {11})

    -- other index is first
    local plan, err = select_plan.new(box.space.customers, {
        cond_funcs.eq('full_name', {'Ivan', 'Ivanov'}),
        cond_funcs.eq('id', 15),
        cond_funcs.eq('has_a_car', true),
        cond_funcs.gt('age', 20),
    })

    t.assert_equals(err, nil)

    t.assert_equals(plan.total_tuples_count, nil)
    t.assert_equals(plan.sharding_key, nil)

    -- gt
    local plan, err = select_plan.new(box.space.customers, {
        cond_funcs.eq('has_a_car', true),
        cond_funcs.gt('id', 15),
        cond_funcs.eq('full_name', {'Ivan', 'Ivanov'}),
        cond_funcs.gt('age', 20),
    })

    t.assert_equals(err, nil)

    t.assert_equals(plan.total_tuples_count, nil)
    t.assert_equals(plan.sharding_key, nil)

    -- multipart primary index

    -- specified only first part
    local plan, err = select_plan.new(box.space.coord, {
        cond_funcs.eq('primary', 0),
    })

    t.assert_equals(err, nil)

    t.assert_equals(plan.total_tuples_count, nil)
    t.assert_equals(plan.sharding_key, nil)

    -- specified both parts
    local plan, err = select_plan.new(box.space.coord, {
        cond_funcs.eq('primary', {1, 0}),
    })

    t.assert_equals(err, nil)

    t.assert_equals(plan.total_tuples_count, 1)
    t.assert_equals(plan.sharding_key, {1, 0})
end

g.test_first = function()
    -- positive first
    local plan, err = select_plan.new(box.space.customers, nil, {
        first = 10,
    })

    t.assert_equals(err, nil)
    t.assert_type(plan, 'table')

    t.assert_equals(plan.total_tuples_count, 10)
    t.assert_equals(plan.after_tuple, nil)

    -- negative first

    local after_tuple = {777, 1777, 'Leo', 'Tolstoy', 76, 'Tula', false}

    -- select by primary key, no conditions
    -- first -10 after_tuple 777
    local plan, err = select_plan.new(box.space.customers, nil, {
        first = -10,
        after_tuple = after_tuple,
    })

    t.assert_equals(err, nil)
    t.assert_type(plan, 'table')

    t.assert_equals(plan.conditions, {})
    t.assert_equals(plan.space_name, 'customers')
    t.assert_equals(plan.index_id, 0) -- primary index
    t.assert_equals(plan.scan_value, {777}) -- after_tuple id
    t.assert_equals(plan.after_tuple, after_tuple)
    t.assert_equals(plan.scan_condition_num, nil)
    t.assert_equals(plan.tarantool_iter, box.index.LT) -- inverted iterator
    t.assert_equals(plan.total_tuples_count, 10)
    t.assert_equals(plan.sharding_key, nil)
    t.assert_equals(plan.field_names, nil)

    -- select by primary key, eq condition
    -- first 10 after_tuple 777
    local conditions = { cond_funcs.eq('age', 90) }
    local plan, err = select_plan.new(box.space.customers, conditions, {
        first = 10,
        after_tuple = after_tuple,
    })

    t.assert_equals(err, nil)
    t.assert_type(plan, 'table')

    t.assert_equals(plan.conditions, conditions)
    t.assert_equals(plan.space_name, 'customers')
    t.assert_equals(plan.index_id, 1)
    t.assert_equals(plan.scan_value, {90}) -- after_tuple id
    t.assert_equals(plan.after_tuple, after_tuple)
    t.assert_equals(plan.scan_condition_num, 1)
    t.assert_equals(plan.tarantool_iter, box.index.GE) -- inverted iterator
    t.assert_equals(plan.total_tuples_count, 10)
    t.assert_equals(plan.sharding_key, nil)
    t.assert_equals(plan.field_names, nil)

    -- select by primary key, lt condition
    -- first -10 after_tuple 777
    local conditions = { cond_funcs.lt('age', 90) }
    local plan, err = select_plan.new(box.space.customers, conditions, {
        first = -10,
        after_tuple = after_tuple,
    })

    t.assert_equals(err, nil)
    t.assert_type(plan, 'table')

    t.assert_equals(plan.conditions, conditions)
    t.assert_equals(plan.space_name, 'customers')
    t.assert_equals(plan.index_id, 1) -- primary index
    t.assert_equals(plan.scan_value, {76})  -- after_tuple age value
    t.assert_equals(plan.after_tuple, after_tuple) -- after_tuple key
    t.assert_equals(plan.scan_condition_num, nil)
    t.assert_equals(plan.tarantool_iter, box.index.GT) -- inverted iterator
    t.assert_equals(plan.total_tuples_count, 10)
    t.assert_equals(plan.sharding_key, nil)
    t.assert_equals(plan.field_names, nil)
end

g.test_construct_after = function()
    local after_tuple = {'Leo', 'Tula', 76, 777}
    local expected_after_tuple = {[1] = 777, [3] = "Leo", [5] = 76, [6] = "Tula"}
    local field_names = {'name', 'city'}
    local expected_field_names = {'name', 'city', 'age', 'id'}

    -- select by primary key, lt condition
    -- after_tuple 777
    local conditions = { cond_funcs.lt('age', 76) }
    local plan, err = select_plan.new(box.space.customers, conditions, {
        after_tuple = after_tuple,
        field_names = field_names,
    })

    t.assert_equals(err, nil)
    t.assert_type(plan, 'table')

    t.assert_equals(plan.conditions, conditions)
    t.assert_equals(plan.space_name, 'customers')
    t.assert_equals(plan.index_id, 1) -- primary index
    t.assert_equals(plan.scan_value, {76})  -- after_tuple age value
    t.assert_equals(plan.after_tuple, expected_after_tuple) -- after_tuple key
    t.assert_equals(plan.scan_condition_num, 1)
    t.assert_equals(plan.tarantool_iter, box.index.LT) -- inverted iterator
    t.assert_equals(plan.sharding_key, nil)
    t.assert_equals(plan.field_names, expected_field_names)
end

g.test_table_count = function()
    t.assert_equals(utils.table_count({'Leo', 'Tula', 76, 777}), 4)
    t.assert_equals(utils.table_count({'Ivan', nil, 76, 777}), 3)
    t.assert_equals(utils.table_count({'Ivan', 'Peter', 'Fyodor', 'Alexander'}), 4)
    t.assert_equals(utils.table_count(
        {['Ivan'] = 1, ['Peter'] = 2, ['Fyodor'] = 3, ['Alexander'] = 4}), 4)
end
