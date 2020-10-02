local select_plan = require('crud.select.plan')
local select_conditions = require('crud.select.conditions')
local cond_funcs = select_conditions.funcs

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
            { field = 'name', collation = 'unicode_ci' },
            { field = 'last_name', is_nullable = true },
        },
        unique = false,
        if_not_exists = true,
    })
    customers_space:create_index('name_id', { -- id: 3
        parts = { {field = 'name'}, {field = 'id'} },
        unique = false,
        if_not_exists = true,
    })
end

g.after_all(function()
    box.space.customers:drop()
end)

g.test_scanner_bad_operand_name = function()
    local plan, err = select_plan.new(box.space.customers, {
        cond_funcs.gt('non-existent-field-index', 20),
    })

    t.assert_equals(plan, nil)
    t.assert(err ~= nil)
    t.assert_str_contains(err.err, 'No field or index "non-existent-field-index" found')
end

g.test_scanner_indexed_field = function()
    -- select by indexed field
    local conditions = { cond_funcs.gt('age', 20) }

    local plan, err = select_plan.new(box.space.customers, conditions)

    t.assert_equals(err, nil)
    t.assert_type(plan, 'table')

    t.assert_equals(plan.conditions, conditions)
    t.assert_equals(plan.space_name, 'customers')
    t.assert_equals(plan.index_id, 1) -- age index
    t.assert_equals(plan.scan_value, {20})
    t.assert_equals(plan.scan_condition_num, 1)
    t.assert_equals(plan.iter, box.index.GT)
    t.assert_equals(plan.total_tuples_count, nil)
    t.assert_equals(plan.sharding_key, nil)
end

g.test_scanner_non_indexed_field = function()
    local conditions = { cond_funcs.eq('city', 'Moscow') }
    local plan, err = select_plan.new(box.space.customers, conditions)

    t.assert_equals(err, nil)
    t.assert_type(plan, 'table')

    t.assert_equals(plan.conditions, conditions)
    t.assert_equals(plan.space_name, 'customers')
    t.assert_equals(plan.index_id, 0) -- primary index
    t.assert_equals(plan.scan_value, {})
    t.assert_equals(plan.scan_condition_num, nil)
    t.assert_equals(plan.iter, box.index.GE)
    t.assert_equals(plan.total_tuples_count, nil)
    t.assert_equals(plan.sharding_key, nil)
end

g.test_scanner_partial_indexed_field = function()
    -- select by first part of the index
    local conditions = { cond_funcs.gt('name', 'A'), }
    local plan, err = select_plan.new(box.space.customers, conditions)

    t.assert_equals(err, nil)
    t.assert_type(plan, 'table')

    t.assert_equals(plan.conditions, conditions)
    t.assert_equals(plan.space_name, 'customers')
    t.assert_equals(plan.index_id, 2) -- full_name index
    t.assert_equals(plan.scan_value, {'A'})
    t.assert_equals(plan.scan_condition_num, 1)
    t.assert_equals(plan.iter, box.index.GT)
    t.assert_equals(plan.total_tuples_count, nil)
    t.assert_equals(plan.sharding_key, nil)

    -- select by second part of the index
    local conditions = { cond_funcs.gt('last_name', 'A'), }
    local plan, err = select_plan.new(box.space.customers, conditions)

    t.assert_equals(err, nil)
    t.assert_type(plan, 'table')

    t.assert_equals(plan.conditions, conditions)
    t.assert_equals(plan.space_name, 'customers')
    t.assert_equals(plan.index_id, 0) -- primary index
    t.assert_equals(plan.scan_value, {})
    t.assert_equals(plan.scan_condition_num, nil)
    t.assert_equals(plan.iter, box.index.GE)
    t.assert_equals(plan.total_tuples_count, nil)
    t.assert_equals(plan.sharding_key, nil)
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
end
