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
        parts = {'id'},
        if_not_exists = true,
    })
    customers_space:create_index('age', { -- id: 1
        parts = {'age'},
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
    local plan, err = select_plan.new(box.space.customers, {
        cond_funcs.gt('age', 20),
    })

    t.assert_equals(err, nil)
    t.assert_type(plan, 'table')

    local scanner = plan.scanner
    t.assert_type(scanner, 'table')

    t.assert_equals(scanner.space_name, 'customers')
    t.assert_equals(scanner.index_id, 1)
    t.assert_equals(scanner.index_name, 'age')
    t.assert_equals(scanner.iter, box.index.GT)
    t.assert_equals(scanner.value, {20})
    t.assert_equals(scanner.condition_num, 1)
    t.assert_equals(scanner.limit, nil)
    t.assert_equals(scanner.after_tuple, nil)
end

g.test_scanner_non_indexed_field = function()
    local plan, err = select_plan.new(box.space.customers, {
        cond_funcs.eq('city', 'Moscow'),
    })

    t.assert_equals(err, nil)
    t.assert_type(plan, 'table')

    local scanner = plan.scanner
    t.assert_type(scanner, 'table')

    t.assert_equals(scanner.space_name, 'customers')
    t.assert_equals(scanner.index_id, 0)
    t.assert_equals(scanner.index_name, 'id')
    t.assert_equals(scanner.iter, box.index.GE)
    t.assert_equals(scanner.value, {})
    t.assert_equals(scanner.condition_num, nil)
    t.assert_equals(scanner.limit, nil)
    t.assert_equals(scanner.after_tuple, nil)
end

g.test_scanner_partial_indexed_field = function()
    -- select by first part of the index
    local plan, err = select_plan.new(box.space.customers, {
        cond_funcs.gt('name', 'A'),
    })

    t.assert_equals(err, nil)
    t.assert_type(plan, 'table')

    local scanner = plan.scanner
    t.assert_type(scanner, 'table')

    t.assert_equals(scanner.space_name, 'customers')
    t.assert_equals(scanner.index_id, 2)
    t.assert_equals(scanner.index_name, 'full_name')
    t.assert_equals(scanner.iter, box.index.GT)
    t.assert_equals(scanner.value, {'A'})
    t.assert_equals(scanner.condition_num, 1)
    t.assert_equals(scanner.limit, nil)
    t.assert_equals(scanner.after_tuple, nil)

    -- select by second part of the index
    local plan, err = select_plan.new(box.space.customers, {
        cond_funcs.gt('last_name', 'A'),
    })

    t.assert_equals(err, nil)
    t.assert_type(plan, 'table')

    local scanner = plan.scanner
    t.assert_type(scanner, 'table')

    t.assert_equals(scanner.space_name, 'customers')
    t.assert_equals(scanner.index_id, 0)
    t.assert_equals(scanner.index_name, 'id')
    t.assert_equals(scanner.iter, box.index.GE)
    t.assert_equals(scanner.value, {})
    t.assert_equals(scanner.condition_num, nil)
    t.assert_equals(scanner.limit, nil)
    t.assert_equals(scanner.after_tuple, nil)
end

g.test_limit_passed = function()
    -- select by indexed field with conditions by index and field
    local plan, err = select_plan.new(box.space.customers, {
        cond_funcs.gt('age', 20),
    }, { limit = 100 })

    t.assert_equals(err, nil)
    t.assert_type(plan, 'table')

    local scanner = plan.scanner
    t.assert_type(scanner, 'table')

    t.assert_equals(scanner.space_name, 'customers')
    t.assert_equals(scanner.index_id, 1)
    t.assert_equals(scanner.index_name, 'age')
    t.assert_equals(scanner.iter, box.index.GT)
    t.assert_equals(scanner.value, {20})
    t.assert_equals(scanner.condition_num, 1)
    t.assert_equals(scanner.limit, 100)
    t.assert_equals(scanner.after_tuple, nil)
end

g.test_full_primary_key = function()
    -- select by indexed field with conditions by index and field
    local plan, err = select_plan.new(box.space.customers, {
        cond_funcs.eq('id', 15),
        cond_funcs.gt('age', 20),
        cond_funcs.eq('full_name', {'Ivan', 'Ivanov'}),
        cond_funcs.eq('has_a_car', true)
    })

    t.assert_equals(err, nil)
    t.assert_type(plan, 'table')

    local scanner = plan.scanner
    t.assert_type(scanner, 'table')

    t.assert_equals(scanner.space_name, 'customers')
    t.assert_equals(scanner.index_id, 0)
    t.assert_equals(scanner.index_name, 'id')
    t.assert_equals(scanner.iter, box.index.REQ)
    t.assert_equals(scanner.value, {15})
    t.assert_equals(scanner.condition_num, 1)
    t.assert_equals(scanner.limit, 1)
    t.assert_equals(scanner.after_tuple, nil)
end

g.test_filter_conditions = function()
    -- select by indexed field with conditions by index and field
    local plan, err = select_plan.new(box.space.customers, {
        cond_funcs.gt('age', 20),
        cond_funcs.lt('age', 40),
        cond_funcs.eq('full_name', {'Ivan', 'Ivanov'}),
        cond_funcs.eq('has_a_car', true)
    })

    t.assert_equals(err, nil)
    t.assert_type(plan, 'table')

    t.assert_type(plan.filter_conditions, 'table')
    t.assert_equals(#plan.filter_conditions, 3)

    -- age filter (early exit is possible)
    local age_filter_condition = plan.filter_conditions[1]
    t.assert_type(age_filter_condition, 'table')
    t.assert_equals(age_filter_condition.fieldnos, {5})
    t.assert_equals(age_filter_condition.operator, select_conditions.operators.LT)
    t.assert_equals(age_filter_condition.values, {40})
    t.assert_equals(age_filter_condition.types, {'number'})
    t.assert_equals(age_filter_condition.early_exit_is_possible, true)

    -- full_name filter
    local full_name_filter_condition = plan.filter_conditions[2]
    t.assert_type(full_name_filter_condition, 'table')
    t.assert_equals(full_name_filter_condition.fieldnos, {3, 4})
    t.assert_equals(full_name_filter_condition.operator, select_conditions.operators.EQ)
    t.assert_equals(full_name_filter_condition.values, {'Ivan', 'Ivanov'})
    t.assert_equals(full_name_filter_condition.types, {'string', 'string'})
    t.assert_equals(full_name_filter_condition.early_exit_is_possible, false)

    local full_name_values_opts = full_name_filter_condition.values_opts
    t.assert_type(full_name_values_opts, 'table')
    t.assert_equals(#full_name_values_opts, 2)

    -- - name part opts
    local name_opts = full_name_values_opts[1]
    t.assert_equals(name_opts.is_nullable, false)
    t.assert_equals(name_opts.collation, 'unicode_ci')

    -- - last_name part opts
    local last_name_opts = full_name_values_opts[2]
    t.assert_equals(last_name_opts.is_nullable, true)
    t.assert_equals(last_name_opts.collation, nil)

    -- has_a_car filter
    local has_a_car_filter_condition = plan.filter_conditions[3]
    t.assert_type(has_a_car_filter_condition, 'table')
    t.assert_equals(has_a_car_filter_condition.fieldnos, {7})
    t.assert_equals(has_a_car_filter_condition.operator, select_conditions.operators.EQ)
    t.assert_equals(has_a_car_filter_condition.values, {true})
    t.assert_equals(has_a_car_filter_condition.types, {'boolean'})
    t.assert_equals(has_a_car_filter_condition.early_exit_is_possible, false)

    t.assert_equals(#has_a_car_filter_condition.values_opts, 1)
    local has_a_car_opts = has_a_car_filter_condition.values_opts[1]
    t.assert_equals(has_a_car_opts.is_nullable, true)
    t.assert_equals(has_a_car_opts.collation, nil)
end
