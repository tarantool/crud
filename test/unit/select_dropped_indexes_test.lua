local select_plan = require('crud.select.plan')

local select_conditions = require('crud.select.conditions')
local cond_funcs = select_conditions.funcs

local t = require('luatest')
local g = t.group('select_dropped_indexes')

local helpers = require('test.helper')

g.before_all = function()
    helpers.box_cfg()

    local customers = box.schema.space.create('customers', {
        format = {
            {'id', 'unsigned'},
            {'bucket_id', 'unsigned'},
            {'name', 'string'},
            {'age', 'unsigned'},
            {'number_of_pets', 'unsigned'},
            {'cars', 'array'},
        },
        if_not_exists = true,
    })

    customers:create_index('index1', {
        type = 'TREE',
        parts = {'id'},
        if_not_exists = true,
    })

    customers:create_index('index2', {
        parts = {'bucket_id'},
        unique = false,
        if_not_exists = true,
    })

    customers:create_index('index3', {
        type = 'TREE',
        parts = {'age'},
        unique = false,
        if_not_exists = true,
    })

    customers:create_index('index_dropped1', {
        type = 'HASH',
        parts = {'name'},
        if_not_exists = true,
    })

    customers:create_index('index_dropped2', {
        type = 'HASH',
        parts = {'age'},
        if_not_exists = true,
    })

    customers:create_index('index6', {
        type = 'TREE',
        parts = {'name'},
        unique = false,
        if_not_exists = true,
    })

    customers.index.index_dropped1:drop()
    customers.index.index_dropped2:drop()
end

g.after_all = function()
    box.space.customers:drop()
end


g.test_dropped_index_call = function()
    local plan, err = select_plan.new(box.space.customers, {
        cond_funcs.gt('index_dropped1', 15),
    })

    t.assert_equals(plan, nil)
    t.assert(err ~= nil)
    t.assert_str_contains(err.err, 'No field or index "index_dropped1" found')
end


g.test_before_dropped_index_field = function()
    local conditions = { cond_funcs.eq('index3', 20) }
    local plan, err = select_plan.new(box.space.customers, conditions)

    t.assert_equals(err, nil)
    t.assert_type(plan, 'table')

    t.assert_equals(plan.conditions, conditions)
    t.assert_equals(plan.space_name, 'customers')
    t.assert_equals(plan.index_id, 2)
end

g.test_after_dropped_index_field = function()
    local conditions = { cond_funcs.eq('index6', 'Alexey') }
    local plan, err = select_plan.new(box.space.customers, conditions)

    t.assert_equals(err, nil)
    t.assert_type(plan, 'table')

    t.assert_equals(plan.conditions, conditions)
    t.assert_equals(plan.space_name, 'customers')
    t.assert_equals(plan.index_id, 5)
end

g.test_non_indexed_field = function()
    local conditions = { cond_funcs.eq('number_of_pets', 2) }
    local plan, err = select_plan.new(box.space.customers, conditions)

    t.assert_equals(err, nil)
    t.assert_type(plan, 'table')

    t.assert_equals(plan.conditions, conditions)
    t.assert_equals(plan.space_name, 'customers')
    t.assert_equals(plan.index_id, 0) -- PK
end
