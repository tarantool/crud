local select_plan = require('crud.compare.plan')

local compare_conditions = require('crud.compare.conditions')
local cond_funcs = compare_conditions.funcs

local t = require('luatest')
local g = t.group('select_plan_bad_indexes')

local helpers = require('test.helper')

local NOT_FOUND_INDEX_ERR_MSG = 'An index that matches specified conditions was not found'

g.before_all = function()
    helpers.box_cfg()

    local customers_space = box.schema.space.create('customers', {
        format = {
            {'id', 'unsigned'},
            {'bucket_id', 'unsigned'},
            {'name', 'string'},
            {'last_name', 'string'},
            {'age', 'unsigned'},
            {'cars', 'array'},
        },
        if_not_exists = true,
    })
    customers_space:create_index('id', { -- primary index is HASH
        type = 'HASH',
        parts = {'id'},
        if_not_exists = true,
    })
    customers_space:create_index('bucket_id', {
        parts = {'bucket_id'},
        unique = false,
        if_not_exists = true,
    })
    customers_space:create_index('age_tree', {
        type = 'TREE',
        parts = {'age'},
        unique = false,
        if_not_exists = true,
    })
    customers_space:create_index('age_hash', {
        type = 'HASH',
        parts = {'age'},
        if_not_exists = true,
    })
    customers_space:create_index('age_bitset', {
        type = 'BITSET',
        parts = {'age'},
        unique = false,
        if_not_exists = true,
    })
    customers_space:create_index('full_name_hash', {
        type = 'HASH',
        parts = {
            {field = 'name', collation = 'unicode_ci'},
            {field = 'last_name'},
        },
        if_not_exists = true,
    })
    customers_space:create_index('cars_rtree', {
        type = 'RTREE',
        parts = {'cars'},
        unique = false,
        if_not_exists = true,
    })
end

g.after_all = function()
    box.space.customers:drop()
end

g.test_select_all_bad_primary = function()
    local plan, err = select_plan.new(box.space.customers)

    t.assert_equals(plan, nil)
    t.assert_str_contains(err.err, NOT_FOUND_INDEX_ERR_MSG)
end

g.test_cond_with_good_index = function()
    -- check that conditions with bad indexes are just skipped
    local plan, err = select_plan.new(box.space.customers, {
        cond_funcs.lt('age_hash', 30),
        cond_funcs.lt('age_tree', 30),
    })

    t.assert_equals(err, nil)
    local index = box.space.customers.index[plan.index_id]
    t.assert_equals(index.name, 'age_tree')
end

g.test_cond_with_hash_index = function()
    local plan, err = select_plan.new(box.space.customers, {
        cond_funcs.lt('age_hash', 30),
    })

    t.assert_equals(plan, nil)
    t.assert_str_contains(err.err, NOT_FOUND_INDEX_ERR_MSG)
end

g.test_cond_with_hash_index = function()
    local plan, err = select_plan.new(box.space.customers, {
        cond_funcs.lt('age_bitset', 30),
    })

    t.assert_equals(plan, nil)
    t.assert_str_contains(err.err, NOT_FOUND_INDEX_ERR_MSG)
end

g.test_cond_with_bad_composite_index = function()
    local plan, err = select_plan.new(box.space.customers, {
        cond_funcs.lt('name', {'John', 'Doe'}),
    })

    t.assert_equals(plan, nil)
    t.assert_str_contains(err.err, NOT_FOUND_INDEX_ERR_MSG)
end

g.test_cond_with_rtree_index = function()
    local plan, err = select_plan.new(box.space.customers, {
        cond_funcs.eq('cars', {'Porshe', 'Mercedes', 'Range Rover'}),
    })

    t.assert_equals(plan, nil)
    t.assert_str_contains(err.err, NOT_FOUND_INDEX_ERR_MSG)
end
