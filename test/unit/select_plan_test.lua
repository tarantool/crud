local select_plan = require('crud.compare.plan')
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
    t.assert_equals(plan.sharding_key, {15})

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
    t.assert_equals(plan.tarantool_iter, box.index.LE) -- inverted iterator
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
    t.assert_equals(plan.tarantool_iter, box.index.EQ)
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

local get_sharding_key_from_scan_value_cases = {
    for_non_table_value = {
        -- Input values.
        scan_value = 2,
        scan_index = 'id',
        scan_iter = box.index.EQ,
        sharding_index = 'id',
        -- Expected values.
        sharding_key = 2,
    },
    for_empty_value = {
        -- Input values.
        scan_value = nil,
        scan_index = 'id',
        scan_iter = box.index.EQ,
        sharding_index = 'id',
        -- Expected values.
        sharding_key = nil,
    },
    for_ge_iter_returns_nil = {
        -- Input values.
        scan_value = 2,
        scan_index = 'id',
        scan_iter = box.index.GE,
        sharding_index = 'id',
        -- Expected values.
        sharding_key = nil,
    },
    returns_nil_if_sharding_index_is_not_scan_index = {
        -- Input values.
        scan_value = 2,
        scan_index = 'id',
        scan_iter = box.index.EQ,
        sharding_index = 'age',
        -- Expected values.
        sharding_key = nil,
    },
    for_table_value = {
        -- Input values.
        scan_value = { 'John', 'Doe' },
        scan_index = 'age',
        scan_iter = box.index.EQ,
        sharding_index = 'age',
        -- Expected values.
        sharding_key = { 'John', 'Doe' },
    },
    for_partial_table_value_returns_nil = {
        -- Input values.
        scan_value = { nil, 'Doe' },
        scan_index = 'age',
        scan_iter = box.index.EQ,
        sharding_index = 'age',
        -- Expected values.
        sharding_key = nil,
    },
}

for name, case in pairs(get_sharding_key_from_scan_value_cases) do
    g[('test_get_sharding_key_from_scan_value_%s'):format(name)] = function()
        local scan_value = case.scan_value
        local scan_index = box.space.customers.index[case.scan_index]
        local scan_iter = case.scan_iter
        local sharding_index = box.space.customers.index[case.sharding_index]

        local get_sharding_key = select_plan.internal.get_sharding_key_from_scan_value
        local sharding_key = get_sharding_key(scan_value, scan_index, scan_iter, sharding_index)
        t.assert_equals(sharding_key, case.sharding_key)
    end
end

local extract_sharding_key_from_conditions_cases = {
    pk_field_sharding_key_from_double_equal_sign_pk_condition = {
        -- Input values.
        sharding_index = { parts = {{ fieldno = 1 }} },
        conditions = {{ '==', 'id', 2 }},
        -- Expected values.
        sharding_key = { 2 },
    },
    pk_field_sharding_key_from_single_equal_sign_pk_condition = {
        -- Input values.
        sharding_index = { parts = {{ fieldno = 1 }} },
        conditions = {{ '=', 'id', 2 }},
        -- Expected values.
        sharding_key = { 2 },
    },
    pk_field_sharding_key_from_ge_sign_pk_condition_returns_nil = {
        -- Input values.
        sharding_index = { parts = {{ fieldno = 1 }} },
        conditions = {{ '>=', 'id', 2 }},
        -- Expected values.
        sharding_key = nil,
    },
    pk_field_sharding_key_from_le_sign_pk_condition_returns_nil = {
        -- Input values.
        sharding_index = { parts = {{ fieldno = 1 }} },
        conditions = {{ '<=', 'id', 2 }},
        -- Expected values.
        sharding_key = nil,
    },
    pk_field_sharding_key_from_gt_sign_pk_condition_returns_nil = {
        -- Input values.
        sharding_index = { parts = {{ fieldno = 1 }} },
        conditions = {{ '<', 'id', 2 }},
        -- Expected values.
        sharding_key = nil,
    },
    pk_field_sharding_key_from_lt_sign_pk_condition_returns_nil = {
        -- Input values.
        sharding_index = { parts = {{ fieldno = 1 }} },
        conditions = {{ '<', 'id', 2 }},
        -- Expected values.
        sharding_key = nil,
    },
    field_sharding_key_from_its_non_unique_single_field_secondary_index_condition = {
        -- Input values.
        sharding_index = { parts = {{ fieldno = 5 }} },
        conditions = {{ '==', 'age', 42 }},
        -- Expected values.
        sharding_key = { 42 },
    },
    field_sharding_key_from_its_multiple_fields_secondary_index_condition = {
        -- Input values.
        sharding_index = { parts = {{ fieldno = 3 }} },
        conditions = {{ '==', 'full_name', { 'John',  'Doe' } }},
        -- Expected values.
        sharding_key = { 'John' },
    },
    table_sharding_key_from_two_conditions = {
        -- Input values.
        sharding_index = { parts = {{ fieldno = 3 }, { fieldno = 5 }} },
        conditions = {{ '==', 'name', 'John' }, { '==', 'age', 42 }},
        -- Expected values.
        sharding_key = { 'John', 42 },
    },
    table_sharding_key_from_two_index_conditions = {
        -- Input values.
        sharding_index = { parts = {{ fieldno = 3 }, { fieldno = 5 }} },
        conditions = {{ '==', 'full_name', { 'John',  'Doe' } }, { '==', 'age', 42 }},
        -- Expected values.
        sharding_key = { 'John', 42 },
    },
    table_sharding_key_from_eq_an_ge_conditions_returns_nil = {
        -- Input values.
        sharding_index = { parts = {{ fieldno = 3 }, { fieldno = 5 }} },
        conditions = {{ '==', 'name', 'John' }, { '>=', 'age', 42 }},
        -- Expected values.
        sharding_key = nil,
    },
    table_sharding_key_from_partial_conditions = {
        -- Input values.
        sharding_index = { parts = {{ fieldno = 1 }, { fieldno = 4 }} },
        conditions = {{ '==', 'full_name', { nil,  'Doe' } }, { '==', 'id', 1 }},
        -- Expected values.
        sharding_key = { 1, 'Doe' },
    },
    table_sharding_key_from_conditions_with_nil_and_non_nil_for_same_value = {
        -- Input values.
        sharding_index = { parts = {{ fieldno = 3 }, { fieldno = 5 }} },
        conditions = {{ '==', 'full_name', { 'John',  'Doe' } }, { '==', 'name_id', { nil, 1 } }},
        -- Expected values.
        sharding_key = nil,
    },
}

for name, case in pairs(extract_sharding_key_from_conditions_cases) do
    g[('test_extract_%s'):format(name)] = function()
        local conditions = compare_conditions.parse(case.conditions)
        local sharding_index = case.sharding_index
        local space_indexes = box.space.customers.index
        local space_format = box.space.customers:format()
        local fieldno_map = utils.get_format_fieldno_map(space_format)

        local extract_sharding_key = select_plan.internal.extract_sharding_key_from_conditions
        local sharding_key = extract_sharding_key(conditions, sharding_index, space_indexes, fieldno_map)
        t.assert_equals(sharding_key, case.sharding_key)
    end
end

g.before_test('test_extract_sharding_key_from_conditions_for_index_and_field_with_same_name', function()
    box.space.customers:create_index('city', {
        parts = { {field = 'id'}, {field = 'city'} },
        unique = false,
        if_not_exists = true,
    })
end)

g.after_test('test_extract_sharding_key_from_conditions_for_index_and_field_with_same_name', function()
    box.space.customers.index.city:drop()
end)

g.test_extract_sharding_key_from_conditions_for_index_and_field_with_same_name = function()
    local space_indexes = box.space.customers.index
    local space_format = box.space.customers:format()
    local fieldno_map = utils.get_format_fieldno_map(space_format)

    local sharding_index = { parts = {{ fieldno = 6 }} }

    local conditions = compare_conditions.parse({{ '==', 'city', { 1, 'New York' } }})
    local extract_sharding_key = select_plan.internal.extract_sharding_key_from_conditions
    local sharding_key = extract_sharding_key(conditions, sharding_index, space_indexes, fieldno_map)
    t.assert_equals(sharding_key, { 'New York' },
        "Extracted sharding key from index in case of name collision")
end
