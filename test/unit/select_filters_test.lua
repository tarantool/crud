-- luacheck: push max_line_length 300

local select_conditions = require('crud.select.conditions')
local cond_funcs = select_conditions.funcs
local select_filters = require('crud.select.filters')
local select_plan = require('crud.select.plan')
local collations = require('crud.common.collations')

local t = require('luatest')
local g = t.group('select_filters')

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

g.test_empty_conditions = function()
    local filter_conditions = {}

    local expected_code = 'return true, false'
    local expected_library_code = 'return {}'

    local filter_code = select_filters.internal.gen_filter_code(filter_conditions)
    t.assert_equals(filter_code.code, expected_code)
    t.assert_equals(filter_code.library, expected_library_code)
end

g.test_parse = function()
    -- select by indexed field with conditions by index and field
    local conditions = {
        cond_funcs.gt('age', 20),
        cond_funcs.lt('age', 40),
        cond_funcs.eq('full_name', {'Ivan', 'Ivanov'}),
        cond_funcs.eq('has_a_car', true)
    }

    local plan, err = select_plan.new(box.space.customers, conditions)
    t.assert_equals(err, nil)

    local space = box.space.customers

    local filter_conditions, err = select_filters.internal.parse(space, conditions, {
        scan_condition_num = plan.scan_condition_num,
        tarantool_iter = plan.tarantool_iter,
    })
    t.assert_equals(err, nil)

    -- age filter (early exit is possible)
    local age_filter_condition = filter_conditions[1]
    t.assert_type(age_filter_condition, 'table')
    t.assert_equals(age_filter_condition.fieldnos, {5})
    t.assert_equals(age_filter_condition.operator, select_conditions.operators.LT)
    t.assert_equals(age_filter_condition.values, {40})
    t.assert_equals(age_filter_condition.types, {'number'})
    t.assert_equals(age_filter_condition.early_exit_is_possible, true)

    -- full_name filter
    local full_name_filter_condition = filter_conditions[2]
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
    t.assert_equals(name_opts.collation, collations.UNICODE_CI)

    -- - last_name part opts
    local last_name_opts = full_name_values_opts[2]
    t.assert_equals(last_name_opts.is_nullable, true)
    t.assert_equals(last_name_opts.collation, collations.NONE)

    -- has_a_car filter
    local has_a_car_filter_condition = filter_conditions[3]
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

g.test_one_condition_number = function()
    local filter_conditions = {
        {
            fieldnos = {1},
            operator = select_conditions.operators.EQ,
            values = {3},
            types = {'number'},
            early_exit_is_possible = true,
        }
    }

    local expected_code = [[local tuple = ...

local field_1 = tuple[1]

if not eq_1(field_1) then return false, true end

return true, false]]

    local expected_library_code = [[local M = {}

function M.eq_1(field_1)
    return (eq(field_1, 3))
end

return M]]

    local filter_code = select_filters.internal.gen_filter_code(filter_conditions)
    t.assert_equals(filter_code.code, expected_code)
    t.assert_equals(filter_code.library, expected_library_code)

    local filter_func = select_filters.internal.compile(filter_code)
    t.assert_equals({ filter_func({3, 2, 1}) }, {true, false})
    t.assert_equals({ filter_func({2, 2, 1}) }, {false, true})
    t.assert_equals({ filter_func({nil, 2, 1}) }, {false, true})
end

g.test_one_condition_boolean = function()
    local filter_conditions = {
        {
            fieldnos = {1},
            operator = select_conditions.operators.EQ,
            values = {true},
            types = {'boolean'},
            early_exit_is_possible = true,
            values_opts = {
                {is_boolean = true, is_nullable = true},
            },
        }
    }

    local expected_code = [[local tuple = ...

local field_1 = tuple[1]

if not eq_1(field_1) then return false, true end

return true, false]]

    local expected_library_code = [[local M = {}

function M.eq_1(field_1)
    return (eq(field_1, true))
end

return M]]

    local filter_code = select_filters.internal.gen_filter_code(filter_conditions)
    t.assert_equals(filter_code.code, expected_code)
    t.assert_equals(filter_code.library, expected_library_code)

    local filter_func = select_filters.internal.compile(filter_code)
    t.assert_equals({ filter_func({true, 2, 1}) }, {true, false})
    t.assert_equals({ filter_func({false, 2, 1}) }, {false, true})
    t.assert_equals({ filter_func({nil, 2, 1}) }, {false, true})
end

g.test_one_condition_string = function()
    local filter_conditions = {
        {
            fieldnos = {2},
            operator = select_conditions.operators.GT,
            values = {'dddddddd'},
            types = {'string'},
            early_exit_is_possible = true,
        },
    }

    local expected_code = [[local tuple = ...

local field_2 = tuple[2]

if not cmp_1(field_2) then return false, true end

return true, false]]

    local expected_library_code = [[local M = {}

function M.cmp_1(field_2)
    if lt(field_2, "dddddddd") then return false end
    if not eq(field_2, "dddddddd") then return true end

    return false
end

return M]]

    local filter_code = select_filters.internal.gen_filter_code(filter_conditions)
    t.assert_equals(filter_code.code, expected_code)
    t.assert_equals(filter_code.library, expected_library_code)

    local filter_func = select_filters.internal.compile(filter_code)
    t.assert_equals({ filter_func({3, 'ddddeeee', 1}) }, {true, false})
    t.assert_equals({ filter_func({3, 'dddddddd', 1}) }, {false, true})
    t.assert_equals({ filter_func({3, 'aaaaaaaa', 1}) }, {false, true})
    t.assert_equals({ filter_func({3, nil, 1}) }, {false, true})
end

g.test_two_conditions = function()
    local filter_conditions = {
        {
            fieldnos = {1},
            operator = select_conditions.operators.EQ,
            values = {4},
            types = {'number'},
            early_exit_is_possible = true,
        },
        {
            fieldnos = {3},
            operator = select_conditions.operators.GE,
            values = {"dddddddd"},
            types = {'string'},
            early_exit_is_possible = false,
        }
    }

    local expected_code = [[local tuple = ...

local field_1 = tuple[1]
local field_3 = tuple[3]

if not eq_1(field_1) then return false, true end
if not cmp_2(field_3) then return false, false end

return true, false]]

    local expected_library_code = [[local M = {}

function M.eq_1(field_1)
    return (eq(field_1, 4))
end

function M.cmp_2(field_3)
    if lt(field_3, "dddddddd") then return false end
    if not eq(field_3, "dddddddd") then return true end

    return true
end

return M]]

    local filter_code = select_filters.internal.gen_filter_code(filter_conditions)
    t.assert_equals(filter_code.code, expected_code)
    t.assert_equals(filter_code.library, expected_library_code)

    local filter_func = select_filters.internal.compile(filter_code)
    t.assert_equals({ filter_func({4, 'xxx', 'dddddddd'}) }, {true, false})
    t.assert_equals({ filter_func({5, 'xxx', 'dddddddd'}) }, {false, true})
    t.assert_equals({ filter_func({4, 'xxx', 'dddddeee'}) }, {true, false})
    t.assert_equals({ filter_func({4, 'xxx', 'aaaaaaaa'}) }, {false, false})
    t.assert_equals({ filter_func({nil, 'xxx', 'dddddeee'}) }, {false, true})
    t.assert_equals({ filter_func({4, 'xxx', nil}) }, {false, false})
end

g.test_two_conditions_non_nullable = function()
    local filter_conditions = {
        {
            fieldnos = {2, 3},
            operator = select_conditions.operators.GE,
            values = {"test", 5},
            types = {'string', 'number'},
            early_exit_is_possible = false,
            values_opts = {
                {is_nullable = false},
                {is_nullable = true},
            }
        },
        {
            fieldnos = {1},
            operator = select_conditions.operators.LT,
            values = {3},
            types = {'number'},
            early_exit_is_possible = true,
            values_opts = {
                {is_nullable = false},
            }
        },
    }

    local expected_code = [[local tuple = ...

local field_2 = tuple[2]
local field_3 = tuple[3]
local field_1 = tuple[1]

if not cmp_1(field_2, field_3) then return false, false end
if not cmp_2(field_1) then return false, true end

return true, false]]

    local expected_library_code = [[local M = {}

function M.cmp_1(field_2, field_3)
    if lt_strict(field_2, "test") then return false end
    if not eq(field_2, "test") then return true end

    if lt(field_3, 5) then return false end
    if not eq(field_3, 5) then return true end

    return true
end

function M.cmp_2(field_1)
    if lt_strict(field_1, 3) then return true end
    if not eq(field_1, 3) then return false end

    return false
end

return M]]

    local filter_code = select_filters.internal.gen_filter_code(filter_conditions)
    t.assert_equals(filter_code.code, expected_code)
    t.assert_equals(filter_code.library, expected_library_code)

    local filter_func = select_filters.internal.compile(filter_code)
    t.assert_equals({ filter_func({2, 'test', 5}) }, {true, false})
    t.assert_equals({ filter_func({4, 'test', 5}) }, {false, true})
    t.assert_equals({ filter_func({4, 'test', nil}) }, {false, false})
    t.assert_equals({ filter_func({5, 'test', nil}) }, {false, false})
end

g.test_one_condition_with_nil_value = function()
    local filter_conditions = {
        {
            fieldnos = {2, 3},
            operator = select_conditions.operators.GE,
            values = {"test"},
            types = {'string', 'number'},
            early_exit_is_possible = false,
            values_opts = {
                {is_nullable = false},
                {is_nullable = true},
            }
        },
    }

    local expected_code = [[local tuple = ...

local field_2 = tuple[2]

if not cmp_1(field_2) then return false, false end

return true, false]]

    local expected_library_code = [[local M = {}

function M.cmp_1(field_2)
    if lt_strict(field_2, "test") then return false end
    if not eq(field_2, "test") then return true end

    return true
end

return M]]

    local filter_code = select_filters.internal.gen_filter_code(filter_conditions)
    t.assert_equals(filter_code.code, expected_code)
    t.assert_equals(filter_code.library, expected_library_code)

    local filter_func = select_filters.internal.compile(filter_code)
    t.assert_equals(filter_func({1, 'test', 3}), true)
    t.assert_equals(filter_func({1, 'xxx', 1}), true)
end

g.test_unicode_collation = function()
    local filter_conditions = {
        {
            fieldnos = {1, 2, 3, 4},
            operator = select_conditions.operators.EQ,
            values = {'A', 'Á', 'Ä', 6},
            types = {'string', 'string', 'string', 'number'},
            early_exit_is_possible = false,
            values_opts = {
                {collation='unicode'},
                {collation='unicode_ci'},
                {collation='unicode_ci'},
            }
        },
    }

    local expected_code = [[local tuple = ...

local field_1 = tuple[1]
local field_2 = tuple[2]
local field_3 = tuple[3]
local field_4 = tuple[4]

if not eq_1(field_1, field_2, field_3, field_4) then return false, false end

return true, false]]

    local expected_library_code = [[local M = {}

function M.eq_1(field_1, field_2, field_3, field_4)
    return (eq_unicode(field_1, "A") and eq_unicode_ci(field_2, "Á") and eq_unicode_ci(field_3, "Ä") and eq(field_4, 6))
end

return M]]

    local filter_code = select_filters.internal.gen_filter_code(filter_conditions)
    t.assert_equals(filter_code.code, expected_code)
    t.assert_equals(filter_code.library, expected_library_code)

    local filter_func = select_filters.internal.compile(filter_code)
    t.assert_equals(filter_func({'A', 'Á', 'Ä', 6}), true)
    t.assert_equals(filter_func({'A', 'á', 'ä', 6}), true)
    t.assert_equals(filter_func({'a', 'Á', 'Ä', 6}), false)
    t.assert_equals(filter_func({'A', 'V', 'ä', 6}), false)
end

g.test_binary_and_none_collation = function()
    local filter_conditions = {
        {
            fieldnos = {1, 2, 3},
            operator = select_conditions.operators.EQ,
            values = {'A', 'B', 'C'},
            types = {'string', 'string', 'string'},
            early_exit_is_possible = false,
            values_opts = {
                {collation='none'},
                {collation='binary'},
                {collation=nil},
            }
        },
    }

    local expected_code = [[local tuple = ...

local field_1 = tuple[1]
local field_2 = tuple[2]
local field_3 = tuple[3]

if not eq_1(field_1, field_2, field_3) then return false, false end

return true, false]]

    local expected_library_code = [[local M = {}

function M.eq_1(field_1, field_2, field_3)
    return (eq(field_1, "A") and eq(field_2, "B") and eq(field_3, "C"))
end

return M]]

    local filter_code = select_filters.internal.gen_filter_code(filter_conditions)
    t.assert_equals(filter_code.code, expected_code)
    t.assert_equals(filter_code.library, expected_library_code)

    local filter_func = select_filters.internal.compile(filter_code)
    t.assert_equals(filter_func({'A', 'B', 'C'}), true)
    t.assert_equals(filter_func({'a', 'B', 'C'}), false)
    t.assert_equals(filter_func({'A', 'b', 'C'}), false)
    t.assert_equals(filter_func({'A', 'B', 'c'}), false)
end

g.test_null_as_last_value_eq = function()
    local filter_conditions = {
        {
            fieldnos = {1, 2},
            operator = select_conditions.operators.EQ,
            values = {'a', box.NULL},
            types = {'string', 'string'},
            early_exit_is_possible = false,
            values_opts = {
                nil,
                {is_nullable = true},
            }
        },
    }

    local expected_code = [[local tuple = ...

local field_1 = tuple[1]
local field_2 = tuple[2]

if not eq_1(field_1, field_2) then return false, false end

return true, false]]

    local expected_library_code = [[local M = {}

function M.eq_1(field_1, field_2)
    return (eq(field_1, "a") and eq(field_2, NULL))
end

return M]]

    local filter_code = select_filters.internal.gen_filter_code(filter_conditions)
    t.assert_equals(filter_code.code, expected_code)
    t.assert_equals(filter_code.library, expected_library_code)

    local filter_func = select_filters.internal.compile(filter_code)
    t.assert_equals(filter_func({'a', box.NULL}), true)
    t.assert_equals(filter_func({'a', 'a'}), false) -- 'a' ~= box.NULL
end

g.test_null_as_last_value_gt = function()
    local filter_conditions = {
        {
            fieldnos = {1, 2},
            operator = select_conditions.operators.GT,
            values = {'a', box.NULL},
            types = {'string', 'string'},
            early_exit_is_possible = false,
            values_opts = {
                nil,
                {is_nullable = true},
            }
        },
    }

    local expected_code = [[local tuple = ...

local field_1 = tuple[1]
local field_2 = tuple[2]

if not cmp_1(field_1, field_2) then return false, false end

return true, false]]

    local expected_library_code = [[local M = {}

function M.cmp_1(field_1, field_2)
    if lt(field_1, "a") then return false end
    if not eq(field_1, "a") then return true end

    if lt(field_2, NULL) then return false end
    if not eq(field_2, NULL) then return true end

    return false
end

return M]]

    local filter_code = select_filters.internal.gen_filter_code(filter_conditions)
    t.assert_equals(filter_code.code, expected_code)
    t.assert_equals(filter_code.library, expected_library_code)

    local filter_func = select_filters.internal.compile(filter_code)
    t.assert_equals(filter_func({'a', 'a'}), true)       -- 'a' > box.NULL
    t.assert_equals(filter_func({'a', box.NULL}), false) -- box.NULL > box.NULL is false
end

g.test_null_as_last_value_gt_non_nullable = function()
    local filter_conditions = {
        {
            fieldnos = {1, 2},
            operator = select_conditions.operators.GT,
            values = {'a', box.NULL},
            types = {'string', 'string'},
            early_exit_is_possible = false,
            values_opts = {
                nil,
                {is_nullable = false},
            }
        },
    }

    local expected_code = [[local tuple = ...

local field_1 = tuple[1]
local field_2 = tuple[2]

if not cmp_1(field_1, field_2) then return false, false end

return true, false]]

    local expected_library_code = [[local M = {}

function M.cmp_1(field_1, field_2)
    if lt(field_1, "a") then return false end
    if not eq(field_1, "a") then return true end

    if lt_strict(field_2, NULL) then return false end
    if not eq(field_2, NULL) then return true end

    return false
end

return M]]

    local filter_code = select_filters.internal.gen_filter_code(filter_conditions)
    t.assert_equals(filter_code.code, expected_code)
    t.assert_equals(filter_code.library, expected_library_code)

    local filter_func = select_filters.internal.compile(filter_code)
    t.assert_equals(filter_func({'a', 'a'}), true)       -- 'a' > box.NULL
    t.assert_equals(filter_func({'a', box.NULL}), false) -- box.NULL > box.NULL is false
end

g.test_select_filter_get_index_by_name = function()
    local space_indexes = {
        [0] = {name = 'primary'},
        [1] = {name = 'second'},
        [2] = {name = 'third'}
    }

    local index = select_filters.internal.get_index_by_name(space_indexes, "primary");
    t.assert_equals(index, space_indexes[0]);

    local index = select_filters.internal.get_index_by_name(space_indexes, "third");
    t.assert_equals(index, space_indexes[2]);

    local index = select_filters.internal.get_index_by_name(space_indexes, "not_exist_index");
    t.assert_equals(index, nil)
end

-- luacheck: pop
