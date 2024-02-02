-- luacheck: push max_line_length 300

local compare_conditions = require('crud.compare.conditions')
local cond_funcs = compare_conditions.funcs
local select_filters = require('crud.compare.filters')
local select_plan = require('crud.compare.plan')
local collations = require('crud.common.collations')

local t = require('luatest')
local g = t.group('select_filters')

local crud_utils = require('crud.common.utils')

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
            {field = 'name', collation = 'unicode_ci'},
            {field = 'last_name', is_nullable = true},
        },
        unique = false,
        if_not_exists = true,
    })

    if crud_utils.tarantool_supports_jsonpath_indexes() then
        local cars_space = box.schema.space.create('cars', {
            format = {
                {name = 'id', type = 'map'},
                {name = 'bucket_id', type = 'unsigned'},
                {name = 'age', type = 'number'},
                {name = 'manufacturer', type = 'string'},
                {name = 'data', type = 'map'}
            },
            if_not_exists = true,
        })

        -- primary index
        cars_space:create_index('id_ind', {
            parts = {
                {1, 'unsigned', path = 'car_id.signed'},
            },
            if_not_exists = true,
        })

        cars_space:create_index('bucket_id', {
            parts = { 'bucket_id' },
            unique = false,
            if_not_exists = true,
        })

        cars_space:create_index('data_index', {
            parts = {
                {5, 'str', path = 'car.color'},
                {5, 'str', path = 'car.model'},
            },
            unique = false,
            if_not_exists = true,
        })
    end
end

g.after_all(function()
    box.space.customers:drop()
    box.space.cars:drop()
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
    local scan_index = space.index[plan.index_id]

    local filter_conditions, err = select_filters.internal.parse(space, scan_index, conditions, {
        scan_condition_num = plan.scan_condition_num,
        tarantool_iter = plan.tarantool_iter,
    })
    t.assert_equals(err, nil)

    -- age filter (early exit is possible)
    local age_filter_condition = filter_conditions[1]
    t.assert_type(age_filter_condition, 'table')
    t.assert_equals(age_filter_condition.fields, {5})
    t.assert_equals(age_filter_condition.operator, compare_conditions.operators.LT)
    t.assert_equals(age_filter_condition.values, {40})
    t.assert_equals(age_filter_condition.types, {'number'})
    t.assert_equals(age_filter_condition.early_exit_is_possible, true)

    -- full_name filter
    local full_name_filter_condition = filter_conditions[2]
    t.assert_type(full_name_filter_condition, 'table')
    t.assert_equals(full_name_filter_condition.fields, {3, 4})
    t.assert_equals(full_name_filter_condition.operator, compare_conditions.operators.EQ)
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
    t.assert_equals(has_a_car_filter_condition.fields, {7})
    t.assert_equals(has_a_car_filter_condition.operator, compare_conditions.operators.EQ)
    t.assert_equals(has_a_car_filter_condition.values, {true})
    t.assert_equals(has_a_car_filter_condition.types, {'boolean'})
    t.assert_equals(has_a_car_filter_condition.early_exit_is_possible, false)

    t.assert_equals(#has_a_car_filter_condition.values_opts, 1)
    local has_a_car_opts = has_a_car_filter_condition.values_opts[1]
    t.assert_equals(has_a_car_opts.is_nullable, false)
    t.assert_equals(has_a_car_opts.collation, nil)
end

local gh_418_cases = {
    scan_condition = {
        eq = {
            conditions = cond_funcs.eq('age', 20),
        },
        le = {
            conditions = cond_funcs.le('age', 20),
        },
        lt = {
            conditions = cond_funcs.lt('age', 20),
        },
        ge = {
            conditions = cond_funcs.ge('age', 20),
        },
        gt = {
            conditions = cond_funcs.gt('age', 20),
        },
        req = {
            conditions = cond_funcs.eq('age', 20),
            opts = {first = -1},
        },
    },
    secondary_condition = {
        eq = {
            conditions = cond_funcs.eq('full_name', {'Ivan', 'Ivanov'}),
        },
        le = {
            conditions = cond_funcs.le('full_name', {'Ivan', 'Ivanov'}),
        },
        lt = {
            conditions = cond_funcs.lt('full_name', {'Ivan', 'Ivanov'}),
        },
        ge = {
            conditions = cond_funcs.ge('full_name', {'Ivan', 'Ivanov'}),
        },
        gt = {
            conditions = cond_funcs.gt('full_name', {'Ivan', 'Ivanov'}),
        },
    }
}

for scan_name, scan_case in pairs(gh_418_cases.scan_condition) do
    for sec_name, sec_case in pairs(gh_418_cases.secondary_condition) do
        local test_name = ('test_gh_418_scan_%s_secondary_%s_no_early_exit'):format(scan_name, sec_name)

        g[test_name] = function()
            local conditions = {
                scan_case.conditions,
                sec_case.conditions,
            }

            local plan, err = select_plan.new(box.space.customers, conditions, scan_case.opts)
            t.assert_equals(err, nil)

            local space = box.space.customers
            local scan_index = space.index[plan.index_id]

            local filter_conditions, err = select_filters.internal.parse(
                space,
                scan_index,
                conditions,
                {
                    scan_condition_num = plan.scan_condition_num,
                    tarantool_iter = plan.tarantool_iter,
                }
            )
            t.assert_equals(err, nil)

            local full_name_filter_condition = filter_conditions[1]
            t.assert_equals(full_name_filter_condition.early_exit_is_possible, false)
        end
    end
end

g.test_one_condition_number = function()
    local filter_conditions = {
        {
            fields = {1},
            operator = compare_conditions.operators.EQ,
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
            fields = {1},
            operator = compare_conditions.operators.EQ,
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
            fields = {2},
            operator = compare_conditions.operators.GT,
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
            fields = {1},
            operator = compare_conditions.operators.EQ,
            values = {4},
            types = {'number'},
            early_exit_is_possible = true,
        },
        {
            fields = {3},
            operator = compare_conditions.operators.GE,
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
            fields = {2, 3},
            operator = compare_conditions.operators.GE,
            values = {"test", 5},
            types = {'string', 'number'},
            early_exit_is_possible = false,
            values_opts = {
                {is_nullable = false},
                {is_nullable = true},
            },
        },
        {
            fields = {1},
            operator = compare_conditions.operators.LT,
            values = {3},
            types = {'number'},
            early_exit_is_possible = true,
            values_opts = {
                {is_nullable = false},
            },
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
            fields = {2, 3},
            operator = compare_conditions.operators.GE,
            values = {"test"},
            types = {'string', 'number'},
            early_exit_is_possible = false,
            values_opts = {
                {is_nullable = false},
                {is_nullable = true},
            },
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
            fields = {1, 2, 3, 4},
            operator = compare_conditions.operators.EQ,
            values = {'A', 'Á', 'Ä', 6},
            types = {'string', 'string', 'string', 'number'},
            early_exit_is_possible = false,
            values_opts = {
                {collation='unicode'},
                {collation='unicode_ci'},
                {collation='unicode_ci'},
            },
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
            fields = {1, 2, 3},
            operator = compare_conditions.operators.EQ,
            values = {'A', 'B', 'C'},
            types = {'string', 'string', 'string'},
            early_exit_is_possible = false,
            values_opts = {
                {collation='none'},
                {collation='binary'},
                {collation=nil},
            },
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
            fields = {1, 2},
            operator = compare_conditions.operators.EQ,
            values = {'a', box.NULL},
            types = {'string', 'string'},
            early_exit_is_possible = false,
            values_opts = {
                nil,
                {is_nullable = true},
            },
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
            fields = {1, 2},
            operator = compare_conditions.operators.GT,
            values = {'a', box.NULL},
            types = {'string', 'string'},
            early_exit_is_possible = false,
            values_opts = {
                nil,
                {is_nullable = true},
            },
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
            fields = {1, 2},
            operator = compare_conditions.operators.GT,
            values = {'a', box.NULL},
            types = {'string', 'string'},
            early_exit_is_possible = false,
            values_opts = {
                nil,
                {is_nullable = false},
            },
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

g.test_jsonpath_fields_eq = function()
    local filter_conditions = {
        {
            fields = {'[2].a.b'},
            operator = compare_conditions.operators.EQ,
            values = {55},
            types = {'number'},
            early_exit_is_possible = true,
        },
    }

    local expected_code = [[local tuple = ...

local field__2__a_b = tuple["[2].a.b"]

if not eq_1(field__2__a_b) then return false, true end

return true, false]]

    local expected_library_code = [[local M = {}

function M.eq_1(field__2__a_b)
    return (eq(field__2__a_b, 55))
end

return M]]

    local filter_code = select_filters.internal.gen_filter_code(filter_conditions)
    t.assert_equals(filter_code.code, expected_code)
    t.assert_equals(filter_code.library, expected_library_code)

    local filter_func = select_filters.internal.compile(filter_code)
    t.assert_equals({ filter_func(box.tuple.new({3, {a = {b = 55}}, 1})) }, {true, false})
    t.assert_equals({ filter_func(box.tuple.new({3, {a = {b = 23}}, 1})) }, {false, true})
    t.assert_equals({ filter_func(box.tuple.new({3, {a = {c = 55}}, 1})) }, {false, true})
    t.assert_equals({ filter_func(box.tuple.new({3, nil, 1})) }, {false, true})
end

g.test_jsonpath_fields_ge = function()
    local filter_conditions = {
        {
            fields = {'[2]["field_2"]'},
            operator = compare_conditions.operators.GT,
            values = {23},
            types = {'number'},
            early_exit_is_possible = true,
        },
    }

    local expected_code = [[local tuple = ...

local field__2___field_2__ = tuple["[2][\"field_2\"]"]

if not cmp_1(field__2___field_2__) then return false, true end

return true, false]]

    local expected_library_code = [[local M = {}

function M.cmp_1(field__2___field_2__)
    if lt(field__2___field_2__, 23) then return false end
    if not eq(field__2___field_2__, 23) then return true end

    return false
end

return M]]

    local filter_code = select_filters.internal.gen_filter_code(filter_conditions)
    t.assert_equals(filter_code.code, expected_code)
    t.assert_equals(filter_code.library, expected_library_code)

    local filter_func = select_filters.internal.compile(filter_code)
    t.assert_equals({ filter_func(box.tuple.new({3, {field_2 = 55}, 1})) }, {true, false})
    t.assert_equals({ filter_func(box.tuple.new({3, {field_2 = 24, field_3 = 32}, nil})) }, {true, false})
    t.assert_equals({ filter_func(box.tuple.new({{field_2 = 59}, 173, 1})) }, {false, true})
end

g.test_several_jsonpath = function()
    local filter_conditions = {
        {
            fields = {'[3]["f2"][\'f3\']', '[4].f3'},
            operator = compare_conditions.operators.EQ,
            values = {'a', 'b'},
            types = {'string', 'string'},
            early_exit_is_possible = true,
        },
    }

    local expected_code = [[local tuple = ...

local field__3___f2____f3__ = tuple["[3][\"f2\"]['f3']"]
local field__4__f3 = tuple["[4].f3"]

if not eq_1(field__3___f2____f3__, field__4__f3) then return false, true end

return true, false]]

    local expected_library_code = [[local M = {}

function M.eq_1(field__3___f2____f3__, field__4__f3)
    return (eq(field__3___f2____f3__, "a") and eq(field__4__f3, "b"))
end

return M]]

    local filter_code = select_filters.internal.gen_filter_code(filter_conditions)
    t.assert_equals(filter_code.code, expected_code)
    t.assert_equals(filter_code.library, expected_library_code)

    local filter_func = select_filters.internal.compile(filter_code)
    t.assert_equals({ filter_func(box.tuple.new({1, 2, {f2 = {f3 = "a"}}, {f3 = "b"}})) }, {true, false})
    t.assert_equals({ filter_func(box.tuple.new({1, 2, {f3 = "b"}, {f2 = {f3 = "a"}}})) }, {false, true})
    t.assert_equals({ filter_func(box.tuple.new({1, 2, {f2 = {f3 = "a"}}, "b"})) }, {false, true})
end

g.test_jsonpath_two_conditions = function()
    local filter_conditions = {
        {
            fields = {'[2].fld_1', '[3]["f_1"]'},
            operator = compare_conditions.operators.GE,
            values = {"jsonpath_test", 23},
            types = {'string', 'number'},
            early_exit_is_possible = false,
        },
        {
            fields = {'[1].field_1'},
            operator = compare_conditions.operators.LT,
            values = {8},
            types = {'number'},
            early_exit_is_possible = true,
        },
    }

    local expected_code = [[local tuple = ...

local field__2__fld_1 = tuple["[2].fld_1"]
local field__3___f_1__ = tuple["[3][\"f_1\"]"]
local field__1__field_1 = tuple["[1].field_1"]

if not cmp_1(field__2__fld_1, field__3___f_1__) then return false, false end
if not cmp_2(field__1__field_1) then return false, true end

return true, false]]

    local expected_library_code = [[local M = {}

function M.cmp_1(field__2__fld_1, field__3___f_1__)
    if lt(field__2__fld_1, "jsonpath_test") then return false end
    if not eq(field__2__fld_1, "jsonpath_test") then return true end

    if lt(field__3___f_1__, 23) then return false end
    if not eq(field__3___f_1__, 23) then return true end

    return true
end

function M.cmp_2(field__1__field_1)
    if lt(field__1__field_1, 8) then return true end
    if not eq(field__1__field_1, 8) then return false end

    return false
end

return M]]

    local filter_code = select_filters.internal.gen_filter_code(filter_conditions)
    t.assert_equals(filter_code.code, expected_code)
    t.assert_equals(filter_code.library, expected_library_code)

    local filter_func = select_filters.internal.compile(filter_code)
    t.assert_equals({ filter_func(box.tuple.new({{field_1 = 7}, {fld_1 = "jsonpath_test"}, {f_1 = 23}})) }, {true, false})
    t.assert_equals({ filter_func(box.tuple.new({{field_1 = 8}, {fld_1 = "jsonpath_test"}, {f_1 = 23}})) }, {false, true})
    t.assert_equals({ filter_func(box.tuple.new({{field_1 = 5}, {f_1 = "jsonpath_test"}, {fld_1 = 23}})) }, {false, false})
    t.assert_equals({ filter_func(box.tuple.new({{field_1 = 5, f2 = 3}, {fld_1 = "jsonpath_test"}, 23})) }, {false, false})
end

g.test_jsonpath_indexes = function()
    t.skip_if(
        not crud_utils.tarantool_supports_jsonpath_indexes(),
        "Jsonpath indexes supported since 2.6.3/2.7.2/2.8.1"
    )

    local conditions = {
        cond_funcs.gt('id', 20),
        cond_funcs.eq('data_index', {'Yellow', 'BMW'})
    }

    local plan, err = select_plan.new(box.space.cars, conditions)
    t.assert_equals(err, nil)

    local space = box.space.cars
    local scan_index = space.index[plan.index_id]

    local filter_conditions, err = select_filters.internal.parse(space, scan_index, conditions, {
        scan_condition_num = plan.scan_condition_num,
        tarantool_iter = plan.tarantool_iter,
    })

    t.assert_equals(err, nil)

    local data_condition = filter_conditions[1]
    t.assert_type(data_condition, 'table')
    t.assert_equals(data_condition.fields, {"[5]car.color", "[5]car.model"})
    t.assert_equals(data_condition.operator, compare_conditions.operators.EQ)
    t.assert_equals(data_condition.values, {'Yellow', 'BMW'})
    t.assert_equals(data_condition.types, {'string', 'string'})
    t.assert_equals(data_condition.early_exit_is_possible, false)
end

-- luacheck: pop
