local _, datetime = pcall(require, 'datetime')

local compare_conditions = require('crud.compare.conditions')
local cond_funcs = compare_conditions.funcs
local select_filters = require('crud.compare.filters')
local collations = require('crud.common.collations')
local select_plan = require('crud.compare.plan')

local t = require('luatest')
local g = t.group('select_filters_datetime')

local helpers = require('test.helper')

g.before_all = function()
    helpers.skip_datetime_unsupported()

    helpers.box_cfg()

    local customers_space = box.schema.space.create('customers', {
        format = {
            {'datetime', 'datetime'},
            {'bucket_id', 'unsigned'},
            {'name', 'string'},
            {'second_datetime', 'datetime'},
        },
        if_not_exists = true,
    })
    customers_space:create_index('datetime', { -- id: 0
        parts = {'datetime'},
        if_not_exists = true,
    })
    customers_space:create_index('second_datetime', { -- id: 1
        parts = {
            { field = 'second_datetime', is_nullable = true },
        },
        if_not_exists = true,
    })
end

g.after_all(function()
    box.space.customers:drop()
end)

g.test_parse = function()
    -- select by indexed field with conditions by index and field
    local dt1 = datetime.new{year = 2000, month = 1, day = 1, tz = 'Europe/Moscow'}
    local dt2 = datetime.new{year = 2012, month = 1, day = 1, tzoffset = -180}
    local dt3 = datetime.new{year = 1980, month = 1, day = 1}

    local conditions = {
        cond_funcs.gt('datetime', dt1),
        cond_funcs.lt('datetime', dt2),
        cond_funcs.eq('name', 'Charlie'),
        cond_funcs.eq('second_datetime', dt3)
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

    -- datetime filter (early exit is possible)
    local datetime_filter_condition = filter_conditions[1]
    t.assert_type(datetime_filter_condition, 'table')
    t.assert_equals(datetime_filter_condition.fields, {1})
    t.assert_equals(datetime_filter_condition.operator, compare_conditions.operators.LT)
    t.assert_equals(datetime_filter_condition.values, {dt2})
    t.assert_equals(datetime_filter_condition.types, {'datetime'})
    t.assert_equals(datetime_filter_condition.early_exit_is_possible, true)

    -- name filter
    local name_filter_condition = filter_conditions[2]
    t.assert_type(name_filter_condition, 'table')
    t.assert_equals(name_filter_condition.fields, {3})
    t.assert_equals(name_filter_condition.operator, compare_conditions.operators.EQ)
    t.assert_equals(name_filter_condition.values, {'Charlie'})
    t.assert_equals(name_filter_condition.types, {'string'})
    t.assert_equals(name_filter_condition.early_exit_is_possible, false)

    -- second_datetime filter
    local second_datetime_filter_condition = filter_conditions[3]
    t.assert_type(second_datetime_filter_condition, 'table')
    t.assert_equals(second_datetime_filter_condition.fields, {4})
    t.assert_equals(second_datetime_filter_condition.operator, compare_conditions.operators.EQ)
    t.assert_equals(second_datetime_filter_condition.values, {dt3})
    t.assert_equals(second_datetime_filter_condition.types, {'datetime'})
    t.assert_equals(second_datetime_filter_condition.early_exit_is_possible, false)

    t.assert_equals(#second_datetime_filter_condition.values_opts, 1)
    local second_datetime_opts = second_datetime_filter_condition.values_opts[1]
    t.assert_equals(second_datetime_opts.is_nullable, true)
    t.assert_equals(second_datetime_opts.collation, collations.NONE)
end

g.test_one_condition_datetime = function()
    local dt1 = datetime.new{year = 2000, month = 1, day = 1, tz = 'Europe/Moscow'}
    local dt2 = datetime.new{year = 2012, month = 1, day = 1, tzoffset = -180}

    local filter_conditions = {
        {
            fields = {1},
            operator = compare_conditions.operators.EQ,
            values = {dt1},
            types = {'datetime'},
            early_exit_is_possible = true,
        }
    }

    local expected_code = [[local tuple = ...

local field_1 = tuple[1]

if not eq_1(field_1) then return false, true end

return true, false]]

    local expected_library_code = [[local M = {}

function M.eq_1(field_1)
    return (eq_datetime(field_1, "2000-01-01T00:00:00 Europe/Moscow"))
end

return M]]

    local filter_code = select_filters.internal.gen_filter_code(filter_conditions)
    t.assert_equals(filter_code.code, expected_code)
    t.assert_equals(filter_code.library, expected_library_code)

    local filter_func = select_filters.internal.compile(filter_code)
    t.assert_equals({ filter_func({dt1, dt1:format(), 1}) }, {true, false})
    t.assert_equals({ filter_func({dt2, dt1:format(), 1}) }, {false, true})
    t.assert_equals({ filter_func({nil, dt1:format(), 1}) }, {false, true})
end

g.test_one_condition_datetime_gt = function()
    local dt1 = datetime.new{year = 2000, month = 1, day = 1, tz = 'Europe/Moscow'}
    local dt2 = datetime.new{year = 2012, month = 1, day = 1, tzoffset = -180}

    local filter_conditions = {
        {
            fields = {1},
            operator = compare_conditions.operators.GT,
            values = {dt1},
            types = {'datetime'},
            early_exit_is_possible = true,
        }
    }

    local expected_code = [[local tuple = ...

local field_1 = tuple[1]

if not cmp_1(field_1) then return false, true end

return true, false]]

    local expected_library_code = [[local M = {}

function M.cmp_1(field_1)
    if lt_datetime(field_1, "2000-01-01T00:00:00 Europe/Moscow") then return false end
    if not eq_datetime(field_1, "2000-01-01T00:00:00 Europe/Moscow") then return true end

    return false
end

return M]]

    local filter_code = select_filters.internal.gen_filter_code(filter_conditions)
    t.assert_equals(filter_code.code, expected_code)
    t.assert_equals(filter_code.library, expected_library_code)

    local filter_func = select_filters.internal.compile(filter_code)
    t.assert_equals({ filter_func({dt2, dt1:format(), 1}) }, {true, false})
    t.assert_equals({ filter_func({dt1, dt2:format(), 1}) }, {false, true})
    t.assert_equals({ filter_func({nil, dt1:format(), 1}) }, {false, true})
end

g.test_one_condition_datetime_with_nil_value = function()
    local dt1 = datetime.new{year = 2000, month = 1, day = 1, tz = 'Europe/Moscow'}
    local dt2 = datetime.new{year = 2012, month = 1, day = 1, tzoffset = -180}

    local filter_conditions = {
        {
            fields = {1, 3},
            operator = compare_conditions.operators.GE,
            values = {dt1},
            types = {'datetime', 'string'},
            early_exit_is_possible = false,
            values_opts = {
                {is_nullable = false},
                {is_nullable = true},
            },
        },
    }

    local expected_code = [[local tuple = ...

local field_1 = tuple[1]

if not cmp_1(field_1) then return false, false end

return true, false]]

    local expected_library_code = [[local M = {}

function M.cmp_1(field_1)
    if lt_datetime_strict(field_1, "2000-01-01T00:00:00 Europe/Moscow") then return false end
    if not eq_datetime(field_1, "2000-01-01T00:00:00 Europe/Moscow") then return true end

    return true
end

return M]]

    local filter_code = select_filters.internal.gen_filter_code(filter_conditions)
    t.assert_equals(filter_code.code, expected_code)
    t.assert_equals(filter_code.library, expected_library_code)

    local filter_func = select_filters.internal.compile(filter_code)
    t.assert_equals(filter_func({dt1, 'test', 3}), true)
    t.assert_equals(filter_func({dt2, 'xxx', 1}), true)
end

g.test_two_conditions_datetime = function()
    local dt1 = datetime.new{year = 2000, month = 1, day = 1, tz = 'Europe/Moscow'}
    local dt2 = datetime.new{year = 2012, month = 1, day = 1, tzoffset = -180}

    local filter_conditions = {
        {
            fields = {2},
            operator = compare_conditions.operators.EQ,
            values = {'Charlie'},
            types = {'string'},
            early_exit_is_possible = true,
        },
        {
            fields = {3},
            operator = compare_conditions.operators.GE,
            values = {dt2:format()},
            types = {'datetime'},
            early_exit_is_possible = false,
        }
    }

    local expected_code = [[local tuple = ...

local field_2 = tuple[2]
local field_3 = tuple[3]

if not eq_1(field_2) then return false, true end
if not cmp_2(field_3) then return false, false end

return true, false]]

    local expected_library_code = [[local M = {}

function M.eq_1(field_2)
    return (eq(field_2, "Charlie"))
end

function M.cmp_2(field_3)
    if lt_datetime(field_3, "2012-01-01T00:00:00-0300") then return false end
    if not eq_datetime(field_3, "2012-01-01T00:00:00-0300") then return true end

    return true
end

return M]]

    local filter_code = select_filters.internal.gen_filter_code(filter_conditions)
    t.assert_equals(filter_code.code, expected_code)
    t.assert_equals(filter_code.library, expected_library_code)

    local filter_func = select_filters.internal.compile(filter_code)
    t.assert_equals({ filter_func({4, 'xxx', dt1}) }, {false, true})
    t.assert_equals({ filter_func({5, 'Charlie', dt1}) }, {false, false})
    t.assert_equals({ filter_func({5, 'xxx', dt2}) }, {false, true})
    t.assert_equals({ filter_func({6, 'Charlie', dt2}) }, {true, false})
    t.assert_equals({ filter_func({6, 'Charlie', nil}) }, {false, false})
end
