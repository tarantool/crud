-- luacheck: push max_line_length 300

local uuid = require('uuid')

local crud_utils = require('crud.common.utils')
local select_conditions = require('crud.select.conditions')
local cond_funcs = select_conditions.funcs
local select_filters = require('crud.select.filters')
local collations = require('crud.common.collations')
local select_plan = require('crud.select.plan')

local t = require('luatest')
local g = t.group('select_filters_uuid')

local helpers = require('test.helper')

g.before_all = function()
    if crud_utils.tarantool_supports_uuids() then
        helpers.box_cfg()

        local customers_space = box.schema.space.create('customers', {
            format = {
                {'uuid', 'uuid'},
                {'bucket_id', 'unsigned'},
                {'name', 'string'},
                {'category_id', 'uuid'},
            },
            if_not_exists = true,
        })
        customers_space:create_index('uuid', { -- id: 0
            parts = {'uuid'},
            if_not_exists = true,
        })
        customers_space:create_index('category_id', { -- id: 1
            parts = {
                { field = 'category_id', is_nullable = true },
            },
            if_not_exists = true,
        })
    end
end

g.after_all(function()
    box.space.customers:drop()
end)

g.test_parse = function()
    t.skip_if(not crud_utils.tarantool_supports_uuids(), "UUIDs are not supported on Tarantool <= 2.4.1")

    -- select by indexed field with conditions by index and field
    local uuid1 = uuid.fromstr('b85f6440-24d7-11eb-8d27-000000000000')
    local uuid2 = uuid.fromstr('cea48870-24d7-11eb-8d27-111111111111')
    local uuid3 = uuid.new()

    local conditions = {
        cond_funcs.gt('uuid', uuid1),
        cond_funcs.lt('uuid', uuid2),
        cond_funcs.eq('name', 'Charlie'),
        cond_funcs.eq('category_id', uuid3)
    }

    local plan, err = select_plan.new(box.space.customers, conditions)
    t.assert_equals(err, nil)

    local space = box.space.customers

    local filter_conditions, err = select_filters.internal.parse(space, conditions, {
        scan_condition_num = plan.scan_condition_num,
        iter = plan.iter,
    })
    t.assert_equals(err, nil)

    -- uuid filter (early exit is possible)
    local uuid_filter_condition = filter_conditions[1]
    t.assert_type(uuid_filter_condition, 'table')
    t.assert_equals(uuid_filter_condition.fieldnos, {1})
    t.assert_equals(uuid_filter_condition.operator, select_conditions.operators.LT)
    t.assert_equals(uuid_filter_condition.values, {uuid2})
    t.assert_equals(uuid_filter_condition.types, {'uuid'})
    t.assert_equals(uuid_filter_condition.early_exit_is_possible, true)

    -- name filter
    local name_filter_condition = filter_conditions[2]
    t.assert_type(name_filter_condition, 'table')
    t.assert_equals(name_filter_condition.fieldnos, {3})
    t.assert_equals(name_filter_condition.operator, select_conditions.operators.EQ)
    t.assert_equals(name_filter_condition.values, {'Charlie'})
    t.assert_equals(name_filter_condition.types, {'string'})
    t.assert_equals(name_filter_condition.early_exit_is_possible, false)

    -- has_a_car filter
    local category_id_filter_condition = filter_conditions[3]
    t.assert_type(category_id_filter_condition, 'table')
    t.assert_equals(category_id_filter_condition.fieldnos, {4})
    t.assert_equals(category_id_filter_condition.operator, select_conditions.operators.EQ)
    t.assert_equals(category_id_filter_condition.values, {uuid3})
    t.assert_equals(category_id_filter_condition.types, {'uuid'})
    t.assert_equals(category_id_filter_condition.early_exit_is_possible, false)

    t.assert_equals(#category_id_filter_condition.values_opts, 1)
    local category_id_opts = category_id_filter_condition.values_opts[1]
    t.assert_equals(category_id_opts.is_nullable, true)
    t.assert_equals(category_id_opts.collation, collations.NONE)
end

g.test_one_condition_uuid = function()
    t.skip_if(not crud_utils.tarantool_supports_uuids(), "UUIDs are not supported on Tarantool <= 2.4.1")

    local uuid1 = uuid.fromstr('b85f6440-24d7-11eb-8d27-000000000000')
    local uuid2 = uuid.fromstr('cea48870-24d7-11eb-8d27-111111111111')

    local filter_conditions = {
        {
            fieldnos = {1},
            operator = select_conditions.operators.EQ,
            values = {uuid1},
            types = {'uuid'},
            early_exit_is_possible = true,
        }
    }

    local expected_code = [[local tuple = ...

local field_1 = tuple[1]

if not eq_1(field_1) then return false, true end

return true, false]]

    local expected_library_code = [[local M = {}

function M.eq_1(field_1)
    return (eq_uuid(field_1, "b85f6440-24d7-11eb-8d27-000000000000"))
end

return M]]

    local filter_code = select_filters.internal.gen_filter_code(filter_conditions)
    t.assert_equals(filter_code.code, expected_code)
    t.assert_equals(filter_code.library, expected_library_code)

    local filter_func = select_filters.internal.compile(filter_code)
    t.assert_equals({ filter_func({uuid1, uuid1:str(), 1}) }, {true, false})
    t.assert_equals({ filter_func({uuid2, uuid1:str(), 1}) }, {false, true})
    t.assert_equals({ filter_func({nil, uuid1:str(), 1}) }, {false, true})
end

g.test_one_condition_uuid_gt = function()
    t.skip_if(not crud_utils.tarantool_supports_uuids(), "UUIDs are not supported on Tarantool <= 2.4.1")

    local uuid1 = uuid.fromstr('b85f6440-24d7-11eb-8d27-000000000000')
    local uuid2 = uuid.fromstr('cea48870-24d7-11eb-8d27-111111111111')

    local filter_conditions = {
        {
            fieldnos = {1},
            operator = select_conditions.operators.GT,
            values = {uuid1},
            types = {'uuid'},
            early_exit_is_possible = true,
        }
    }

    local expected_code = [[local tuple = ...

local field_1 = tuple[1]

if not cmp_1(field_1) then return false, true end

return true, false]]

    local expected_library_code = [[local M = {}

function M.cmp_1(field_1)
    if lt_uuid(field_1, "b85f6440-24d7-11eb-8d27-000000000000") then return false end
    if not eq_uuid(field_1, "b85f6440-24d7-11eb-8d27-000000000000") then return true end

    return false
end

return M]]

    local filter_code = select_filters.internal.gen_filter_code(filter_conditions)
    t.assert_equals(filter_code.code, expected_code)
    t.assert_equals(filter_code.library, expected_library_code)

    local filter_func = select_filters.internal.compile(filter_code)
    t.assert_equals({ filter_func({uuid2, uuid1:str(), 1}) }, {true, false})
    t.assert_equals({ filter_func({uuid1, uuid2:str(), 1}) }, {false, true})
    t.assert_equals({ filter_func({nil, uuid1:str(), 1}) }, {false, true})
end

g.test_one_condition_uuid_with_nil_value = function()
    t.skip_if(not crud_utils.tarantool_supports_uuids(), "UUIDs are not supported on Tarantool <= 2.4.1")

    local uuid1 = uuid.fromstr('b85f6440-24d7-11eb-8d27-000000000000')
    local uuid2 = uuid.fromstr('cea48870-24d7-11eb-8d27-111111111111')

    local filter_conditions = {
        {
            fieldnos = {1, 3},
            operator = select_conditions.operators.GE,
            values = {uuid1},
            types = {'uuid', 'string'},
            early_exit_is_possible = false,
            values_opts = {
                {is_nullable = false},
                {is_nullable = true},
            }
        },
    }

    local expected_code = [[local tuple = ...

local field_1 = tuple[1]

if not cmp_1(field_1) then return false, false end

return true, false]]

    local expected_library_code = [[local M = {}

function M.cmp_1(field_1)
    if lt_uuid_strict(field_1, "b85f6440-24d7-11eb-8d27-000000000000") then return false end
    if not eq_uuid(field_1, "b85f6440-24d7-11eb-8d27-000000000000") then return true end

    return true
end

return M]]

    local filter_code = select_filters.internal.gen_filter_code(filter_conditions)
    t.assert_equals(filter_code.code, expected_code)
    t.assert_equals(filter_code.library, expected_library_code)

    local filter_func = select_filters.internal.compile(filter_code)
    t.assert_equals(filter_func({uuid1, 'test', 3}), true)
    t.assert_equals(filter_func({uuid2, 'xxx', 1}), true)
end

g.test_two_conditions_uuid = function()
    t.skip_if(not crud_utils.tarantool_supports_uuids(), "UUIDs are not supported on Tarantool <= 2.4.1")

    local uuid1 = uuid.fromstr('b85f6440-24d7-11eb-8d27-000000000000')
    local uuid2 = uuid.fromstr('cea48870-24d7-11eb-8d27-111111111111')

    local filter_conditions = {
        {
            fieldnos = {2},
            operator = select_conditions.operators.EQ,
            values = {'Charlie'},
            types = {'string'},
            early_exit_is_possible = true,
        },
        {
            fieldnos = {3},
            operator = select_conditions.operators.GE,
            values = {uuid2:str()},
            types = {'uuid'},
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
    if lt_uuid(field_3, "cea48870-24d7-11eb-8d27-111111111111") then return false end
    if not eq_uuid(field_3, "cea48870-24d7-11eb-8d27-111111111111") then return true end

    return true
end

return M]]

    local filter_code = select_filters.internal.gen_filter_code(filter_conditions)
    t.assert_equals(filter_code.code, expected_code)
    t.assert_equals(filter_code.library, expected_library_code)

    local filter_func = select_filters.internal.compile(filter_code)
    t.assert_equals({ filter_func({4, 'xxx', uuid1}) }, {false, true})
    t.assert_equals({ filter_func({5, 'Charlie', uuid1}) }, {false, false})
    t.assert_equals({ filter_func({5, 'xxx', uuid2}) }, {false, true})
    t.assert_equals({ filter_func({6, 'Charlie', uuid2}) }, {true, false})
    t.assert_equals({ filter_func({6, 'Charlie', nil}) }, {false, false})
end

-- luacheck: pop
