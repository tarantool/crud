-- luacheck: push max_line_length 300

local t = require('luatest')
local g = t.group('select_filters')

local select_conditions = require('crud.select.conditions')
local select_filters = require('crud.select.filters')

g.test_empty_conditions = function()
    local filter_conditions = {}

    local expected_code = 'return true, false'
    local expected_library_code = 'return {}'

    local filter = select_filters.gen_code(filter_conditions)
    t.assert_equals(filter.code, expected_code)
    t.assert_equals(filter.library_code, expected_library_code)
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

    local filter = select_filters.gen_code(filter_conditions)
    t.assert_equals(filter.code, expected_code)
    t.assert_equals(filter.library_code, expected_library_code)

    local func = select_filters.compile(filter)
    t.assert_equals({ func({3, 2, 1}) }, {true, false})
    t.assert_equals({ func({2, 2, 1}) }, {false, true})
    t.assert_equals({ func({nil, 2, 1}) }, {false, true})
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

    local filter = select_filters.gen_code(filter_conditions)
    t.assert_equals(filter.code, expected_code)
    t.assert_equals(filter.library_code, expected_library_code)

    local func = select_filters.compile(filter)
    t.assert_equals({ func({true, 2, 1}) }, {true, false})
    t.assert_equals({ func({false, 2, 1}) }, {false, true})
    t.assert_equals({ func({nil, 2, 1}) }, {false, true})
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

    local filter = select_filters.gen_code(filter_conditions)
    t.assert_equals(filter.code, expected_code)
    t.assert_equals(filter.library_code, expected_library_code)

    local func = select_filters.compile(filter)
    t.assert_equals({ func({3, 'ddddeeee', 1}) }, {true, false})
    t.assert_equals({ func({3, 'dddddddd', 1}) }, {false, true})
    t.assert_equals({ func({3, 'aaaaaaaa', 1}) }, {false, true})
    t.assert_equals({ func({3, nil, 1}) }, {false, true})
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

    local filter = select_filters.gen_code(filter_conditions)
    t.assert_equals(filter.code, expected_code)
    t.assert_equals(filter.library_code, expected_library_code)

    local func = select_filters.compile(filter)
    t.assert_equals({ func({4, 'xxx', 'dddddddd'}) }, {true, false})
    t.assert_equals({ func({5, 'xxx', 'dddddddd'}) }, {false, true})
    t.assert_equals({ func({4, 'xxx', 'dddddeee'}) }, {true, false})
    t.assert_equals({ func({4, 'xxx', 'aaaaaaaa'}) }, {false, false})
    t.assert_equals({ func({nil, 'xxx', 'dddddeee'}) }, {false, true})
    t.assert_equals({ func({4, 'xxx', nil}) }, {false, false})
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

    local filter = select_filters.gen_code(filter_conditions)
    t.assert_equals(filter.code, expected_code)
    t.assert_equals(filter.library_code, expected_library_code)

    local func = select_filters.compile(filter)
    t.assert_equals({ func({2, 'test', 5}) }, {true, false})
    t.assert_equals({ func({4, 'test', 5}) }, {false, true})
    t.assert_equals({ func({4, 'test', nil}) }, {false, false})
    t.assert_equals({ func({5, 'test', nil}) }, {false, false})
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

    local filter = select_filters.gen_code(filter_conditions)
    t.assert_equals(filter.code, expected_code)
    t.assert_equals(filter.library_code, expected_library_code)

    local func = select_filters.compile(filter)
    t.assert_equals(func({1, 'test', 3}), true)
    t.assert_equals(func({1, 'xxx', 1}), true)
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

    local filter = select_filters.gen_code(filter_conditions)
    t.assert_equals(filter.code, expected_code)
    t.assert_equals(filter.library_code, expected_library_code)

    local func = select_filters.compile(filter)
    t.assert_equals(func({'A', 'Á', 'Ä', 6}), true)
    t.assert_equals(func({'A', 'á', 'ä', 6}), true)
    t.assert_equals(func({'a', 'Á', 'Ä', 6}), false)
    t.assert_equals(func({'A', 'V', 'ä', 6}), false)
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

    local filter = select_filters.gen_code(filter_conditions)
    t.assert_equals(filter.code, expected_code)
    t.assert_equals(filter.library_code, expected_library_code)

    local func = select_filters.compile(filter)
    t.assert_equals(func({'a', box.NULL}), true)
    t.assert_equals(func({'a', 'a'}), false) -- 'a' ~= box.NULL
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

    local filter = select_filters.gen_code(filter_conditions)
    t.assert_equals(filter.code, expected_code)
    t.assert_equals(filter.library_code, expected_library_code)

    local func = select_filters.compile(filter)
    t.assert_equals(func({'a', 'a'}), true)       -- 'a' > box.NULL
    t.assert_equals(func({'a', box.NULL}), false) -- box.NULL > box.NULL is false
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

    local filter = select_filters.gen_code(filter_conditions)
    t.assert_equals(filter.code, expected_code)
    t.assert_equals(filter.library_code, expected_library_code)

    local func = select_filters.compile(filter)
    t.assert_equals(func({'a', 'a'}), true)       -- 'a' > box.NULL
    t.assert_equals(func({'a', box.NULL}), false) -- box.NULL > box.NULL is false
end

-- luacheck: pop
