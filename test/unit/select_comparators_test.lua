local uuid = require('uuid')

local select_comparators = require('crud.select.comparators')
local select_conditions = require('crud.select.conditions')
local operators = select_conditions.operators

local t = require('luatest')
local g = t.group('select_comparators')

g.test_eq_no_collations = function()
    local func_eq, err = select_comparators.gen_func(operators.EQ, {
        {}, {}, {},
    })
    t.assert(err == nil)

    -- check that index length is used
    t.assert(func_eq({1, 2, 3}, {1, 2, 3, 4}))
    t.assert(func_eq({1, 2, 3, 4}, {1, 2, 3}))

    t.assert(not func_eq({nil}, {1})) -- tarantool index iteration logic

    -- nil equals to nil
    t.assert(func_eq({}, {}))
    t.assert(func_eq({1}, {1}))
    t.assert(not func_eq({0}, {1}))
    t.assert(func_eq({1, 2}, {1, 2}))
    t.assert(func_eq({1, 2, 3}, {1, 2, 3}))
end

g.test_lt_no_collations = function()
    local func_lt, err = select_comparators.gen_func(operators.LT, {
        {}, {}, {},
    })
    t.assert(err == nil)

    t.assert(not func_lt({1}, {1}))
    t.assert(func_lt({1}, {1, 2}))
    t.assert(func_lt({0}, {1}))
    t.assert(not func_lt({1, 2}, {1, 2}))
    t.assert(func_lt({1, 2}, {1, 3}))
    t.assert(not func_lt({1, 2, 3}, {1, 2, 3}))
    t.assert(func_lt({1, 2, 3}, {1, 2, 4}))
    t.assert(func_lt({1, 2, 3}, {4, 1, 2}))
end

g.test_le_no_collations = function()
    local func_le, err = select_comparators.gen_func(operators.LE, {
        {}, {}, {},
    })
    t.assert(err == nil)

    t.assert(func_le({1}, {1}))
    t.assert(func_le({1}, {1, 2}))
    t.assert(func_le({0}, {1}))
    t.assert(not func_le({2}, {1}))
    t.assert(func_le({1, 2}, {1, 2}))
    t.assert(func_le({1, 2}, {1, 3}))
    t.assert(not func_le({2, 2}, {1, 3}))
    t.assert(func_le({1, 2, 3}, {1, 2, 3}))
    t.assert(func_le({1, 2, 3}, {1, 2, 4}))
    t.assert(func_le({1, 2, 3}, {4, 1, 2}))
    t.assert(not func_le({5, 2, 3}, {4, 1, 2}))
end

g.test_gt_no_collations = function()
    local func_gt, err = select_comparators.gen_func(operators.GT, {
        {}, {}, {},
    })
    t.assert(err == nil)

    t.assert(not func_gt({1}, {1}))
    t.assert(func_gt({1, 2}, {1}))
    t.assert(func_gt({1}, {0}))
    t.assert(not func_gt({0, 2}, {1, 2}))
    t.assert(not func_gt({1, 2}, {1, 2}))
    t.assert(func_gt({3, 1}, {2, 1}))
    t.assert(func_gt({2, 3}, {2, 2}))
    t.assert(not func_gt({1, 2, 3}, {1, 2, 3}))
    t.assert(func_gt({2, 2, 3}, {1, 2, 4}))
    t.assert(func_gt({1, 5, 3}, {1, 4, 3}))
    t.assert(func_gt({1, 2, 4}, {1, 2, 3}))
end

g.test_ge_no_collations = function()
    local func_ge, err = select_comparators.gen_func(operators.GE, {
        {}, {}, {},
    })
    t.assert(err == nil)

    t.assert(func_ge({1}, {1}))
    t.assert(func_ge({1, 2}, {1}))
    t.assert(func_ge({1}, {0}))
    t.assert(not func_ge({0, 2}, {1, 2}))
    t.assert(func_ge({1, 2}, {1, 2}))
    t.assert(func_ge({3, 1}, {2, 1}))
    t.assert(func_ge({2, 3}, {2, 2}))
    t.assert(func_ge({1, 2, 3}, {1, 2, 3}))
    t.assert(func_ge({2, 2, 3}, {1, 2, 4}))
    t.assert(func_ge({1, 5, 3}, {1, 4, 3}))
    t.assert(func_ge({1, 2, 4}, {1, 2, 3}))
end

g.test_unicode_collations = function()
    local unicode_parts = {
        { collation = 'unicode', type = 'string' },
        { collation = 'unicode', type = 'string' },
        { collation = 'unicode', type = 'string' },
    }
    local unicode_ci_parts = {
        { collation = 'unicode_ci', type = 'string' },
        { collation = 'unicode_ci', type = 'string' },
        { collation = 'unicode_ci', type = 'string' },
    }

    local func_eq_unicode, err = select_comparators.gen_func(operators.GE, unicode_parts)
    t.assert(err == nil)
    local func_eq_unicode_ci, err = select_comparators.gen_func(operators.GE, unicode_ci_parts)
    t.assert(err == nil)

    t.assert(func_eq_unicode({'a'}, {'a'}))
    t.assert(func_eq_unicode_ci({'a'}, {'a'}))

    t.assert(not func_eq_unicode({'a', 'A', 'a'}, {'A', 'Á', 'Ä'}))
    t.assert(func_eq_unicode_ci({'a', 'A', 'a'}, {'A', 'Á', 'Ä'}))

    local func_lt_unicode, err = select_comparators.gen_func(operators.LT, unicode_parts)
    t.assert(err == nil)
    t.assert(func_lt_unicode({'a', 'A', 'a'}, {'A', 'Á', 'Ä'}, 3))

    local func_gt_unicode, err = select_comparators.gen_func(operators.GT, unicode_parts)
    t.assert(err == nil)
    t.assert(not func_gt_unicode({'a', 'A', 'a'}, {'A', 'Á', 'Ä'}, 3))

    local func_ge_unicode, err = select_comparators.gen_func(operators.GE, unicode_parts)
    t.assert(err == nil)
    local func_ge_unicode_ci, err = select_comparators.gen_func(operators.GE, unicode_ci_parts)
    t.assert(err == nil)
    t.assert(not func_ge_unicode({'a', 'A', 'a'}, {'A', 'Á', 'Ä'}, 3))
    t.assert(func_ge_unicode_ci({'a', 'A', 'a'}, {'A', 'Á', 'Ä'}, 3))

    local func_le_unicode, err = select_comparators.gen_func(operators.LE, unicode_parts)
    t.assert(err == nil)
    local func_le_unicode_ci, err = select_comparators.gen_func(operators.LE, unicode_ci_parts)
    t.assert(err == nil)
    t.assert(not func_le_unicode({'A', 'Á', 'Ä'}, {'a', 'A', 'a'}, 3))
    t.assert(func_le_unicode_ci({'A', 'Á', 'Ä'}, {'a', 'A', 'a'}, 3))
end

g.test_uuid_type_parts = function()
    local key_parts = {
        {type = 'uuid'}, {}
    }

    local func_eq, err = select_comparators.gen_func(operators.EQ, key_parts)
    t.assert(err == nil)
    local u1 = uuid.fromstr('8a1ffbc0-241e-11eb-8d27-0800200c9a66')
    local u2 = uuid.fromstr('8f1ffbc0-241e-11eb-8d27-0800200c9a66')
    local u1_copy = uuid.fromstr(u1:str())
    t.assert(func_eq({u1, 'str'}, {u1, 'str'}))
    t.assert(func_eq({u1, 'str'}, {u1_copy, 'str'}))
    t.assert(not func_eq({u1, 'str'}, {u2, 'str'}))

    local func_ge, err = select_comparators.gen_func(operators.GE, key_parts)
    t.assert(err == nil)
    t.assert(not func_ge({u1, 'str'}, {u2, 'str'}))
    t.assert(func_ge({u1, 'str'}, {u1, 'str'}))
    t.assert(func_ge({u1, 'ttr'}, {u1, 'str'}))
    t.assert(func_ge({u2, 'str'}, {u1, 'str'}))
end
