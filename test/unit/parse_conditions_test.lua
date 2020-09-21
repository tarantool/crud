local t = require('luatest')
local g = t.group('parse_conditions')

local select_conditions = require('crud.select.conditions')
local cond_funcs = select_conditions.funcs

g.test_parse = function()
    local user_conditions = {
        {'==', 'aaaa', nil},
        {'=', 'bbb', {12, 'aaaa'}},
        {'<', 'ccccc', 11},
        {'<=', 'dd', {1, 2, 3}},
        {'>', 'eeeeee', 666},
        {'>=', 'f', {3, 3, 4}},
    }

    local conditions, err = select_conditions.parse(user_conditions)
    t.assert(err == nil)
    t.assert_equals(conditions, {
        cond_funcs.eq('aaaa', nil),
        cond_funcs.eq('bbb', {12, 'aaaa'}),
        cond_funcs.lt('ccccc', 11),
        cond_funcs.le('dd', {1, 2, 3}),
        cond_funcs.gt('eeeeee', 666),
        cond_funcs.ge('f', {3, 3, 4}),
    })
end

g.test_parse_errors = function()
    -- conditions are no table
    local user_conditions = 'bbb = {12}'

    local _, err = select_conditions.parse(user_conditions)
    t.assert_str_contains(err.err, 'Conditions should be table, got "string"')

    -- condition is no table
    local user_conditions = {
        {'==', 'aaaa', nil},
        'bbb = {12}',
    }

    local _, err = select_conditions.parse(user_conditions)
    t.assert_str_contains(err.err, 'Each condition should be table, got "string" (condition 2)')

    -- condition len is wrong
    local user_conditions = {
        {'==', 'aaaa', nil},
        {'='},
    }

    local _, err = select_conditions.parse(user_conditions)
    t.assert_str_contains(
        err.err,
        'Each condition should be {"<operator>", "<operand>", <value>} (condition 2)'
    )

    local user_conditions = {
        {'==', 'aaaa', nil},
        {'=', 'bb', 1, 2},
    }

    local _, err = select_conditions.parse(user_conditions)
    t.assert_str_contains(
        err.err,
        'Each condition should be {"<operator>", "<operand>", <value>} (condition 2)'
    )

    -- bad operator type
    local user_conditions = {
        {'==', 'aaaa', nil},
        {3, 'bb', 1},
    }

    local _, err = select_conditions.parse(user_conditions)
    t.assert_str_contains(
        err.err,
        'condition[1] should be string, got "number" (condition 2)'
    )

    -- bad operator
    local user_conditions = {
        {'==', 'aaaa', nil},
        {'===', 'bb', 1},
    }

    local _, err = select_conditions.parse(user_conditions)
    t.assert_str_contains(
        err.err,
        'condition[1] "===" isn\'t a valid condition oprator, (condition 2)'
    )

    -- bad operand
    local user_conditions = {
        {'==', 'aaaa', nil},
        {'=', 3, 1},
    }

    local _, err = select_conditions.parse(user_conditions)
    t.assert_str_contains(
        err.err,
        'condition[2] should be string, got "number" (condition 2)'
    )
end
