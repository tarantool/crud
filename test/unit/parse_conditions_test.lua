local t = require('luatest')
local g = t.group('parse_conditions')

local compare_conditions = require('crud.compare.conditions')
local cond_funcs = compare_conditions.funcs

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

g.test_jsonpath_parse = function()
    t.skip_if(not crud_utils.tarantool_supports_jsonpath_filters(), "Jsonpath is not supported on Tarantool < 1.10")

    local user_conditions = {
        {'==', '[\'name\']', 'Alexey'},
        {'=', '["name"].a.b', 'Sergey'},
        {'<', '["year"]["field_1"][\'field_2\']', 2021},
        {'<=', '[2].a', {1, 2, 3}},
        {'>', '[2]', 'Jackson'},
        {'>=', '[\'year\'].a["f2"][\'f3\']', 2017},
    }

    local space_format = {
        {name = 'name', type = 'string'},
        {name = 'surname', type = 'any'},
        {name = 'year', type ='unsigned'},
    }

    local conditions, err = select_conditions.parse(user_conditions, space_format)
    t.assert(err == nil)
    t.assert_equals(conditions, {
        cond_funcs.eq('name', 'Alexey'),
        cond_funcs.eq('name', 'Sergey', '[1].a.b'),
        cond_funcs.lt('year', 2021, '[3]["field_1"][\'field_2\']'),
        cond_funcs.le('surname', {1, 2, 3}, '[2].a'),
        cond_funcs.gt('surname', 'Jackson'),
        cond_funcs.ge('year', 2017, '[3].a["f2"][\'f3\']'),
    })
end

g.test_jsonpath_parse_errors = function()
    t.skip_if(not crud_utils.tarantool_supports_jsonpaths(), "Jsonpath is not supported on Tarantool < 2")
    local space_format = {
        {name = 'name', type = 'string'},
        {name = 'surname', type = 'any'},
        {name = 'year', type ='unsigned'},
    }

    -- bad jsonpath
    local user_conditions = {
        {'==', '1].a', 'Alexey'},
    }

    local _, err = select_conditions.parse(user_conditions, space_format)
    t.assert_str_contains(
        err.err,
        'Invalid jsonpath format'
    )

    -- non-existen fieldno
    local user_conditions = {
         {'==', '[4].a.b', 88},
    }

    local _, err = select_conditions.parse(user_conditions, space_format)
    t.assert_str_contains(
        err.err,
        'Space doesn\'t contains field [4]'
    )

    -- non-existen field
    local user_conditions = {
        {'==', '[\'bucket_id\']', 41},
    }

    local _, err = select_conditions.parse(user_conditions, space_format)
    t.assert_str_contains(
        err.err,
        'Space doesn\'t contains field [\'bucket_id\']'
    )
end
