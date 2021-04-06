local t = require('luatest')
local g = t.group('cut_rows')

local cut_rows = require('crud.cut_rows')

g.test_cut_rows = function()
    local rows = {
        {3, 'Pavel', 27},
        {6, 'Alexey', 31},
        {4, 'Mikhail', 51},
    }

    local expected_rows = {
        {3, 'Pavel'},
        {6, 'Alexey'},
        {4, 'Mikhail'},
    }

    local metadata = {
        {name = 'id', type = 'unsigned'},
        {name = 'name', type = 'string'},
        {name = 'age', type = 'number'},
    }

    local expected_metadata = {
        {name = 'id', type = 'unsigned'},
        {name = 'name', type = 'string'},
    }

    local res = {
        metadata = metadata,
        rows = rows,
    }

    local fields = {'id', 'name'}

    local result, err = cut_rows.call(res, fields)

    t.assert_equals(err, nil)
    t.assert_equals(result.metadata, expected_metadata)
    t.assert_equals(result.rows, expected_rows)
end

g.test_cut_rows_errors = function()
    local rows = {
        {3, 'Pavel', 27},
        {6, 'Alexey', 31},
        {4, 'Mikhail', 51},
    }

    local metadata = {
        {name = 'id', type = 'unsigned'},
        {name = 'name', type = 'string'},
        {name = 'age', type = 'number'},
    }

    local res = {
        metadata = metadata,
        rows = rows,
    }

    local fields = {'id', 'name', 'age', 'age'}

    local result, err = cut_rows.call(res, fields)

    t.assert_equals(result, nil)
    t.assert_str_contains(err.err, 'Field names don\'t match to tuple metadata')

    local fields = {'id', 'lastname'}

    local result, err = cut_rows.call(res, fields)

    t.assert_equals(result, nil)
    t.assert_str_contains(err.err, 'Field names don\'t match to tuple metadata')
end
