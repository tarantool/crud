local t = require('luatest')
local g = t.group('cut_rows')

local utils = require('crud.common.utils')

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

    local fields = {'id', 'name'}

    local result, err = utils.cut_rows(rows, metadata, fields)

    t.assert_equals(err, nil)
    t.assert_equals(result.metadata, expected_metadata)
    t.assert_equals(result.rows, expected_rows)

    -- using box.tuple
    rows = {
        box.tuple.new({3, 'Pavel', 27}),
        box.tuple.new({6, 'Alexey', 31}),
        box.tuple.new({4, 'Mikhail', 51}),
    }

    metadata = {
        {name = 'id', type = 'unsigned'},
        {name = 'name', type = 'string'},
        {name = 'age', type = 'number'},
    }

    result, err = utils.cut_rows(rows, metadata, fields)

    t.assert_equals(err, nil)
    t.assert_equals(result.metadata, expected_metadata)
    t.assert_equals(result.rows, expected_rows)

    -- without metadata

    local rows = {
        {3, 'Pavel', 27},
        {6, 'Alexey', 31},
        {4, 'Mikhail', 51},
    }

    result, err = utils.cut_rows(rows, nil, fields)

    t.assert_equals(err, nil)
    t.assert_equals(result.metadata, nil)
    t.assert_equals(result.rows, expected_rows)

    -- with mapped data
    local objs = {
        {id = 3, name = 'Pavel', age = 27},
        {id = 6, name = 'Alexey', age = 31},
        {id = 4, name = 'Mikhail', age = 51},
    }

    local expected_objs = {
        {id = 3, name = 'Pavel'},
        {id = 6, name = 'Alexey'},
        {id = 4, name = 'Mikhail'},
    }

    result, err = utils.cut_rows(objs, nil, fields, {mapped = true})

    t.assert_equals(err, nil)
    t.assert_equals(result.metadata, nil)
    t.assert_equals(result.rows, expected_objs)

    -- with mapped flag as box.NULL
    rows = {
        {3, 'Nastya', 27},
        {6, 'Alexey', 31},
        {4, 'Mikhail', 51},
    }

    expected_rows = {
        {3, 'Nastya'},
        {6, 'Alexey'},
        {4, 'Mikhail'},
    }

    metadata = {
        {name = 'id', type = 'unsigned'},
        {name = 'name', type = 'string'},
        {name = 'age', type = 'number'},
    }

    expected_metadata = {
        {name = 'id', type = 'unsigned'},
        {name = 'name', type = 'string'},
    }

    result, err = utils.cut_rows(rows, metadata, fields, {mapped = box.NULL})

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

    local fields = {'name', 'id', 'age'}

    local result, err = utils.cut_rows(rows, metadata, fields)

    t.assert_equals(result, nil)
    t.assert_str_contains(err.err, 'Field names don\'t match to tuple metadata')

    local fields = {'id', 'lastname'}

    local result, err = utils.cut_rows(rows, metadata, fields)

    t.assert_equals(result, nil)
    t.assert_str_contains(err.err, 'Field names don\'t match to tuple metadata')
end
