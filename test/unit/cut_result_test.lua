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
end

g.test_cut_objects = function()
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

    local fields = {'id', 'name'}

    local result = utils.cut_objects(objs, fields)

    t.assert_equals(result, expected_objs)

    -- with nullable field
    local objs = {
        {id = 3, name = box.NULL, lastname = 'Smith', age = 27},
        {id = 6, name = 'Alexey', lastname = 'Black', age = 31},
        {id = 4, name = 'Mikhail', lastname = 'Smith', age = 51},
    }

    fields = {'id', 'name', 'lastname'}

    local expected_objs = {
        {id = 3, name = box.NULL, lastname = 'Smith'},
        {id = 6, name = 'Alexey', lastname = 'Black'},
        {id = 4, name = 'Mikhail', lastname = 'Smith'},
    }

    result = utils.cut_objects(objs, fields)

    t.assert_equals(result, expected_objs)

    fields = {'id', 'surname', 'name'}

    objs = {
        {id = 3, name = 'Pavel', lastname = 'Smith', age = 27},
        {id = 6, name = 'Alexey', lastname = 'Black', age = 31},
        {id = 4, name = 'Mikhail', lastname = 'Smith', age = 51},
    }

    expected_objs = {
        {id = 3, name = 'Pavel'},
        {id = 6, name = 'Alexey'},
        {id = 4, name = 'Mikhail'},
    }

    result = utils.cut_objects(objs, fields)

    t.assert_equals(result, expected_objs)
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

    fields = {'id', 'lastname'}

    result, err = utils.cut_rows(rows, metadata, fields)

    t.assert_equals(result, nil)
    t.assert_str_contains(err.err, 'Field names don\'t match to tuple metadata')
end
