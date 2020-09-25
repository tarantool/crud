local utils = require('crud.common.utils')

local t = require('luatest')
local g = t.group('serialization')

local helpers = require('test.helper')

g.before_all = function()
    helpers.box_cfg()

    local customers_space = box.schema.space.create('customers', {
        format = {
            {name = 'id', type = 'unsigned'},
            {name = 'bucket_id', type = 'unsigned'},
            {name = 'name', type = 'string'},
            {name = 'age', type = 'number', is_nullable = true},
        },
        if_not_exists = true,
    })
    customers_space:create_index('id', {
        parts = {'id'},
        if_not_exists = true,
    })
end

g.after_all(function()
    box.space.customers:drop()
end)

g.test_flatten = function()
    local space_format = box.space.customers:format()

    -- ok
    local object = {
        id = 1,
        bucket_id = 1024,
        name = 'Marilyn',
        age = 50,
    }

    local tuple, err = utils.flatten(object, space_format)
    t.assert(err == nil)
    t.assert_equals(tuple, {1, 1024, 'Marilyn', 50})


    -- set bucket_id
    local object = {
        id = 1,
        name = 'Marilyn',
        age = 50,
    }

    local tuple, err = utils.flatten(object, space_format, 1025)
    t.assert(err == nil)
    t.assert_equals(tuple, {1, 1025, 'Marilyn', 50})

    -- non-nullable field name is nil
    local object = {
        id = 1,
        bucket_id = 1024,
        name = nil,
        age = 50,
    }

    local tuple, err = utils.flatten(object, space_format)
    t.assert(tuple == nil)
    t.assert(err ~= nil)
    t.assert_str_contains(err.err, 'Field "name" isn\'t nullable')

    -- system field bucket_id is nil
    local object = {
        id = 1,
        bucket_id = nil,
        name = 'Marilyn',
        age = 50,
    }

    local tuple, err = utils.flatten(object, space_format)
    t.assert(err == nil)
    t.assert_equals(tuple, {1, nil, 'Marilyn', 50})

    -- nullable field is nil
    local object = {
        id = 1,
        bucket_id = 1024,
        name = 'Marilyn',
        age = nil,
    }

    local tuple, err = utils.flatten(object, space_format)
    t.assert(err == nil)
    t.assert_equals(tuple, {1, 1024, 'Marilyn', nil})
end

g.test_unflatten = function()
    local space_format = box.space.customers:format()

    -- ok
    local tuple = {1, 1024, 'Marilyn', 50}
    local object, err = utils.unflatten(tuple, space_format)
    t.assert(err == nil)
    t.assert_equals(object, {
        id = 1,
        bucket_id = 1024,
        name = 'Marilyn',
        age = 50,
    })

    -- non-nullable field id nil
    local tuple = {1, nil, 'Marilyn', 50}
    local object, err = utils.unflatten(tuple, space_format)
    t.assert(object == nil)
    t.assert(err ~= nil)
    t.assert_str_contains(err.err, "Field 2 isn't nullable")

    -- nullable field is nil
    local tuple = {1, 1024, 'Marilyn', nil}
    local object, err = utils.unflatten(tuple, space_format)
    t.assert(err == nil)
    t.assert_equals(object, {
        id = 1,
        bucket_id = 1024,
        name = 'Marilyn',
        age = nil,
    })

    -- extra field
    local tuple = {1, 1024, 'Marilyn', 50, 'one-bad-value'}
    local object, err = utils.unflatten(tuple, space_format)
    t.assert(err == nil)
    t.assert_equals(object, {
        id = 1,
        bucket_id = 1024,
        name = 'Marilyn',
        age = 50,
    })
end

g.test_extract_key = function()
    local tuple = {1, nil, 'Marilyn', 50}

    local key = utils.extract_key(tuple, {{fieldno = 1}})
    t.assert_equals(key, {1})

    local key = utils.extract_key(tuple, {
        {fieldno = 3}, {fieldno = 2}, {fieldno = 1},
    })
    t.assert_equals(key, {'Marilyn', nil, 1})
end
