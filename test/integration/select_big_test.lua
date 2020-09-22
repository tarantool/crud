local fio = require('fio')

local t = require('luatest')
local g = t.group('select_big')

local helpers = require('test.helper')

math.randomseed(os.time())

local function gen_test_values(count)
    count = count or 10000

    local test_values = {}
    for i = 1, count do
        local value_obj = {
            key = math.random(1000 * i, 1000 * (i + 1)),
            value = math.random(1000 * i, 1000 * (i + 1)),
        }
        table.insert(test_values, value_obj)
    end

    return test_values
end

g.before_all = function()
    g.cluster = helpers.Cluster:new({
        datadir = fio.tempdir(),
        server_command = helpers.entrypoint('srv_select_big'),
        use_vshard = true,
        replicasets = {
            {
                uuid = helpers.uuid('a'),
                alias = 'router',
                roles = { 'vshard-router' },
                servers = {
                    { instance_uuid = helpers.uuid('a', 1), alias = 'router' },
                },
            },
            {
                uuid = helpers.uuid('b'),
                alias = 's-1',
                roles = { 'values-storage' },
                servers = {
                    { instance_uuid = helpers.uuid('b', 1), alias = 's1-master' },
                    { instance_uuid = helpers.uuid('b', 2), alias = 's1-replica' },
                },
            },
            {
                uuid = helpers.uuid('c'),
                alias = 's-2',
                roles = { 'values-storage' },
                servers = {
                    { instance_uuid = helpers.uuid('c', 1), alias = 's2-master' },
                    { instance_uuid = helpers.uuid('c', 2), alias = 's2-replica' },
                },
            }
        },
    })
    g.cluster:start()
end

g.after_all = function()
    g.cluster:stop()
    fio.rmtree(g.cluster.datadir)
end

g.before_each(function()
    for _, server in ipairs(g.cluster.servers) do
        server.net_box:eval([[
            local space = box.space.values
            if space ~= nil and not box.cfg.read_only then
                space:truncate()
            end
        ]])
    end
end)

local function insert_values(values)
    local inserted_objects = {}

    for _, value in ipairs(values) do
        local obj, err = g.cluster.main_server.net_box:eval([[
            local crud = require('crud')
            return crud.insert('values', ...)
        ]],{value})

        t.assert_equals(err, nil)

        table.insert(inserted_objects, obj)
    end

    return inserted_objects
end

local function get_first_n(objects, n)
    local result_objects = {}

    for i = 1, n do
        table.insert(result_objects, objects[i])
    end

    return result_objects
end

local function get_last_n(objects, n)
    local result_objects = {}

    for i = #objects - n + 1, #objects do
        table.insert(result_objects, objects[i])
    end

    return result_objects
end

g.test_select_no_conditions = function()
    local values = gen_test_values()
    values = insert_values(values)

    table.sort(values, function(obj1, obj2) return obj1.key < obj2.key end)

    -- no after
    local objects, err = g.cluster.main_server.net_box:eval([[
        local crud = require('crud')

        local objects, err = crud.select('values', nil)
        return objects, err
    ]])

    t.assert_equals(err, nil)
    t.assert_equals(#objects, #values)
    t.assert_equals(objects, values)

    -- w/ limit
    local limit = #values / 2
    local objects, err = g.cluster.main_server.net_box:eval([[
        local crud = require('crud')

        local limit = ...

        local objects, err = crud.select('values', nil, {
            limit = limit,
        })
        return objects, err
    ]], {limit})

    local expected_values = get_first_n(values, limit)

    t.assert_equals(err, nil)
    t.assert_equals(#objects, #expected_values)
    t.assert_equals(objects, expected_values)

    -- w/ batch_size
    local batch_size = 200
    local objects, err = g.cluster.main_server.net_box:eval([[
        local crud = require('crud')

        local batch_size = ...

        local objects, err = crud.select('values', nil, {
            batch_size = batch_size,
        })
        return objects, err
    ]], {batch_size})

    t.assert_equals(err, nil)
    t.assert_equals(#objects, #values)
    t.assert_equals(objects, values)

    -- w/ limit and batch_size
    local limit = #values / 2
    local batch_size = 200
    local objects, err = g.cluster.main_server.net_box:eval([[
        local crud = require('crud')

        local limit, batch_size = ...

        local objects, err = crud.select('values', nil, {
            limit = limit,
            batch_size = batch_size,
        })
        return objects, err
    ]], {limit, batch_size})

    local expected_values = get_first_n(values, limit)

    t.assert_equals(err, nil)
    t.assert_equals(#objects, #expected_values)
    t.assert_equals(objects, expected_values)
end

g.test_ge_condition_with_index = function()
    local values = gen_test_values()
    values = insert_values(values)

    table.sort(values, function(obj1, obj2) return obj1.value < obj2.value end)

    local value_to_cmp_idx = #values/2
    local value_to_cmp = values[value_to_cmp_idx].value

    local conditions = {
        {'>=', 'value', value_to_cmp},
    }

    local expected_values = get_last_n(values, value_to_cmp_idx + 1) -- equal condition

    -- no after
    local objects, err = g.cluster.main_server.net_box:eval([[
        local crud = require('crud')

        local conditions = ...

        local objects, err = crud.select('values', conditions)
        return objects, err
    ]], {conditions})

    t.assert_equals(err, nil)
    t.assert_equals(#objects, #expected_values)
    t.assert_equals(objects, expected_values)

    -- w/ limit
    local limit = #values / 2
    local objects, err = g.cluster.main_server.net_box:eval([[
        local crud = require('crud')

        local conditions, limit = ...

        local objects, err = crud.select('values', conditions, {
            limit = limit,
        })
        return objects, err
    ]], {conditions, limit})

    local expected_values_with_limit = get_first_n(expected_values, limit)

    t.assert_equals(err, nil)
    t.assert_equals(#objects, #expected_values_with_limit)
    t.assert_equals(objects, expected_values_with_limit)

    -- w/ batch_size
    local batch_size = 200
    local objects, err = g.cluster.main_server.net_box:eval([[
        local crud = require('crud')

        local conditions, batch_size = ...

        local objects, err = crud.select('values', conditions, {
            batch_size = batch_size,
        })
        return objects, err
    ]], {conditions, batch_size})

    t.assert_equals(err, nil)
    t.assert_equals(#objects, #expected_values)
    t.assert_equals(objects, expected_values)

    -- w/ limit and batch_size
    local limit = #values / 2
    local batch_size = 200
    local objects, err = g.cluster.main_server.net_box:eval([[
        local crud = require('crud')

        local conditions, limit, batch_size = ...

        local objects, err = crud.select('values', conditions, {
            batch_size = batch_size,
            limit = limit,
        })
        return objects, err
    ]], {conditions, limit, batch_size})

    t.assert_equals(err, nil)
    t.assert_equals(#objects, #expected_values_with_limit)
    t.assert_equals(objects, expected_values_with_limit)
end

g.test_le_condition_with_index = function()
    local values = gen_test_values()
    values = insert_values(values)

    table.sort(values, function(obj1, obj2) return obj1.value > obj2.value end)

    local value_to_cmp_idx = #values/2
    local value_to_cmp = values[value_to_cmp_idx].value

    local conditions = {
        {'<=', 'value', value_to_cmp},
    }

    local expected_values = get_last_n(values, value_to_cmp_idx + 1)

    -- no after
    local objects, err = g.cluster.main_server.net_box:eval([[
        local crud = require('crud')

        local conditions = ...

        local objects, err = crud.select('values', conditions)
        return objects, err
    ]], {conditions})

    t.assert_equals(err, nil)
    t.assert_equals(#objects, #expected_values)
    t.assert_equals(objects, expected_values)

    -- w/ limit
    local limit = #values / 2
    local objects, err = g.cluster.main_server.net_box:eval([[
        local crud = require('crud')

        local conditions, limit = ...

        local objects, err = crud.select('values', conditions, {
            limit = limit,
        })
        return objects, err
    ]], {conditions, limit})

    local expected_values_with_limit = get_first_n(expected_values, limit)

    t.assert_equals(err, nil)
    t.assert_equals(#objects, #expected_values_with_limit)
    t.assert_equals(objects, expected_values_with_limit)

    -- w/ batch_size
    local batch_size = 200
    local objects, err = g.cluster.main_server.net_box:eval([[
        local crud = require('crud')

        local conditions, batch_size = ...

        local objects, err = crud.select('values', conditions, {
            batch_size = batch_size,
        })
        return objects, err
    ]], {conditions, batch_size})

    t.assert_equals(err, nil)
    t.assert_equals(#objects, #expected_values)
    t.assert_equals(objects, expected_values)

    -- w/ limit and batch_size
    local limit = #values / 2
    local batch_size = 200
    local objects, err = g.cluster.main_server.net_box:eval([[
        local crud = require('crud')

        local conditions, limit, batch_size = ...

        local objects, err = crud.select('values', conditions, {
            batch_size = batch_size,
            limit = limit,
        })
        return objects, err
    ]], {conditions, limit, batch_size})

    t.assert_equals(err, nil)
    t.assert_equals(#objects, #expected_values_with_limit)
    t.assert_equals(objects, expected_values_with_limit)
end
