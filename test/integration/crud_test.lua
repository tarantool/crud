local fio = require('fio')

local t = require('luatest')
local g = t.group('crud')

local helpers = require('test.helper')

math.randomseed(os.time())

g.before_all = function()
    g.cluster = helpers.Cluster:new({
        datadir = fio.tempdir(),
        server_command = helpers.entrypoint('srv_crud'),
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
                roles = { 'customers-storage' },
                servers = {
                    { instance_uuid = helpers.uuid('b', 1), alias = 's1-master' },
                    { instance_uuid = helpers.uuid('b', 2), alias = 's1-replica' },
                },
            },
            {
                uuid = helpers.uuid('c'),
                alias = 's-2',
                roles = { 'customers-storage' },
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
            local space = box.space.customers
            if space ~= nil and not box.cfg.read_only then
                space:truncate()
            end
        ]])
    end
end)

g.test_non_existent_space = function()
    -- insert
    local obj, err = g.cluster.main_server.net_box:eval([[
        local elect = require('elect')
        return elect.insert('non_existent_space', {id = 0, name = 'Fedor', age = 59})
    ]])

    t.assert_equals(obj, nil)
    t.assert_str_contains(err.err, 'Space "non_existent_space" doesn\'t exists')

    -- get
    local obj, err = g.cluster.main_server.net_box:eval([[
        local elect = require('elect')
        return elect.get('non_existent_space', 0)
    ]])

    t.assert_equals(obj, nil)
    t.assert_str_contains(err.err, 'Space "non_existent_space" doesn\'t exists')

    -- update
    local obj, err = g.cluster.main_server.net_box:eval([[
        local elect = require('elect')
        return elect.update('non_existent_space', 0, {{'+', 'age', 1}})
    ]])

    t.assert_equals(obj, nil)
    t.assert_str_contains(err.err, 'Space "non_existent_space" doesn\'t exists')

    -- delete
    local obj, err = g.cluster.main_server.net_box:eval([[
        local elect = require('elect')
        return elect.delete('non_existent_space', 0)
    ]])

    t.assert_equals(obj, nil)
    t.assert_str_contains(err.err, 'Space "non_existent_space" doesn\'t exists')
end

g.test_insert_get = function()
    -- insert
    local obj, err = g.cluster.main_server.net_box:eval([[
        local elect = require('elect')
        return elect.insert('customers', {id = 1, name = 'Fedor', age = 59})
    ]])

    t.assert_equals(err, nil)
    t.assert_covers(obj, {id = 1, name = 'Fedor', age = 59})
    t.assert(type(obj.bucket_id) == 'number')

    -- get
    local obj, err = g.cluster.main_server.net_box:eval([[
        local elect = require('elect')
        return elect.get('customers', 1)
    ]])

    t.assert_equals(err, nil)
    t.assert(obj ~= nil)
    t.assert_covers(obj, {id = 1, name = 'Fedor', age = 59})
    t.assert(type(obj.bucket_id) == 'number')

    -- insert again
    local obj, err = g.cluster.main_server.net_box:eval([[
        local elect = require('elect')
        return elect.insert('customers', {id = 1, name = 'Alexander', age = 37})
    ]])

    t.assert_equals(obj, nil)
    t.assert_str_contains(err.err, "Failed for %w+%-0000%-0000%-0000%-000000000000", true)
    t.assert_str_contains(err.err, "Duplicate key exists")

    -- bad format
    local obj, err = g.cluster.main_server.net_box:eval([[
        local elect = require('elect')
        return elect.insert('customers', {id = 2, name = 'Alexander'})
    ]])

    t.assert_equals(obj, nil)
    t.assert_str_contains(err.err, " Field \"age\" isn't nullable")

    -- get non existent
    local obj, err = g.cluster.main_server.net_box:eval([[
        local elect = require('elect')
        return elect.get('customers', 100)
    ]])

    t.assert_equals(err, nil)
    t.assert_equals(obj, nil)
end

g.test_update = function()
    -- insert tuple
    local obj, err = g.cluster.main_server.net_box:eval([[
        local elect = require('elect')
        return elect.insert('customers', {id = 22, name = 'Leo', age = 72})
    ]])

    t.assert_equals(err, nil)
    t.assert_covers(obj, {id = 22, name = 'Leo', age = 72})
    t.assert(type(obj.bucket_id) == 'number')

    -- update age and name fields
    local obj, err = g.cluster.main_server.net_box:eval([[
        local elect = require('elect')
        return elect.update('customers', 22, {
            {'+', 'age', 10},
            {'=', 'name', 'Leo Tolstoy'}
        })
    ]])

    t.assert_equals(err, nil)
    t.assert_covers(obj, {id = 22, name = 'Leo Tolstoy', age = 82})
    t.assert(type(obj.bucket_id) == 'number')

    -- get
    local obj, err = g.cluster.main_server.net_box:eval([[
        local elect = require('elect')
        return elect.get('customers', 22)
    ]])

    t.assert_equals(err, nil)
    t.assert_covers(obj, {id = 22, name = 'Leo Tolstoy', age = 82})
    t.assert(type(obj.bucket_id) == 'number')

    -- bad key
    local obj, err = g.cluster.main_server.net_box:eval([[
        local elect = require('elect')
        return elect.update('customers', 'bad-key', {{'+', 'age', 10},})
    ]])

    t.assert_equals(obj, nil)
    t.assert_str_contains(err.err, "Failed for %w+%-0000%-0000%-0000%-000000000000", true)
    t.assert_str_contains(err.err, "Supplied key type of part 0 does not match index part type:")
end

g.test_delete = function()
    -- insert tuple
    local obj, err = g.cluster.main_server.net_box:eval([[
        local elect = require('elect')
        return elect.insert('customers', {id = 33, name = 'Mayakovsky', age = 36})
    ]])

    t.assert_equals(err, nil)
    t.assert_covers(obj, {id = 33, name = 'Mayakovsky', age = 36})
    t.assert(type(obj.bucket_id) == 'number')

    -- delete
    local obj, err = g.cluster.main_server.net_box:eval([[
        local elect = require('elect')
        return elect.delete('customers', 33)
    ]])

    t.assert_equals(err, nil)
    t.assert_covers(obj, {id = 33, name = 'Mayakovsky', age = 36})
    t.assert(type(obj.bucket_id) == 'number')

    -- get
    local obj, err = g.cluster.main_server.net_box:eval([[
        local elect = require('elect')
        return elect.get('customers', 33)
    ]])

    t.assert_equals(err, nil)
    t.assert_equals(obj, nil)

    -- bad key
    local obj, err = g.cluster.main_server.net_box:eval([[
        local elect = require('elect')
        return elect.delete('customers', 'bad-key')
    ]])

    t.assert_equals(obj, nil)
    t.assert_str_contains(err.err, "Failed for %w+%-0000%-0000%-0000%-000000000000", true)
    t.assert_str_contains(err.err, "Supplied key type of part 0 does not match index part type:")
end

