local fio = require('fio')

local t = require('luatest')
local g_memtx = t.group('simple_operations_memtx')
local g_vinyl = t.group('simple_operations_vinyl')

local helpers = require('test.helper')

math.randomseed(os.time())

local function before_all(g, engine)
    g.cluster = helpers.Cluster:new({
        datadir = fio.tempdir(),
        server_command = helpers.entrypoint('srv_simple_operations'),
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
        env = {
            ['ENGINE'] = engine,
        },
    })
    g.engine = engine
    g.cluster:start()
end

g_memtx.before_all = function() before_all(g_memtx, 'memtx') end
g_vinyl.before_all = function() before_all(g_vinyl, 'vinyl') end

local function after_all(g)
    g.cluster:stop()
    fio.rmtree(g.cluster.datadir)
end

g_memtx.after_all = function() after_all(g_memtx) end
g_vinyl.after_all = function() after_all(g_vinyl) end

local function before_each(g)
    for _, server in ipairs(g.cluster.servers) do
        server.net_box:eval([[
            local space = box.space.customers
            if space ~= nil and not box.cfg.read_only then
                space:truncate()
            end
        ]])
    end
end

g_memtx.before_each(function() before_each(g_memtx) end)
g_vinyl.before_each(function() before_each(g_vinyl) end)

local function add(name, fn)
    g_memtx[name] = fn
    g_vinyl[name] = fn
end

add('test_non_existent_space', function(g)
    -- insert
    local obj, err = g.cluster.main_server.net_box:eval([[
        local space_name = ...
        local crud = require('crud')
        return crud.insert('non_existent_space', {id = 0, name = 'Fedor', age = 59})
    ]])

    t.assert_equals(obj, nil)
    t.assert_str_contains(err.err, 'Space "non_existent_space" doesn\'t exists')

    -- get
    local obj, err = g.cluster.main_server.net_box:eval([[
        local crud = require('crud')
        return crud.get('non_existent_space', 0)
    ]])

    t.assert_equals(obj, nil)
    t.assert_str_contains(err.err, 'Space "non_existent_space" doesn\'t exists')

    -- update
    local obj, err = g.cluster.main_server.net_box:eval([[
        local crud = require('crud')
        return crud.update('non_existent_space', 0, {{'+', 'age', 1}})
    ]])

    t.assert_equals(obj, nil)
    t.assert_str_contains(err.err, 'Space "non_existent_space" doesn\'t exists')

    -- delete
    local obj, err = g.cluster.main_server.net_box:eval([[
        local crud = require('crud')
        return crud.delete('non_existent_space', 0)
    ]])

    t.assert_equals(obj, nil)
    t.assert_str_contains(err.err, 'Space "non_existent_space" doesn\'t exists')

    -- replace
    local obj, err = g.cluster.main_server.net_box:eval([[
        local crud = require('crud')
        return crud.replace('non_existent_space', {id = 0, name = 'Fedor', age = 59})
    ]])

    t.assert_equals(obj, nil)
    t.assert_str_contains(err.err, 'Space "non_existent_space" doesn\'t exists')

    -- upsert
    local obj, err = g.cluster.main_server.net_box:eval([[
        local crud = require('crud')
        return crud.upsert('non_existent_space', {id = 0, name = 'Fedor', age = 59}, {{'+', 'age', 1}})
    ]])

    t.assert_equals(obj, nil)
    t.assert_str_contains(err.err, 'Space "non_existent_space" doesn\'t exists')
end)

add('test_insert_get', function(g)
    -- insert
    local obj, err = g.cluster.main_server.net_box:eval([[
        local crud = require('crud')
        return crud.insert('customers', {id = 1, name = 'Fedor', age = 59})
    ]])

    t.assert_equals(err, nil)
    t.assert_covers(obj, {id = 1, name = 'Fedor', age = 59})
    t.assert(type(obj.bucket_id) == 'number')

    -- get
    local obj, err = g.cluster.main_server.net_box:eval([[
        local crud = require('crud')
        return crud.get('customers', 1)
    ]])

    t.assert_equals(err, nil)
    t.assert(obj ~= nil)
    t.assert_covers(obj, {id = 1, name = 'Fedor', age = 59})
    t.assert(type(obj.bucket_id) == 'number')

    -- insert again
    local obj, err = g.cluster.main_server.net_box:eval([[
        local crud = require('crud')
        return crud.insert('customers', {id = 1, name = 'Alexander', age = 37})
    ]])

    t.assert_equals(obj, nil)
    t.assert_str_contains(err.err, "Failed for %w+%-0000%-0000%-0000%-000000000000", true)
    t.assert_str_contains(err.err, "Duplicate key exists")

    -- bad format
    local obj, err = g.cluster.main_server.net_box:eval([[
        local crud = require('crud')
        return crud.insert('customers', {id = 2, name = 'Alexander'})
    ]])

    t.assert_equals(obj, nil)
    t.assert_str_contains(err.err, " Field \"age\" isn't nullable")

    -- get non existent
    local obj, err = g.cluster.main_server.net_box:eval([[
        local crud = require('crud')
        return crud.get('customers', 100)
    ]])

    t.assert_equals(err, nil)
    t.assert_equals(obj, nil)
end)

add('test_insert_get_as_tuple', function(g)
    -- insert
    local obj, err = g.cluster.main_server.net_box:eval([[
        local crud = require('crud')
        return crud.insert('customers', {1, nil, 'Fedor', 59}, {tuples_tomap = false})
    ]])

    t.assert_equals(err, nil)
    t.assert(type(obj[2]) == 'number')
    t.assert_covers(obj, {1, obj[2], 'Fedor', 59})

    -- get
    local obj, err = g.cluster.main_server.net_box:eval([[
        local crud = require('crud')
        return crud.get('customers', 1, {tuples_tomap = false})
    ]])

    t.assert_equals(err, nil)
    t.assert(obj ~= nil)
    t.assert(type(obj[2]) == 'number')
    t.assert_covers(obj, {1, obj[2], 'Fedor', 59})

    -- get as map
    local obj, err = g.cluster.main_server.net_box:eval([[
        local crud = require('crud')
        return crud.get('customers', 1, {tuples_tomap = true})
    ]])

    t.assert_equals(err, nil)
    t.assert(obj ~= nil)
    t.assert_covers(obj, {id = 1, name = 'Fedor', age = 59})
    t.assert(type(obj.bucket_id) == 'number')

    -- insert again
    local obj, err = g.cluster.main_server.net_box:eval([[
        local crud = require('crud')
        return crud.insert('customers', {1, nil, 'Alexander', 37}, {tuples_tomap = false})
    ]])

    t.assert_equals(obj, nil)
    t.assert_str_contains(err.err, "Failed for %w+%-0000%-0000%-0000%-000000000000", true)
    t.assert_str_contains(err.err, "Duplicate key exists")

    -- get non existent
    local obj, err = g.cluster.main_server.net_box:eval([[
        local crud = require('crud')
        return crud.get('customers', 100, {tuples_tomap = false})
    ]])

    t.assert_equals(err, nil)
    t.assert_equals(obj, nil)
end)

add('test_update', function(g)
    -- insert tuple
    local obj, err = g.cluster.main_server.net_box:eval([[
        local crud = require('crud')
        return crud.insert('customers', {id = 22, name = 'Leo', age = 72})
    ]])

    t.assert_equals(err, nil)
    t.assert_covers(obj, {id = 22, name = 'Leo', age = 72})
    t.assert(type(obj.bucket_id) == 'number')

    -- update age and name fields
    local obj, err = g.cluster.main_server.net_box:eval([[
        local crud = require('crud')
        return crud.update('customers', 22, {
            {'+', 'age', 10},
            {'=', 'name', 'Leo Tolstoy'}
        })
    ]])

    t.assert_equals(err, nil)
    t.assert_covers(obj, {id = 22, name = 'Leo Tolstoy', age = 82})
    t.assert(type(obj.bucket_id) == 'number')

    -- get
    local obj, err = g.cluster.main_server.net_box:eval([[
        local crud = require('crud')
        return crud.get('customers', 22)
    ]])

    t.assert_equals(err, nil)
    t.assert_covers(obj, {id = 22, name = 'Leo Tolstoy', age = 82})
    t.assert(type(obj.bucket_id) == 'number')

    -- bad key
    local obj, err = g.cluster.main_server.net_box:eval([[
        local crud = require('crud')
        return crud.update('customers', 'bad-key', {{'+', 'age', 10},})
    ]])

    t.assert_equals(obj, nil)
    t.assert_str_contains(err.err, "Failed for %w+%-0000%-0000%-0000%-000000000000", true)
    t.assert_str_contains(err.err, "Supplied key type of part 0 does not match index part type:")

    -- update by field numbers
    local obj, err = g.cluster.main_server.net_box:eval([[
        local crud = require('crud')
        return crud.update('customers', 22, {
            {'-', 4, 10},
            {'=', 3, 'Leo'}
        })
    ]])

    t.assert_equals(err, nil)
    t.assert_covers(obj, {id = 22, name = 'Leo', age = 72})
    t.assert(type(obj.bucket_id) == 'number')

    -- get
    local obj, err = g.cluster.main_server.net_box:eval([[
        local crud = require('crud')
        return crud.get('customers', 22)
    ]])

    t.assert_equals(err, nil)
    t.assert_covers(obj, {id = 22, name = 'Leo', age = 72})
    t.assert(type(obj.bucket_id) == 'number')
end)

add('test_update_as_tuple', function(g)
    -- insert tuple
    local obj, err = g.cluster.main_server.net_box:eval([[
        local crud = require('crud')
        return crud.insert('customers', {22, nil, 'Leo', 72}, {tuples_tomap = false})
    ]])

    t.assert_equals(err, nil)
    t.assert(type(obj[2]) == 'number')
    t.assert_equals(obj, {22, obj[2], 'Leo', 72})

    -- update age and name fields
    local obj, err = g.cluster.main_server.net_box:eval([[
        local crud = require('crud')
        return crud.update('customers', 22, {
            {'+', 'age', 10},
            {'=', 'name', 'Leo Tolstoy'}
        }, {tuples_tomap = false})
    ]])

    t.assert_equals(err, nil)
    t.assert(type(obj[2]) == 'number')
    t.assert_equals(obj, {22, obj[2], 'Leo Tolstoy', 82})

    -- get
    local obj, err = g.cluster.main_server.net_box:eval([[
        local crud = require('crud')
        return crud.get('customers', 22, {tuples_tomap = false})
    ]])

    t.assert_equals(err, nil)
    t.assert(type(obj[2]) == 'number')
    t.assert_equals(obj, {22, obj[2], 'Leo Tolstoy', 82})

    -- bad key
    local obj, err = g.cluster.main_server.net_box:eval([[
        local crud = require('crud')
        return crud.update('customers', 'bad-key', {{'+', 'age', 10},}, {tuples_tomap = false})
    ]])

    t.assert_equals(obj, nil)
    t.assert_str_contains(err.err, "Failed for %w+%-0000%-0000%-0000%-000000000000", true)
    t.assert_str_contains(err.err, "Supplied key type of part 0 does not match index part type:")

    -- update by field numbers
    local obj, err = g.cluster.main_server.net_box:eval([[
        local crud = require('crud')
        return crud.update('customers', 22, {
            {'-', 4, 10},
            {'=', 3, 'Leo'}
        }, {tuples_tomap = false})
    ]])

    t.assert_equals(err, nil)
    t.assert(type(obj[2]) == 'number')
    t.assert_equals(obj, {22, obj[2], 'Leo', 72})

    -- get
    local obj, err = g.cluster.main_server.net_box:eval([[
        local crud = require('crud')
        return crud.get('customers', 22, {tuples_tomap = false})
    ]])

    t.assert_equals(err, nil)
    t.assert(type(obj[2]) == 'number')
    t.assert_equals(obj, {22, obj[2], 'Leo', 72})
end)

add('test_delete', function(g)
    -- insert tuple
    local obj, err = g.cluster.main_server.net_box:eval([[
        local crud = require('crud')
        return crud.insert('customers', {id = 33, name = 'Mayakovsky', age = 36})
    ]])

    t.assert_equals(err, nil)
    t.assert_covers(obj, {id = 33, name = 'Mayakovsky', age = 36})
    t.assert(type(obj.bucket_id) == 'number')

    -- delete
    local obj, err = g.cluster.main_server.net_box:eval([[
        local crud = require('crud')
        return crud.delete('customers', 33)
    ]])

    t.assert_equals(err, nil)
    if g.engine == 'memtx' then
    t.assert_covers(obj, {id = 33, name = 'Mayakovsky', age = 36})
    t.assert(type(obj.bucket_id) == 'number')
    else
        t.assert_equals(obj, nil)
    end

    -- get
    local obj, err = g.cluster.main_server.net_box:eval([[
        local crud = require('crud')
        return crud.get('customers', 33)
    ]])

    t.assert_equals(err, nil)
    t.assert_equals(obj, nil)

    -- bad key
    local obj, err = g.cluster.main_server.net_box:eval([[
        local crud = require('crud')
        return crud.delete('customers', 'bad-key')
    ]])

    t.assert_equals(obj, nil)
    t.assert_str_contains(err.err, "Failed for %w+%-0000%-0000%-0000%-000000000000", true)
    t.assert_str_contains(err.err, "Supplied key type of part 0 does not match index part type:")
end)

add('test_delete_as_tuple', function(g)
    -- insert tuple
    local obj, err = g.cluster.main_server.net_box:eval([[
        local crud = require('crud')
        return crud.insert('customers', {33, nil, 'Mayakovsky', 36}, {tuples_tomap = false})
    ]])

    t.assert_equals(err, nil)
    t.assert(type(obj[2]) == 'number')
    t.assert_equals(obj, {33, obj[2], 'Mayakovsky', 36})

    -- delete
    local obj, err = g.cluster.main_server.net_box:eval([[
        local crud = require('crud')
        return crud.delete('customers', 33, {tuples_tomap = false})
    ]])

    t.assert_equals(err, nil)
    if g.engine == 'memtx' then
        t.assert(type(obj[2]) == 'number')
        t.assert_equals(obj, {33, obj[2], 'Mayakovsky', 36})
    else
        t.assert_equals(obj, nil)
    end

    -- get
    local obj, err = g.cluster.main_server.net_box:eval([[
        local crud = require('crud')
        return crud.get('customers', 33, {tuples_tomap = false})
    ]])

    t.assert_equals(err, nil)
    t.assert_equals(obj, nil)

    -- bad key
    local obj, err = g.cluster.main_server.net_box:eval([[
        local crud = require('crud')
        return crud.delete('customers', 'bad-key', {tuples_tomap = false})
    ]])

    t.assert_equals(obj, nil)
    t.assert_str_contains(err.err, "Failed for %w+%-0000%-0000%-0000%-000000000000", true)
    t.assert_str_contains(err.err, "Supplied key type of part 0 does not match index part type:")
end)

add('test_replace', function(g)
    -- get
    local obj, err = g.cluster.main_server.net_box:eval([[
        local crud = require('crud')
        return crud.get('customers', 44)
    ]])

    t.assert_equals(err, nil)
    t.assert_equals(obj, nil)

    -- insert tuple
    local obj, err = g.cluster.main_server.net_box:eval([[
        local crud = require('crud')
        return crud.replace('customers', {id = 44, name = 'John Doe', age = 25})
    ]])

    t.assert_equals(err, nil)
    t.assert_covers(obj, {id = 44, name = 'John Doe', age = 25})
    t.assert(type(obj.bucket_id) == 'number')

    -- replace tuple
    local obj, err = g.cluster.main_server.net_box:eval([[
        local crud = require('crud')
        return crud.replace('customers', {id = 44, name = 'Jane Doe', age = 18})
    ]])

    t.assert_equals(err, nil)
    t.assert_covers(obj, {id = 44, name = 'Jane Doe', age = 18})
    t.assert(type(obj.bucket_id) == 'number')
end)

add('test_upsert', function(g)
    -- upsert tuple not exist
    local obj, err = g.cluster.main_server.net_box:eval([[
        local crud = require('crud')
        return crud.upsert('customers', {id = 66, name = 'Jack Sparrow', age = 25}, {
            {'+', 'age', 25},
            {'=', 'name', 'Leo Tolstoy'}
        })
    ]])

    t.assert_equals(obj, nil)
    t.assert_equals(err, nil)

    -- get
    local obj, err = g.cluster.main_server.net_box:eval([[
        local crud = require('crud')
        return crud.get('customers', 66)
    ]])
    t.assert_equals(err, nil)
    t.assert_covers(obj, {id = 66, name = 'Jack Sparrow', age = 25})
    t.assert(type(obj.bucket_id) == 'number')

    -- upsert same query second time tuple exist
    local obj, err = g.cluster.main_server.net_box:eval([[
        local crud = require('crud')
        return crud.upsert('customers', {id = 66, name = 'Jack Sparrow', age = 25}, {
            {'+', 'age', 25},
            {'=', 'name', 'Leo Tolstoy'}
        })
    ]])

    t.assert_equals(obj, nil)
    t.assert_equals(err, nil)

    -- get
    local obj, err = g.cluster.main_server.net_box:eval([[
        local crud = require('crud')
        return crud.get('customers', 66)
    ]])
    t.assert_equals(err, nil)
    t.assert_covers(obj, {id = 66, name = 'Leo Tolstoy', age = 50})
    t.assert(type(obj.bucket_id) == 'number')
end)
