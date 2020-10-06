local fio = require('fio')

local t = require('luatest')
local g_memtx = t.group('simple_operations_memtx')
local g_vinyl = t.group('simple_operations_vinyl')
local crud = require('crud')

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
    local result, err = g.cluster.main_server.net_box:eval([[
        local space_name = ...
        local crud = require('crud')
        return crud.insert('non_existent_space', {0, box.NULL, 'Fedor', 59})
    ]])

    t.assert_equals(result, nil)
    t.assert_str_contains(err.err, 'Space "non_existent_space" doesn\'t exist')

    -- insert_object
    local result, err = g.cluster.main_server.net_box:eval([[
        local space_name = ...
        local crud = require('crud')
        return crud.insert_object('non_existent_space', {id = 0, name = 'Fedor', age = 59})
    ]])

    t.assert_equals(result, nil)
    t.assert_str_contains(err.err, 'Space "non_existent_space" doesn\'t exist')

    -- get
    local result, err = g.cluster.main_server.net_box:eval([[
        local crud = require('crud')
        return crud.get('non_existent_space', 0)
    ]])

    t.assert_equals(result, nil)
    t.assert_str_contains(err.err, 'Space "non_existent_space" doesn\'t exist')

    -- update
    local result, err = g.cluster.main_server.net_box:eval([[
        local crud = require('crud')
        return crud.update('non_existent_space', 0, {{'+', 'age', 1}})
    ]])

    t.assert_equals(result, nil)
    t.assert_str_contains(err.err, 'Space "non_existent_space" doesn\'t exist')

    -- delete
    local result, err = g.cluster.main_server.net_box:eval([[
        local crud = require('crud')
        return crud.delete('non_existent_space', 0)
    ]])

    t.assert_equals(result, nil)
    t.assert_str_contains(err.err, 'Space "non_existent_space" doesn\'t exist')

    -- replace
    local result, err = g.cluster.main_server.net_box:eval([[
        local crud = require('crud')
        return crud.replace('non_existent_space', {0, box.NULL, 'Fedor', 59})
    ]])

    t.assert_equals(result, nil)
    t.assert_str_contains(err.err, 'Space "non_existent_space" doesn\'t exist')

    -- replace_object
    local result, err = g.cluster.main_server.net_box:eval([[
        local crud = require('crud')
        return crud.replace_object('non_existent_space', {id = 0, name = 'Fedor', age = 59})
    ]])

    t.assert_equals(result, nil)
    t.assert_str_contains(err.err, 'Space "non_existent_space" doesn\'t exist')

    -- upsert
    local result, err = g.cluster.main_server.net_box:eval([[
        local crud = require('crud')
        return crud.upsert_object('non_existent_space', {0, box.NULL, 'Fedor', 59}, {{'+', 'age', 1}})
    ]])

    t.assert_equals(result, nil)
    t.assert_str_contains(err.err, 'Space "non_existent_space" doesn\'t exist')

    -- upsert_object
    local result, err = g.cluster.main_server.net_box:eval([[
        local crud = require('crud')
        return crud.upsert_object('non_existent_space', {id = 0, name = 'Fedor', age = 59}, {{'+', 'age', 1}})
    ]])

    t.assert_equals(result, nil)
    t.assert_str_contains(err.err, 'Space "non_existent_space" doesn\'t exist')
end)

add('test_insert_object_get', function(g)
    -- insert_object
    local result, err = g.cluster.main_server.net_box:eval([[
        local crud = require('crud')
        return crud.insert_object('customers', {id = 1, name = 'Fedor', age = 59})
    ]])

    t.assert_equals(err, nil)
    t.assert_equals(result.metadata, {
        {name = 'id', type = 'unsigned'},
        {name = 'bucket_id', type = 'unsigned'},
        {name = 'name', type = 'string'},
        {name = 'age', type = 'number'},
    })
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, {{id = 1, name = 'Fedor', age = 59, bucket_id = 477}})

    -- get
    local result, err = g.cluster.main_server.net_box:eval([[
        local crud = require('crud')
        return crud.get('customers', 1)
    ]])

    t.assert_equals(err, nil)
    t.assert(result ~= nil)
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, {{id = 1, name = 'Fedor', age = 59, bucket_id = 477}})

    -- insert_object again
    local obj, err = g.cluster.main_server.net_box:eval([[
        local crud = require('crud')
        return crud.insert_object('customers', {id = 1, name = 'Alexander', age = 37})
    ]])

    t.assert_equals(obj, nil)
    t.assert_str_contains(err.err, "Failed for %w+%-0000%-0000%-0000%-000000000000", true)
    t.assert_str_contains(err.err, "Duplicate key exists")

    -- bad format
    local obj, err = g.cluster.main_server.net_box:eval([[
        local crud = require('crud')
        return crud.insert_object('customers', {id = 2, name = 'Alexander'})
    ]])

    t.assert_equals(obj, nil)
    t.assert_str_contains(err.err, "Field \"age\" isn't nullable")
end)

add('test_insert_get', function(g)
    -- insert
    local result, err = g.cluster.main_server.net_box:eval([[
        local crud = require('crud')
        return crud.insert('customers', {2, box.NULL, 'Ivan', 20})
    ]])

    t.assert_equals(err, nil)
    t.assert_equals(result.metadata, {
        {name = 'id', type = 'unsigned'},
        {name = 'bucket_id', type = 'unsigned'},
        {name = 'name', type = 'string'},
        {name = 'age', type = 'number'},
    })
    t.assert_equals(result.rows, {{2, 401, 'Ivan', 20}})

    -- get
    local result, err = g.cluster.main_server.net_box:eval([[
        local crud = require('crud')
        return crud.get('customers', 2)
    ]])

    t.assert_equals(err, nil)
    t.assert(result ~= nil)
    t.assert_equals(result.rows, {{2, 401, 'Ivan', 20}})

    -- insert again
    local obj, err = g.cluster.main_server.net_box:eval([[
        local crud = require('crud')
        return crud.insert('customers', {2, box.NULL, 'Ivan', 20})
    ]])

    t.assert_equals(obj, nil)
    t.assert_str_contains(err.err, "Failed for %w+%-0000%-0000%-0000%-000000000000", true)
    t.assert_str_contains(err.err, "Duplicate key exists")

    -- get non-existent
    local result, err = g.cluster.main_server.net_box:eval([[
        local crud = require('crud')
        return crud.get('customers', 100)
    ]])

    t.assert_equals(err, nil)
    t.assert_equals(#result.rows, 0)
end)

add('test_update', function(g)
    -- insert tuple
    local result, err = g.cluster.main_server.net_box:eval([[
        local crud = require('crud')
        return crud.insert_object('customers', {id = 22, name = 'Leo', age = 72})
    ]])

    t.assert_equals(err, nil)
    t.assert_equals(result.metadata, {
        {name = 'id', type = 'unsigned'},
        {name = 'bucket_id', type = 'unsigned'},
        {name = 'name', type = 'string'},
        {name = 'age', type = 'number'},
    })
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, {{id = 22, name = 'Leo', age = 72, bucket_id = 655}})

    -- update age and name fields
    local result, err = g.cluster.main_server.net_box:eval([[
        local crud = require('crud')
        return crud.update('customers', 22, {
            {'+', 'age', 10},
            {'=', 'name', 'Leo Tolstoy'},
        })
    ]])

    t.assert_equals(err, nil)
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, {{id = 22, name = 'Leo Tolstoy', age = 82, bucket_id = 655}})

    -- get
    local result, err = g.cluster.main_server.net_box:eval([[
        local crud = require('crud')
        return crud.get('customers', 22)
    ]])

    t.assert_equals(err, nil)
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, {{id = 22, name = 'Leo Tolstoy', age = 82, bucket_id = 655}})

    -- bad key
    local result, err = g.cluster.main_server.net_box:eval([[
        local crud = require('crud')
        return crud.update('customers', 'bad-key', {{'+', 'age', 10},})
    ]])

    t.assert_equals(result, nil)
    t.assert_str_contains(err.err, "Failed for %w+%-0000%-0000%-0000%-000000000000", true)
    t.assert_str_contains(err.err, "Supplied key type of part 0 does not match index part type:")

    -- update by field numbers
    local result, err = g.cluster.main_server.net_box:eval([[
        local crud = require('crud')
        return crud.update('customers', 22, {
            {'-', 4, 10},
            {'=', 3, 'Leo'}
        })
    ]])

    t.assert_equals(err, nil)
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, {{id = 22, name = 'Leo', age = 72, bucket_id = 655}})

    -- get
    local result, err = g.cluster.main_server.net_box:eval([[
        local crud = require('crud')
        return crud.get('customers', 22)
    ]])

    t.assert_equals(err, nil)
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, {{id = 22, name = 'Leo', age = 72, bucket_id = 655}})
end)

add('test_delete', function(g)
    -- insert tuple
    local result, err = g.cluster.main_server.net_box:eval([[
        local crud = require('crud')
        return crud.insert_object('customers', {id = 33, name = 'Mayakovsky', age = 36})
    ]])

    t.assert_equals(err, nil)
    t.assert_equals(result.metadata, {
        {name = 'id', type = 'unsigned'},
        {name = 'bucket_id', type = 'unsigned'},
        {name = 'name', type = 'string'},
        {name = 'age', type = 'number'},
    })
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, {{id = 33, name = 'Mayakovsky', age = 36, bucket_id = 907}})

    -- delete
    local result, err = g.cluster.main_server.net_box:eval([[
        local crud = require('crud')
        return crud.delete('customers', 33)
    ]])

    t.assert_equals(err, nil)
    if g.engine == 'memtx' then
        local objects = crud.unflatten_rows(result.rows, result.metadata)
        t.assert_equals(objects, {{id = 33, name = 'Mayakovsky', age = 36, bucket_id = 907}})
    else
        t.assert_equals(#result.rows, 0)
    end

    -- get
    local result, err = g.cluster.main_server.net_box:eval([[
        local crud = require('crud')
        return crud.get('customers', 33)
    ]])

    t.assert_equals(err, nil)
    t.assert_equals(#result.rows, 0)

    -- bad key
    local result, err = g.cluster.main_server.net_box:eval([[
        local crud = require('crud')
        return crud.delete('customers', 'bad-key')
    ]])

    t.assert_equals(result, nil)
    t.assert_str_contains(err.err, "Failed for %w+%-0000%-0000%-0000%-000000000000", true)
    t.assert_str_contains(err.err, "Supplied key type of part 0 does not match index part type:")
end)

add('test_replace_object', function(g)
    -- get
    local result, err = g.cluster.main_server.net_box:eval([[
        local crud = require('crud')
        return crud.get('customers', 44)
    ]])

    t.assert_equals(err, nil)
    t.assert_equals(result.metadata, {
        {name = 'id', type = 'unsigned'},
        {name = 'bucket_id', type = 'unsigned'},
        {name = 'name', type = 'string'},
        {name = 'age', type = 'number'},
    })
    t.assert_equals(#result.rows, 0)

    -- replace_object
    local result, err = g.cluster.main_server.net_box:eval([[
        local crud = require('crud')
        return crud.replace_object('customers', {id = 44, name = 'John Doe', age = 25})
    ]])

    t.assert_equals(err, nil)
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, {{id = 44, name = 'John Doe', age = 25, bucket_id = 2805}})

    -- replace_object
    local result, err = g.cluster.main_server.net_box:eval([[
        local crud = require('crud')
        return crud.replace_object('customers', {id = 44, name = 'Jane Doe', age = 18})
    ]])

    t.assert_equals(err, nil)
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, {{id = 44, name = 'Jane Doe', age = 18, bucket_id = 2805}})
end)

add('test_replace', function(g)
    local result, err = g.cluster.main_server.net_box:eval([[
        local crud = require('crud')
        return crud.replace('customers', {45, box.NULL, 'John Fedor', 99})
    ]])

    t.assert_equals(err, nil)
    t.assert_equals(result.metadata, {
        {name = 'id', type = 'unsigned'},
        {name = 'bucket_id', type = 'unsigned'},
        {name = 'name', type = 'string'},
        {name = 'age', type = 'number'},
    })
    t.assert_equals(result.rows, {{45, 392, 'John Fedor', 99}})

    -- replace again
    local result, err = g.cluster.main_server.net_box:eval([[
        local crud = require('crud')
        return crud.replace('customers', {45, box.NULL, 'John Fedor', 100})
    ]])

    t.assert_equals(err, nil)
    t.assert_equals(result.rows, {{45, 392, 'John Fedor', 100}})
end)

add('test_upsert_object', function(g)
    -- upsert_object first time
    local result, err = g.cluster.main_server.net_box:eval([[
        local crud = require('crud')
        return crud.upsert_object('customers', {id = 66, name = 'Jack Sparrow', age = 25}, {
            {'+', 'age', 25},
            {'=', 'name', 'Leo Tolstoy'},
        })
    ]])

    t.assert_equals(#result.rows, 0)
    t.assert_equals(result.metadata, {
        {name = 'id', type = 'unsigned'},
        {name = 'bucket_id', type = 'unsigned'},
        {name = 'name', type = 'string'},
        {name = 'age', type = 'number'},
    })
    t.assert_equals(err, nil)

    -- get
    local result, err = g.cluster.main_server.net_box:eval([[
        local crud = require('crud')
        return crud.get('customers', 66)
    ]])
    t.assert_equals(err, nil)
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, {{id = 66, name = 'Jack Sparrow', age = 25, bucket_id = 486}})

    -- upsert_object the same query second time when tuple exists
    local result, err = g.cluster.main_server.net_box:eval([[
        local crud = require('crud')
        return crud.upsert_object('customers', {id = 66, name = 'Jack Sparrow', age = 25}, {
            {'+', 'age', 25},
            {'=', 'name', 'Leo Tolstoy'},
        })
    ]])

    t.assert_equals(#result.rows, 0)
    t.assert_equals(err, nil)

    -- get
    local result, err = g.cluster.main_server.net_box:eval([[
        local crud = require('crud')
        return crud.get('customers', 66)
    ]])
    t.assert_equals(err, nil)
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, {{id = 66, name = 'Leo Tolstoy', age = 50, bucket_id = 486}})

end)

add('test_upsert', function(g)
    -- upsert tuple first time
    local result, err = g.cluster.main_server.net_box:eval([[
        local crud = require('crud')
        return crud.upsert('customers', {67, box.NULL, 'Saltykov-Shchedrin', 63}, {
            {'=', 'name', 'Mikhail Saltykov-Shchedrin'},
        })
    ]])

    t.assert_equals(#result.rows, 0)
    t.assert_equals(result.metadata, {
        {name = 'id', type = 'unsigned'},
        {name = 'bucket_id', type = 'unsigned'},
        {name = 'name', type = 'string'},
        {name = 'age', type = 'number'},
    })
    t.assert_equals(err, nil)

    -- get
    local result, err = g.cluster.main_server.net_box:eval([[
        local crud = require('crud')
        return crud.get('customers', 67)
    ]])
    t.assert_equals(err, nil)
    t.assert_equals(result.rows, {{67, 1143, 'Saltykov-Shchedrin', 63}})

    -- upsert the same query second time when tuple exists
    local result, err = g.cluster.main_server.net_box:eval([[
        local crud = require('crud')
        return crud.upsert('customers', {67, box.NULL, 'Saltykov-Shchedrin', 63}, {
            {'=', 'name', 'Mikhail Saltykov-Shchedrin'},
        })
    ]])

    t.assert_equals(#result.rows, 0)
    t.assert_equals(err, nil)

    -- get
    local result, err = g.cluster.main_server.net_box:eval([[
        local crud = require('crud')
        return crud.get('customers', 67)
    ]])
    t.assert_equals(err, nil)
    t.assert_equals(result.rows, {{67, 1143, 'Mikhail Saltykov-Shchedrin', 63}})
end)
