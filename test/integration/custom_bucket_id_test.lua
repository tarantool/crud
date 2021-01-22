local fio = require('fio')

local t = require('luatest')
local g_memtx = t.group('custom_bucket_id_memtx')
local g_vinyl = t.group('custom_bucket_id_vinyl')

local crud_utils = require('crud.common.utils')

local helpers = require('test.helper')

local function before_all(g, engine)
    g.cluster = helpers.Cluster:new({
        datadir = fio.tempdir(),
        server_command = helpers.entrypoint('srv_simple_operations'),
        use_vshard = true,
        replicasets = {
            {
                uuid = helpers.uuid('a'),
                alias = 'router',
                roles = { 'crud-router' },
                servers = {
                    { instance_uuid = helpers.uuid('a', 1), alias = 'router' },
                },
            },
            {
                uuid = helpers.uuid('b'),
                alias = 's-1',
                roles = { 'customers-storage', 'crud-storage' },
                servers = {
                    { instance_uuid = helpers.uuid('b', 1), alias = 's1-master' },
                    { instance_uuid = helpers.uuid('b', 2), alias = 's1-replica' },
                },
            },
            {
                uuid = helpers.uuid('c'),
                alias = 's-2',
                roles = { 'customers-storage', 'crud-storage' },
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

    g.space_format = g.cluster.servers[2].net_box.space.customers:format()
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

local function get_other_storage_bucket_id(g, key)
    local res_bucket_id, err = g.cluster.main_server.net_box:eval([[
        local vshard = require('vshard')

        local key = ...
        local bucket_id = vshard.router.bucket_id_strcrc32(key)

        local replicasets = vshard.router.routeall()

        local other_replicaset_uuid
        for replicaset_uuid, replicaset in pairs(replicasets) do
            local stat, err = replicaset:callrw('vshard.storage.bucket_stat', {bucket_id})

            if err ~= nil and err.name == 'WRONG_BUCKET' then
                other_replicaset_uuid = replicaset_uuid
                break
            end

            if err ~= nil then
                return nil, string.format(
                    'vshard.storage.bucket_stat returned unexpected error: %s',
                    require('json').encode(err)
                )
            end
        end

        if other_replicaset_uuid == nil then
            return nil, 'Other replicaset is not found'
        end

        local other_replicaset = replicasets[other_replicaset_uuid]
        if other_replicaset == nil then
            return nil, string.format('Replicaset %s not found', other_replicaset_uuid)
        end

        local buckets_info = other_replicaset:callrw('vshard.storage.buckets_info')
        local res_bucket_id = next(buckets_info)

        return res_bucket_id
    ]], {key})

    t.assert(res_bucket_id ~= nil, err)
    return res_bucket_id
end

local function check_get(g, space_name, id, bucket_id, tuple)
    local result, err = g.cluster.main_server.net_box:call('crud.get', {
        space_name, id,
    })

    t.assert_equals(err, nil)
    t.assert(result ~= nil)
    t.assert_equals(#result.rows, 0)

    -- get w/ right bucket_id
    local result, err = g.cluster.main_server.net_box:call('crud.get', {
        space_name, id, {bucket_id = bucket_id}
    })

    t.assert_equals(err, nil)
    t.assert(result ~= nil)
    t.assert_equals(result.rows, {tuple})
end

add('test_update', function(g)
    local tuple = {2, box.NULL, 'Ivan', 20}
    local bucket_id = get_other_storage_bucket_id(g, tuple[1])

    local update_operations = {
        {'+', 'age', 10},
        {'=', 'name', 'Leo Tolstoy'},
    }

    -- insert tuple
    local result, err = g.cluster.main_server.net_box:call('crud.insert', {
        'customers', tuple, {bucket_id = bucket_id}
    })

    t.assert_equals(err, nil)
    t.assert(result ~= nil)
    t.assert_equals(#result.rows, 1)

    -- update w/ default bucket_id
    local result, err = g.cluster.main_server.net_box:call('crud.update', {
        'customers', tuple[1], update_operations,
    })

    t.assert_equals(err, nil)
    t.assert(result ~= nil)
    -- tuple not found, update returned nil
    t.assert_equals(#result.rows, 0)

    -- update w/ right bucket_id
    local result, err = g.cluster.main_server.net_box:call('crud.update', {
        'customers', tuple[1], update_operations, {bucket_id = bucket_id},
    })

    t.assert_equals(err, nil)
    t.assert(result ~= nil)
    t.assert_equals(#result.rows, 1)
end)

add('test_delete', function(g)
    local tuple = {2, box.NULL, 'Ivan', 20}
    local bucket_id = get_other_storage_bucket_id(g, tuple[1])

    -- insert tuple
    local result, err = g.cluster.main_server.net_box:call('crud.insert', {
        'customers', tuple, {bucket_id = bucket_id}
    })

    t.assert_equals(err, nil)
    t.assert(result ~= nil)
    t.assert_equals(#result.rows, 1)

    -- delete w/ default bucket_id
    local result, err = g.cluster.main_server.net_box:call('crud.delete', {
        'customers', tuple[1],
    })

    t.assert_equals(err, nil)
    t.assert(result ~= nil)
    -- since delete returns nil for vinyl,
    -- just get tuple to check it wasn't deleted

    -- get w/ right bucket_id
    local result, err = g.cluster.main_server.net_box:call('crud.get', {
        'customers', tuple[1], {bucket_id = bucket_id},
    })

    t.assert_equals(err, nil)
    t.assert(result ~= nil)
    -- tuple wasn't deleted
    t.assert_equals(#result.rows, 1)

    -- delete w/ right bucket_id
    local result, err = g.cluster.main_server.net_box:call('crud.delete', {
        'customers', tuple[1], {bucket_id = bucket_id},
    })

    t.assert_equals(err, nil)
    t.assert(result ~= nil)

    -- get w/ right bucket_id
    local result, err = g.cluster.main_server.net_box:call('crud.get', {
        'customers', tuple[1], {bucket_id = bucket_id},
    })

    t.assert_equals(err, nil)
    t.assert(result ~= nil)
    -- tuple was deleted
    t.assert_equals(#result.rows, 0)
end)

add('test_insert_object', function(g)
    local object = {id = 2, name = 'Ivan', age = 46}
    local bucket_id = get_other_storage_bucket_id(g, object.id)
    object.bucket_id = bucket_id

    local tuple = crud_utils.flatten(object, g.space_format, bucket_id)

    -- insert_object
    local result, err = g.cluster.main_server.net_box:call('crud.insert_object', {
        'customers', object, {bucket_id = bucket_id},
    })

    t.assert_equals(err, nil)
    t.assert(result ~= nil)
    t.assert_equals(result.rows, {tuple})

    check_get(g, 'customers', object.id, bucket_id, tuple)
end)

add('test_insert_object_bucket_id_opt', function(g)
    local object = {id = 1, name = 'Fedor', age = 59}
    local bucket_id = get_other_storage_bucket_id(g, object.id)

    local tuple = crud_utils.flatten(object, g.space_format, bucket_id)

    -- insert_object
    local result, err = g.cluster.main_server.net_box:call('crud.insert_object', {
        'customers', object, {bucket_id = bucket_id},
    })

    t.assert_equals(err, nil)
    t.assert(result ~= nil)
    t.assert_equals(result.rows, {tuple})

    check_get(g, 'customers', object.id, bucket_id, tuple)
end)

add('test_insert_object_bucket_id_specified_twice', function(g)
    local object = {id = 1, name = 'Fedor', age = 59}
    local bucket_id = get_other_storage_bucket_id(g, object.id)
    object.bucket_id = bucket_id

    local tuple = crud_utils.flatten(object, g.space_format, bucket_id)

    -- insert_object, opts.bucket_id is different
    local result, err = g.cluster.main_server.net_box:call('crud.insert_object', {
        'customers', object, {bucket_id = bucket_id + 1},
    })

    t.assert_equals(result, nil)
    t.assert_str_contains(err.err, 'Tuple and opts.bucket_id contain different bucket_id values')

    -- insert_object, opts.bucket_id is the same
    local result, err = g.cluster.main_server.net_box:call('crud.insert_object', {
        'customers', object, {bucket_id = bucket_id},
    })

    t.assert_equals(err, nil)
    t.assert(result ~= nil)
    t.assert_equals(result.rows, {tuple})

    check_get(g, 'customers', object.id, bucket_id, tuple)
end)

add('test_insert', function(g)
    local tuple = {2, box.NULL, 'Ivan', 20}
    local bucket_id = get_other_storage_bucket_id(g, tuple[1])
    tuple[2] = bucket_id

    -- insert
    local result, err = g.cluster.main_server.net_box:call('crud.insert', {
        'customers', tuple,
    })

    t.assert_equals(err, nil)
    t.assert(result ~= nil)
    t.assert_equals(result.rows, {tuple})

    check_get(g, 'customers', tuple[1], bucket_id, tuple)
end)

add('test_insert_bucket_id_opt', function(g)
    local tuple = {1, box.NULL, 'Ivan', 20}
    local bucket_id = get_other_storage_bucket_id(g, tuple[1])

    local tuple_with_bucket_id = table.copy(tuple)
    tuple_with_bucket_id[2] = bucket_id

    -- insert
    local result, err = g.cluster.main_server.net_box:call('crud.insert', {
        'customers', tuple, {bucket_id = bucket_id},
    })

    t.assert_equals(err, nil)
    t.assert(result ~= nil)
    t.assert_equals(result.rows, {tuple_with_bucket_id})

    check_get(g, 'customers', tuple[1], bucket_id, tuple_with_bucket_id)
end)

add('test_insert_bucket_id_specified_twice', function(g)
    local tuple = {2, box.NULL, 'Ivan', 20}
    local bucket_id = get_other_storage_bucket_id(g, tuple[1])
    tuple[2] = bucket_id

    -- insert, opts.bucket_id is different
    local result, err = g.cluster.main_server.net_box:call('crud.insert', {
        'customers', tuple, {bucket_id = bucket_id + 1}
    })

    t.assert_equals(result, nil)
    t.assert_str_contains(err.err, 'Tuple and opts.bucket_id contain different bucket_id values')

    -- insert, opts.bucket_id is the same
    local result, err = g.cluster.main_server.net_box:call('crud.insert', {
        'customers', tuple, {bucket_id = bucket_id}
    })

    t.assert_equals(err, nil)
    t.assert(result ~= nil)
    t.assert_equals(result.rows, {tuple})

    check_get(g, 'customers', tuple[1], bucket_id, tuple)
end)

add('test_replace_object', function(g)
    local object = {id = 2, name = 'Jane', age = 21}
    local bucket_id = get_other_storage_bucket_id(g, object.id)
    object.bucket_id = bucket_id

    local tuple = crud_utils.flatten(object, g.space_format, bucket_id)

    -- replace_object
    local result, err = g.cluster.main_server.net_box:call('crud.replace_object', {
        'customers', object,
    })

    t.assert_equals(err, nil)
    t.assert(result ~= nil)
    t.assert_equals(result.rows, {tuple})

    check_get(g, 'customers', object.id, bucket_id, tuple)
end)

add('test_replace_object_bucket_id_opt', function(g)
    local object = {id = 1, name = 'John', age = 25}
    local bucket_id = get_other_storage_bucket_id(g, object.id)

    local tuple = crud_utils.flatten(object, g.space_format, bucket_id)

    -- replace_object
    local result, err = g.cluster.main_server.net_box:call('crud.replace_object', {
        'customers', object, {bucket_id = bucket_id}
    })

    t.assert_equals(err, nil)
    t.assert(result ~= nil)
    t.assert_equals(result.rows, {tuple})

    check_get(g, 'customers', object.id, bucket_id, tuple)
end)

add('test_replace_object_bucket_id_specified_twice', function(g)
    local object = {id = 1, name = 'Fedor', age = 59}
    local bucket_id = get_other_storage_bucket_id(g, object.id)
    object.bucket_id = bucket_id

    local tuple = crud_utils.flatten(object, g.space_format, bucket_id)

    -- replace_object, opts.bucket_id is different
    local result, err = g.cluster.main_server.net_box:call('crud.replace_object', {
        'customers', object, {bucket_id = bucket_id + 1}
    })

    t.assert_equals(result, nil)
    t.assert_str_contains(err.err, 'Tuple and opts.bucket_id contain different bucket_id values')

    -- replace_object, opts.bucket_id is the same
    local result, err = g.cluster.main_server.net_box:call('crud.replace_object', {
        'customers', object, {bucket_id = bucket_id}
    })

    t.assert_equals(err, nil)
    t.assert(result ~= nil)
    t.assert_equals(result.rows, {tuple})

    check_get(g, 'customers', object.id, bucket_id, tuple)
end)

add('test_replace', function(g)
    local tuple = {2, box.NULL, 'Jane', 21}
    local bucket_id = get_other_storage_bucket_id(g, tuple[1])
    tuple[2] = bucket_id

    -- replace
    local result, err = g.cluster.main_server.net_box:call('crud.replace', {
        'customers', tuple, {bucket_id = bucket_id}
    })

    t.assert_equals(err, nil)
    t.assert(result ~= nil)
    t.assert_equals(result.rows, {tuple})

    check_get(g, 'customers', tuple[1], bucket_id, tuple)
end)

add('test_replace_bucket_id_opt', function(g)
    local tuple = {1, box.NULL, 'John', 25}
    local bucket_id = get_other_storage_bucket_id(g, tuple[1])

    local tuple_with_bucket_id = table.copy(tuple)
    tuple_with_bucket_id[2] = bucket_id

    -- replace
    local result, err = g.cluster.main_server.net_box:call('crud.replace', {
        'customers', tuple, {bucket_id = bucket_id}
    })

    t.assert_equals(err, nil)
    t.assert(result ~= nil)
    t.assert_equals(result.rows, {tuple_with_bucket_id})

    check_get(g, 'customers', tuple[1], bucket_id, tuple_with_bucket_id)
end)

add('test_replace_bucket_id_specified_twice', function(g)
    local tuple = {1, box.NULL, 'John', 25}
    local bucket_id = get_other_storage_bucket_id(g, tuple[1])
    tuple[2] = bucket_id

    -- replace, opts.bucket_id is different
    local result, err = g.cluster.main_server.net_box:call('crud.replace', {
        'customers', tuple, {bucket_id = bucket_id + 1}
    })

    t.assert_equals(result, nil)
    t.assert_str_contains(err.err, 'Tuple and opts.bucket_id contain different bucket_id values')

    -- replace, opts.bucket_id is the same
    local result, err = g.cluster.main_server.net_box:call('crud.replace', {
        'customers', tuple, {bucket_id = bucket_id}
    })

    t.assert_equals(err, nil)
    t.assert(result ~= nil)
    t.assert_equals(result.rows, {tuple})

    check_get(g, 'customers', tuple[1], bucket_id, tuple)
end)

add('test_upsert_object', function(g)
    local  object = {id = 2, name = 'Jane', age = 21}
    local bucket_id = get_other_storage_bucket_id(g, object.id)
    object.bucket_id = bucket_id

    local tuple = crud_utils.flatten(object, g.space_format, bucket_id)

    -- upsert_object
    local result, err = g.cluster.main_server.net_box:call('crud.upsert_object', {
        'customers', object, {},
    })

    t.assert_equals(err, nil)
    t.assert(result ~= nil)
    t.assert_equals(#result.rows, 0)

    check_get(g, 'customers', object.id, bucket_id, tuple)
end)

add('test_upsert_object_bucket_id_opt', function(g)
    local object = {id = 1, name = 'John', age = 25}
    local bucket_id = get_other_storage_bucket_id(g, object.id)

    local tuple = crud_utils.flatten(object, g.space_format, bucket_id)

    -- upsert_object
    local result, err = g.cluster.main_server.net_box:call('crud.upsert_object', {
        'customers', object, {}, {bucket_id = bucket_id}
    })

    t.assert_equals(err, nil)
    t.assert(result ~= nil)
    t.assert_equals(#result.rows, 0)

    check_get(g, 'customers', object.id, bucket_id, tuple)
end)

add('test_upsert_object_bucket_id_specified_twice', function(g)
    local object = {id = 1, name = 'Fedor', age = 59}
    local bucket_id = get_other_storage_bucket_id(g, object.id)
    object.bucket_id = bucket_id

    local tuple = crud_utils.flatten(object, g.space_format, bucket_id)

    -- upsert_object, opts.bucket_id is different
    local result, err = g.cluster.main_server.net_box:call('crud.upsert_object', {
        'customers', object, {}, {bucket_id = bucket_id + 1}
    })

    t.assert_equals(result, nil)
    t.assert_str_contains(err.err, 'Tuple and opts.bucket_id contain different bucket_id values')

    -- upsert_object, opts.bucket_id is the same
    local result, err = g.cluster.main_server.net_box:call('crud.upsert_object', {
        'customers', object, {}, {bucket_id = bucket_id}
    })

    t.assert_equals(err, nil)
    t.assert(result ~= nil)
    t.assert_equals(#result.rows, 0)

    check_get(g, 'customers', object.id, bucket_id, tuple)
end)

add('test_upsert', function(g)
    local tuple = {1, box.NULL, 'John', 25}
    local bucket_id = get_other_storage_bucket_id(g, tuple[1])
    tuple[2] = bucket_id

    -- upsert
    local result, err = g.cluster.main_server.net_box:call('crud.upsert', {
        'customers', tuple, {}, {bucket_id = bucket_id}
    })

    t.assert_equals(err, nil)
    t.assert(result ~= nil)
    t.assert_equals(#result.rows, 0)

    check_get(g, 'customers', tuple[1], bucket_id, tuple)
end)

add('test_upsert_bucket_id_opt', function(g)
    local tuple = {1, box.NULL, 'John', 25}
    local bucket_id = get_other_storage_bucket_id(g, tuple[1])

    local tuple_with_bucket_id = table.copy(tuple)
    tuple_with_bucket_id[2] = bucket_id

    -- upsert
    local result, err = g.cluster.main_server.net_box:call('crud.upsert', {
        'customers', tuple, {}, {bucket_id = bucket_id}
    })

    t.assert_equals(err, nil)
    t.assert(result ~= nil)
    t.assert_equals(#result.rows, 0)

    check_get(g, 'customers', tuple[1], bucket_id, tuple_with_bucket_id)
end)

add('test_upsert_bucket_id_specified_twice', function(g)
    local tuple = {1, box.NULL, 'John', 25}
    local bucket_id = get_other_storage_bucket_id(g, tuple[1])
    tuple[2] = bucket_id

    -- upsert, opts.bucket_id is different
    local result, err = g.cluster.main_server.net_box:call('crud.upsert', {
        'customers', tuple, {}, {bucket_id = bucket_id + 1}
    })

    t.assert_equals(result, nil)
    t.assert_str_contains(err.err, 'Tuple and opts.bucket_id contain different bucket_id values')

    -- upsert, opts.bucket_id is the same
    local result, err = g.cluster.main_server.net_box:call('crud.upsert', {
        'customers', tuple, {}, {bucket_id = bucket_id}
    })

    t.assert_equals(err, nil)
    t.assert(result ~= nil)
    t.assert_equals(#result.rows, 0)

    check_get(g, 'customers', tuple[1], bucket_id, tuple)
end)

add('test_select', function(g)
    local tuple = {2, box.NULL, 'Ivan', 20}
    local bucket_id = get_other_storage_bucket_id(g, tuple[1])

    -- insert tuple
    local result, err = g.cluster.main_server.net_box:call('crud.insert', {
        'customers', tuple, {bucket_id = bucket_id}
    })

    t.assert_equals(err, nil)
    t.assert(result ~= nil)
    t.assert_equals(#result.rows, 1)

    local conditions = {{'==', 'id', tuple[1]}}

    -- select w/ default bucket_id
    local result, err = g.cluster.main_server.net_box:call('crud.select', {
        'customers', conditions,
    })

    t.assert_equals(err, nil)
    t.assert(result ~= nil)
    -- tuple not found
    t.assert_equals(#result.rows, 0)

    -- select w/ right bucket_id
    local result, err = g.cluster.main_server.net_box:call('crud.select', {
        'customers', conditions, {bucket_id = bucket_id},
    })

    t.assert_equals(err, nil)
    t.assert(result ~= nil)
    -- tuple is found
    t.assert_equals(#result.rows, 1)
end)
