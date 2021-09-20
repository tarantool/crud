local fio = require('fio')

local t = require('luatest')
local crud_utils = require('crud.common.utils')

local helpers = require('test.helper')

local pgroup = t.group('custom_bucket_id', {
    {engine = 'memtx'},
    {engine = 'vinyl'},
})

pgroup.before_all(function(g)
    g.cluster = helpers.Cluster:new({
        datadir = fio.tempdir(),
        server_command = helpers.entrypoint('srv_simple_operations'),
        use_vshard = true,
        replicasets = helpers.get_test_replicasets(),
        env = {
            ['ENGINE'] = g.params.engine,
        },
    })

    g.cluster:start()

    g.space_format = g.cluster.servers[2].net_box.space.customers:format()
end)

pgroup.after_all(function(g) helpers.stop_cluster(g.cluster) end)

pgroup.before_each(function(g)
    helpers.truncate_space_on_cluster(g.cluster, 'customers')
end)

local function get_other_storage_bucket_id(g, key)
    local bucket_id = g.cluster.main_server.net_box:eval([[
        local vshard = require('vshard')

        local key = ...
        return vshard.router.bucket_id_strcrc32(key)
    ]], {key})

    local res_bucket_id, err = helpers.get_other_storage_bucket_id(g.cluster, bucket_id)

    t.assert_not_equals(res_bucket_id, nil, err)
    return res_bucket_id
end

local function check_get(g, space_name, id, bucket_id, tuple)
    local result, err = g.cluster.main_server.net_box:call('crud.get', {
        space_name, id,
    })

    t.assert_equals(err, nil)
    t.assert_not_equals(result, nil)
    t.assert_equals(#result.rows, 0)

    -- get w/ right bucket_id
    local result, err = g.cluster.main_server.net_box:call('crud.get', {
        space_name, id, {bucket_id = bucket_id}
    })

    t.assert_equals(err, nil)
    t.assert_not_equals(result, nil)
    t.assert_equals(result.rows, {tuple})
end

pgroup.test_update = function(g)
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
    t.assert_not_equals(result, nil)
    t.assert_equals(#result.rows, 1)

    -- update w/ default bucket_id
    local result, err = g.cluster.main_server.net_box:call('crud.update', {
        'customers', tuple[1], update_operations,
    })

    t.assert_equals(err, nil)
    t.assert_not_equals(result, nil)
    -- tuple not found, update returned nil
    t.assert_equals(#result.rows, 0)

    -- update w/ right bucket_id
    local result, err = g.cluster.main_server.net_box:call('crud.update', {
        'customers', tuple[1], update_operations, {bucket_id = bucket_id},
    })

    t.assert_equals(err, nil)
    t.assert_not_equals(result, nil)
    t.assert_equals(#result.rows, 1)
end

pgroup.test_delete = function(g)
    local tuple = {2, box.NULL, 'Ivan', 20}
    local bucket_id = get_other_storage_bucket_id(g, tuple[1])

    -- insert tuple
    local result, err = g.cluster.main_server.net_box:call('crud.insert', {
        'customers', tuple, {bucket_id = bucket_id}
    })

    t.assert_equals(err, nil)
    t.assert_not_equals(result, nil)
    t.assert_equals(#result.rows, 1)

    -- delete w/ default bucket_id
    local result, err = g.cluster.main_server.net_box:call('crud.delete', {
        'customers', tuple[1],
    })

    t.assert_equals(err, nil)
    t.assert_not_equals(result, nil)
    -- since delete returns nil for vinyl,
    -- just get tuple to check it wasn't deleted

    -- get w/ right bucket_id
    local result, err = g.cluster.main_server.net_box:call('crud.get', {
        'customers', tuple[1], {bucket_id = bucket_id},
    })

    t.assert_equals(err, nil)
    t.assert_not_equals(result, nil)
    -- tuple wasn't deleted
    t.assert_equals(#result.rows, 1)

    -- delete w/ right bucket_id
    local result, err = g.cluster.main_server.net_box:call('crud.delete', {
        'customers', tuple[1], {bucket_id = bucket_id},
    })

    t.assert_equals(err, nil)
    t.assert_not_equals(result, nil)

    -- get w/ right bucket_id
    local result, err = g.cluster.main_server.net_box:call('crud.get', {
        'customers', tuple[1], {bucket_id = bucket_id},
    })

    t.assert_equals(err, nil)
    t.assert_not_equals(result, nil)
    -- tuple was deleted
    t.assert_equals(#result.rows, 0)
end

pgroup.test_insert_object = function(g)
    local object = {id = 2, name = 'Ivan', age = 46}
    local bucket_id = get_other_storage_bucket_id(g, object.id)
    object.bucket_id = bucket_id

    local tuple = crud_utils.flatten(object, g.space_format, bucket_id)

    -- insert_object
    local result, err = g.cluster.main_server.net_box:call('crud.insert_object', {
        'customers', object, {bucket_id = bucket_id},
    })

    t.assert_equals(err, nil)
    t.assert_not_equals(result, nil)
    t.assert_equals(result.rows, {tuple})

    check_get(g, 'customers', object.id, bucket_id, tuple)
end

pgroup.test_insert_object_bucket_id_opt = function(g)
    local object = {id = 1, name = 'Fedor', age = 59}
    local bucket_id = get_other_storage_bucket_id(g, object.id)

    local tuple = crud_utils.flatten(object, g.space_format, bucket_id)

    -- insert_object
    local result, err = g.cluster.main_server.net_box:call('crud.insert_object', {
        'customers', object, {bucket_id = bucket_id},
    })

    t.assert_equals(err, nil)
    t.assert_not_equals(result, nil)
    t.assert_equals(result.rows, {tuple})

    check_get(g, 'customers', object.id, bucket_id, tuple)
end

pgroup.test_insert_object_bucket_id_specified_twice = function(g)
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
    t.assert_not_equals(result, nil)
    t.assert_equals(result.rows, {tuple})

    check_get(g, 'customers', object.id, bucket_id, tuple)
end

pgroup.test_insert = function(g)
    local tuple = {2, box.NULL, 'Ivan', 20}
    local bucket_id = get_other_storage_bucket_id(g, tuple[1])
    tuple[2] = bucket_id

    -- insert
    local result, err = g.cluster.main_server.net_box:call('crud.insert', {
        'customers', tuple,
    })

    t.assert_equals(err, nil)
    t.assert_not_equals(result, nil)
    t.assert_equals(result.rows, {tuple})

    check_get(g, 'customers', tuple[1], bucket_id, tuple)
end

pgroup.test_insert_bucket_id_opt = function(g)
    local tuple = {1, box.NULL, 'Ivan', 20}
    local bucket_id = get_other_storage_bucket_id(g, tuple[1])

    local tuple_with_bucket_id = table.copy(tuple)
    tuple_with_bucket_id[2] = bucket_id

    -- insert
    local result, err = g.cluster.main_server.net_box:call('crud.insert', {
        'customers', tuple, {bucket_id = bucket_id},
    })

    t.assert_equals(err, nil)
    t.assert_not_equals(result, nil)
    t.assert_equals(result.rows, {tuple_with_bucket_id})

    check_get(g, 'customers', tuple[1], bucket_id, tuple_with_bucket_id)
end

pgroup.test_insert_bucket_id_specified_twice = function(g)
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
    t.assert_not_equals(result, nil)
    t.assert_equals(result.rows, {tuple})

    check_get(g, 'customers', tuple[1], bucket_id, tuple)
end

pgroup.test_replace_object = function(g)
    local object = {id = 2, name = 'Jane', age = 21}
    local bucket_id = get_other_storage_bucket_id(g, object.id)
    object.bucket_id = bucket_id

    local tuple = crud_utils.flatten(object, g.space_format, bucket_id)

    -- replace_object
    local result, err = g.cluster.main_server.net_box:call('crud.replace_object', {
        'customers', object,
    })

    t.assert_equals(err, nil)
    t.assert_not_equals(result, nil)
    t.assert_equals(result.rows, {tuple})

    check_get(g, 'customers', object.id, bucket_id, tuple)
end

pgroup.test_replace_object_bucket_id_opt = function(g)
    local object = {id = 1, name = 'John', age = 25}
    local bucket_id = get_other_storage_bucket_id(g, object.id)

    local tuple = crud_utils.flatten(object, g.space_format, bucket_id)

    -- replace_object
    local result, err = g.cluster.main_server.net_box:call('crud.replace_object', {
        'customers', object, {bucket_id = bucket_id}
    })

    t.assert_equals(err, nil)
    t.assert_not_equals(result, nil)
    t.assert_equals(result.rows, {tuple})

    check_get(g, 'customers', object.id, bucket_id, tuple)
end

pgroup.test_replace_object_bucket_id_specified_twice = function(g)
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
    t.assert_not_equals(result, nil)
    t.assert_equals(result.rows, {tuple})

    check_get(g, 'customers', object.id, bucket_id, tuple)
end

pgroup.test_replace = function(g)
    local tuple = {2, box.NULL, 'Jane', 21}
    local bucket_id = get_other_storage_bucket_id(g, tuple[1])
    tuple[2] = bucket_id

    -- replace
    local result, err = g.cluster.main_server.net_box:call('crud.replace', {
        'customers', tuple, {bucket_id = bucket_id}
    })

    t.assert_equals(err, nil)
    t.assert_not_equals(result, nil)
    t.assert_equals(result.rows, {tuple})

    check_get(g, 'customers', tuple[1], bucket_id, tuple)
end

pgroup.test_replace_bucket_id_opt = function(g)
    local tuple = {1, box.NULL, 'John', 25}
    local bucket_id = get_other_storage_bucket_id(g, tuple[1])

    local tuple_with_bucket_id = table.copy(tuple)
    tuple_with_bucket_id[2] = bucket_id

    -- replace
    local result, err = g.cluster.main_server.net_box:call('crud.replace', {
        'customers', tuple, {bucket_id = bucket_id}
    })

    t.assert_equals(err, nil)
    t.assert_not_equals(result, nil)
    t.assert_equals(result.rows, {tuple_with_bucket_id})

    check_get(g, 'customers', tuple[1], bucket_id, tuple_with_bucket_id)
end

pgroup.test_replace_bucket_id_specified_twice = function(g)
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
    t.assert_not_equals(result, nil)
    t.assert_equals(result.rows, {tuple})

    check_get(g, 'customers', tuple[1], bucket_id, tuple)
end

pgroup.test_upsert_object = function(g)
    local  object = {id = 2, name = 'Jane', age = 21}
    local bucket_id = get_other_storage_bucket_id(g, object.id)
    object.bucket_id = bucket_id

    local tuple = crud_utils.flatten(object, g.space_format, bucket_id)

    -- upsert_object
    local result, err = g.cluster.main_server.net_box:call('crud.upsert_object', {
        'customers', object, {},
    })

    t.assert_equals(err, nil)
    t.assert_not_equals(result, nil)
    t.assert_equals(#result.rows, 0)

    check_get(g, 'customers', object.id, bucket_id, tuple)
end

pgroup.test_upsert_object_bucket_id_opt = function(g)
    local object = {id = 1, name = 'John', age = 25}
    local bucket_id = get_other_storage_bucket_id(g, object.id)

    local tuple = crud_utils.flatten(object, g.space_format, bucket_id)

    -- upsert_object
    local result, err = g.cluster.main_server.net_box:call('crud.upsert_object', {
        'customers', object, {}, {bucket_id = bucket_id}
    })

    t.assert_equals(err, nil)
    t.assert_not_equals(result, nil)
    t.assert_equals(#result.rows, 0)

    check_get(g, 'customers', object.id, bucket_id, tuple)
end

pgroup.test_upsert_object_bucket_id_specified_twice = function(g)
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
    t.assert_not_equals(result, nil)
    t.assert_equals(#result.rows, 0)

    check_get(g, 'customers', object.id, bucket_id, tuple)
end

pgroup.test_upsert = function(g)
    local tuple = {1, box.NULL, 'John', 25}
    local bucket_id = get_other_storage_bucket_id(g, tuple[1])
    tuple[2] = bucket_id

    -- upsert
    local result, err = g.cluster.main_server.net_box:call('crud.upsert', {
        'customers', tuple, {}, {bucket_id = bucket_id}
    })

    t.assert_equals(err, nil)
    t.assert_not_equals(result, nil)
    t.assert_equals(#result.rows, 0)

    check_get(g, 'customers', tuple[1], bucket_id, tuple)
end

pgroup.test_upsert_bucket_id_opt = function(g)
    local tuple = {1, box.NULL, 'John', 25}
    local bucket_id = get_other_storage_bucket_id(g, tuple[1])

    local tuple_with_bucket_id = table.copy(tuple)
    tuple_with_bucket_id[2] = bucket_id

    -- upsert
    local result, err = g.cluster.main_server.net_box:call('crud.upsert', {
        'customers', tuple, {}, {bucket_id = bucket_id}
    })

    t.assert_equals(err, nil)
    t.assert_not_equals(result, nil)
    t.assert_equals(#result.rows, 0)

    check_get(g, 'customers', tuple[1], bucket_id, tuple_with_bucket_id)
end

pgroup.test_upsert_bucket_id_specified_twice = function(g)
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
    t.assert_not_equals(result, nil)
    t.assert_equals(#result.rows, 0)

    check_get(g, 'customers', tuple[1], bucket_id, tuple)
end

pgroup.test_select = function(g)
    local tuple = {2, box.NULL, 'Ivan', 20}
    local bucket_id = get_other_storage_bucket_id(g, tuple[1])

    -- insert tuple
    local result, err = g.cluster.main_server.net_box:call('crud.insert', {
        'customers', tuple, {bucket_id = bucket_id}
    })

    t.assert_equals(err, nil)
    t.assert_not_equals(result, nil)
    t.assert_equals(#result.rows, 1)

    local conditions = {{'==', 'id', tuple[1]}}

    -- select w/ default bucket_id
    local result, err = g.cluster.main_server.net_box:call('crud.select', {
        'customers', conditions,
    })

    t.assert_equals(err, nil)
    t.assert_not_equals(result, nil)
    -- tuple not found
    t.assert_equals(#result.rows, 0)

    -- select w/ right bucket_id
    local result, err = g.cluster.main_server.net_box:call('crud.select', {
        'customers', conditions, {bucket_id = bucket_id},
    })

    t.assert_equals(err, nil)
    t.assert_not_equals(result, nil)
    -- tuple is found
    t.assert_equals(#result.rows, 1)
end
