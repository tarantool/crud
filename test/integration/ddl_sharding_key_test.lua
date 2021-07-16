local fio = require('fio')

local t = require('luatest')
local crud_utils = require('crud.common.utils')

local helpers = require('test.helper')

local ok = pcall(require, 'ddl')
if not ok then
    t.skip('Lua module ddl is required to run test')
end

local pgroup = helpers.pgroup.new('ddl_sharding_key', {
    engine = {'memtx', 'vinyl'},
})

pgroup:set_before_all(function(g)
    g.cluster = helpers.Cluster:new({
        datadir = fio.tempdir(),
        server_command = helpers.entrypoint('srv_ddl'),
        use_vshard = true,
        replicasets = helpers.get_test_replicasets(),
        env = {
            ['ENGINE'] = g.params.engine,
        },
    })
    g.cluster:start()
    local result, err = g.cluster.main_server.net_box:eval([[
        local ddl = require('ddl')

        local ok, err = ddl.get_schema()
        return ok, err
    ]])
    t.assert_equals(type(result), 'table')
    t.assert_equals(err, nil)

    g.space_format = g.cluster.servers[2].net_box.space.customers:format()
end)


pgroup:set_after_all(function(g) helpers.stop_cluster(g.cluster) end)

pgroup:set_before_each(function(g)
    helpers.truncate_space_on_cluster(g.cluster, 'customers')
end)

local function get_other_storage_bucket_id(g, key)
    local bucket_id = g.cluster.main_server.net_box:eval([[
        local vshard = require('vshard')

        local key = ...
        return vshard.router.bucket_id_strcrc32(key)
    ]], {key})

    local res_bucket_id, err = helpers.get_other_storage_bucket_id(g.cluster, bucket_id)

    t.assert(res_bucket_id ~= nil, err)
    return res_bucket_id
end

local function check_get(g, space_name, id)
    local result, err = g.cluster.main_server.net_box:call('crud.get', {
        space_name, id,
    })

    t.assert_equals(err, nil)
    t.assert(result ~= nil)
end

local function check_get_with_bucket_id(g, space_name, id, bucket_id, tuple)
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

pgroup:add('test_update', function(g)
    local tuple = {2, box.NULL, 'Ivan', 20}

    local update_operations = {
        {'+', 'age', 10},
        {'=', 'name', 'Leo Tolstoy'},
    }

    -- insert tuple
    local result, err = g.cluster.main_server.net_box:call('crud.insert', {
        'customers', tuple
    })

    t.assert_equals(err, nil)
    t.assert(result ~= nil)
    t.assert_equals(#result.rows, 1)

    local result, err = g.cluster.main_server.net_box:call('crud.update', {
        'customers', tuple[1], update_operations,
    })

    t.assert_equals(err, nil)
    t.assert(result ~= nil)
    t.assert_equals(#result.rows, 1)
end)

pgroup:add('test_delete', function(g)
    local tuple = {2, box.NULL, 'Ivan', 20}

    -- insert tuple
    local result, err = g.cluster.main_server.net_box:call('crud.insert', {
        'customers', tuple
    })

    t.assert_equals(err, nil)
    t.assert(result ~= nil)
    t.assert_equals(#result.rows, 1)

    local result, err = g.cluster.main_server.net_box:call('crud.delete', {
        'customers', tuple[1],
    })

    t.assert_equals(err, nil)
    t.assert(result ~= nil)

    local result, err = g.cluster.main_server.net_box:call('crud.get', {
        'customers', tuple[1]
    })

    t.assert_equals(err, nil)
    t.assert(result ~= nil)
    t.assert_equals(#result.rows, 0)
end)

pgroup:add('test_insert_object', function(g)
    local object = {id = 2, name = 'Ivan', age = 46}
    local bucket_id = get_other_storage_bucket_id(g, object.id)
    object.bucket_id = bucket_id

    local tuple = crud_utils.flatten(object, g.space_format, bucket_id)

    -- insert_object
    local result, err = g.cluster.main_server.net_box:call('crud.insert_object', {
        'customers', object
    })

    t.assert_equals(err, nil)
    t.assert(result ~= nil)
    t.assert_equals(result.rows, {tuple})

    check_get_with_bucket_id(g, 'customers', object.id, bucket_id, tuple)
end)

pgroup:add('test_insert', function(g)
    local tuple = {2, box.NULL, 'Ivan', 20}

    -- insert
    local result, err = g.cluster.main_server.net_box:call('crud.insert', {
        'customers', tuple,
    })

    t.assert_equals(err, nil)
    t.assert(result ~= nil)

    tuple[2] = nil
    t.assert_items_include(result.rows[1], tuple)
    check_get(g, 'customers', 2, tuple)
end)

pgroup:add('test_replace_object', function(g)
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

    check_get_with_bucket_id(g, 'customers', object.id, bucket_id, tuple)
end)

pgroup:add('test_replace', function(g)
    local tuple = {2, box.NULL, 'Jane', 21}

    -- replace
    local result, err = g.cluster.main_server.net_box:call('crud.replace', {
        'customers', tuple
    })

    t.assert_equals(err, nil)
    t.assert(result ~= nil)
    tuple[2] = nil
    t.assert_items_include(result.rows[1], tuple)

    check_get(g, 'customers', tuple[1], tuple)
end)

pgroup:add('test_upsert_object', function(g)
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

    check_get_with_bucket_id(g, 'customers', object.id, bucket_id, tuple)
end)

pgroup:add('test_upsert', function(g)
    local tuple = {1, box.NULL, 'John', 25}

    -- upsert
    local result, err = g.cluster.main_server.net_box:call('crud.upsert', {
        'customers', tuple, {}
    })

    t.assert_equals(err, nil)
    t.assert(result ~= nil)
    t.assert_equals(#result.rows, 0)

    check_get(g, 'customers', tuple[1], tuple)
end)

pgroup:add('test_select', function(g)
    local tuple = {2, box.NULL, 'Ivan', 20}

    -- insert tuple
    local result, err = g.cluster.main_server.net_box:call('crud.insert', {
        'customers', tuple
    })

    t.assert_equals(err, nil)
    t.assert(result ~= nil)
    t.assert_equals(#result.rows, 1)

    local conditions = {{'==', 'id', tuple[1]}}

    local result, err = g.cluster.main_server.net_box:call('crud.select', {
        'customers', conditions,
    })

    t.assert_equals(err, nil)
    t.assert(result ~= nil)
    t.assert_equals(#result.rows, 1)
end)
