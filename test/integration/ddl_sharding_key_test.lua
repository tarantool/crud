local fio = require('fio')
local crud = require('crud')
local t = require('luatest')

local helpers = require('test.helper')

local ok = pcall(require, 'ddl')
if not ok then
    t.skip('Lua module ddl is required to run test')
end

local pgroup = t.group('ddl_sharding_key', {
    {engine = 'memtx'},
    {engine = 'vinyl'},
})

pgroup.before_all(function(g)
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

    helpers.call_on_storages(g.cluster, function(server)
        server.net_box:eval([[
            local storage_stat = require('test.helpers.storage_stat')
            storage_stat.init_on_storage()
        ]])
    end)
end)

pgroup.after_all(function(g) helpers.stop_cluster(g.cluster) end)

pgroup.before_each(function(g)
    helpers.truncate_space_on_cluster(g.cluster, 'customers_name_key')
end)

pgroup.test_insert_object = function(g)
    local result, err = g.cluster.main_server.net_box:call(
        'crud.insert_object', {'customers_name_key', {id = 1, name = 'Augustus', age = 48}})
    t.assert_equals(err, nil)

    t.assert_equals(result.metadata, {
        {is_nullable = false, name = 'id', type = 'unsigned'},
        {is_nullable = false, name = 'bucket_id', type = 'unsigned'},
        {is_nullable = false, name = 'name', type = 'string'},
        {is_nullable = false, name = 'age', type = 'number'},
    })
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, {{id = 1, bucket_id = 782, name = 'Augustus', age = 48}})

    local conn_s1 = g.cluster:server('s1-master').net_box
    -- There is no tuple on s1 that we inserted before using crud.insert_object().
    local result = conn_s1.space['customers_name_key']:get({1, 'Augustus'})
    t.assert_equals(result, nil)

    local conn_s2 = g.cluster:server('s2-master').net_box
    -- There is a tuple on s2 that we inserted before using crud.insert_object().
    local result = conn_s2.space['customers_name_key']:get({1, 'Augustus'})
    t.assert_equals(result, {1, 782, 'Augustus', 48})

end

pgroup.test_insert = function(g)
    -- Insert a tuple.
    local result, err = g.cluster.main_server.net_box:call(
        'crud.insert', {'customers_name_key', {2, box.NULL, 'Ivan', 20}})
    t.assert_equals(err, nil)
    t.assert_equals(result.metadata, {
        {is_nullable = false, name = 'id', type = 'unsigned'},
        {is_nullable = false, name = 'bucket_id', type = 'unsigned'},
        {is_nullable = false, name = 'name', type = 'string'},
        {is_nullable = false, name = 'age', type = 'number'},
    })
    t.assert_equals(#result.rows, 1)
    t.assert_equals(result.rows[1], {2, 1366, 'Ivan', 20})

    -- There is a tuple on s2 that we inserted before using crud.insert().
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers_name_key']:get({2, 'Ivan'})
    t.assert_equals(result, {2, 1366, 'Ivan', 20})

    -- There is no tuple on s1 that we inserted before using crud.insert().
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers_name_key']:get({2, 'Ivan'})
    t.assert_equals(result, nil)
end

pgroup.test_replace = function(g)
    -- bucket_id is 596, storage is s-2
    local tuple = {7, 596, 'Dimitrion', 20}

    -- Put tuple to s1 replicaset.
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers_name_key']:insert(tuple)
    t.assert_not_equals(result, nil)

    -- Put tuple to s2 replicaset.
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers_name_key']:insert(tuple)
    t.assert_not_equals(result, nil)

    local tuple = {7, box.NULL, 'Augustus', 21}

    -- Replace a tuple.
    local result, err = g.cluster.main_server.net_box:call('crud.replace', {
        'customers_name_key', tuple
    })
    t.assert_equals(err, nil)
    t.assert_not_equals(result, nil)
    t.assert_equals(#result.rows, 1)
    t.assert_equals(result.rows[1], {7, 782, 'Augustus', 21})

    -- There is no replaced tuple on s1 replicaset.
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers_name_key']:get({7, 'Augustus'})
    t.assert_equals(result, nil)

    -- There is replaced tuple on s2 replicaset.
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers_name_key']:get({7, 'Augustus'})
    t.assert_equals(result, {7, 782, 'Augustus', 21})
end

pgroup.test_replace_object = function(g)
    -- bucket_id is 596, storage is s-2
    local tuple = {8, 596, 'Dimitrion', 20}

    -- Put tuple to s1 replicaset.
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers_name_key']:insert(tuple)
    t.assert_not_equals(result, nil)

    -- Put tuple to s2 replicaset.
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers_name_key']:insert(tuple)
    t.assert_not_equals(result, nil)

    -- Replace an object.
    local result, err = g.cluster.main_server.net_box:call(
        'crud.replace_object', {'customers_name_key', {id = 8, name = 'John Doe', age = 25}})
    t.assert_equals(err, nil)
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, {{id = 8, bucket_id = 1035, name = 'John Doe', age = 25}})

    -- There is no replaced tuple on s1 replicaset.
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers_name_key']:get({8, 'John Doe'})
    t.assert_equals(result, nil)

    -- There is replaced tuple on s2 replicaset.
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers_name_key']:get({8, 'John Doe'})
    t.assert_equals(result, {8, 1035, 'John Doe', 25})
end

pgroup.test_upsert_object = function(g)
    -- Upsert an object first time.
    local result, err = g.cluster.main_server.net_box:call(
        'crud.upsert_object', {'customers_name_key', {id = 66, name = 'Jack Sparrow', age = 25}, {
             {'+', 'age', 25},
    }})
    t.assert_equals(#result.rows, 0)
    t.assert_equals(result.metadata, {
        {is_nullable = false, name = 'id', type = 'unsigned'},
        {is_nullable = false, name = 'bucket_id', type = 'unsigned'},
        {is_nullable = false, name = 'name', type = 'string'},
        {is_nullable = false, name = 'age', type = 'number'},
    })
    t.assert_equals(err, nil)

    -- There is a tuple on s1 replicaset.
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers_name_key']:get({66, 'Jack Sparrow'})
    t.assert_equals(result, {66, 2719, 'Jack Sparrow', 25})

    -- There is no tuple on s2 replicaset.
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers_name_key']:get({66, 'Jack Sparrow'})
    t.assert_equals(result, nil)

    -- Upsert the same query second time when tuple exists.
    local result, err = g.cluster.main_server.net_box:call(
       'crud.upsert_object', {'customers_name_key', {id = 66, name = 'Jack Sparrow', age = 25}, {
            {'+', 'age', 25},
    }})
    t.assert_equals(#result.rows, 0)
    t.assert_equals(err, nil)

    -- There is an updated tuple on s1 replicaset.
    local result = conn_s1.space['customers_name_key']:get({66, 'Jack Sparrow'})
    t.assert_equals(result, {66, 2719, 'Jack Sparrow', 50})

    -- There is no tuple on s2 replicaset.
    local result = conn_s2.space['customers_name_key']:get({66, 'Jack Sparrow'})
    t.assert_equals(result, nil)
end

pgroup.test_upsert = function(g)
    local tuple = {1, box.NULL, 'John', 25}

    -- Upsert an object first time.
    local result, err = g.cluster.main_server.net_box:call('crud.upsert', {
        'customers_name_key', tuple, {}
    })
    t.assert_equals(err, nil)
    t.assert_not_equals(result, nil)
    t.assert_equals(#result.rows, 0)

    -- There is a tuple on s1 replicaset.
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers_name_key']:get({1, 'John'})
    t.assert_equals(result, {1, 2699, 'John', 25})

    -- There is no tuple on s2 replicaset.
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers_name_key']:get({1, 'John'})
    t.assert_equals(result, nil)

    -- Upsert the same query second time when tuple exists.
    local result, err = g.cluster.main_server.net_box:call(
       'crud.upsert_object', {'customers_name_key', {id = 1, name = 'John', age = 25}, {
            {'+', 'age', 25},
    }})
    t.assert_equals(#result.rows, 0)
    t.assert_equals(err, nil)

    -- There is an updated tuple on s1 replicaset.
    local result = conn_s1.space['customers_name_key']:get({1, 'John'})
    t.assert_equals(result, {1, 2699, 'John', 50})

    -- There is no tuple on s2 replicaset.
    local result = conn_s2.space['customers_name_key']:get({1, 'John'})
    t.assert_equals(result, nil)
end
