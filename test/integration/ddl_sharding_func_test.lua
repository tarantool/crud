local fio = require('fio')
local crud = require('crud')
local t = require('luatest')

local helpers = require('test.helper')

local ok = pcall(require, 'ddl')
if not ok then
    t.skip('Lua module ddl is required to run test')
end

local pgroup = t.group('ddl_sharding_func', {
    {engine = 'memtx', space_name = 'customers_G_func'},
    {engine = 'memtx', space_name = 'customers_body_func'},
    {engine = 'vinyl', space_name = 'customers_G_func'},
    {engine = 'vinyl', space_name = 'customers_body_func'},
})

local cache_group = t.group('ddl_sharding_func_cache', {
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
end)

pgroup.after_all(function(g) helpers.stop_cluster(g.cluster) end)

pgroup.before_each(function(g)
    helpers.truncate_space_on_cluster(g.cluster, 'customers_G_func')
    helpers.truncate_space_on_cluster(g.cluster, 'customers_body_func')
end)

cache_group.before_all(function(g)
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
end)

cache_group.after_all(function(g) helpers.stop_cluster(g.cluster) end)

cache_group.before_each(function(g)
    helpers.truncate_space_on_cluster(g.cluster, 'customers_G_func')
    helpers.truncate_space_on_cluster(g.cluster, 'customers_body_func')
end)

pgroup.test_insert_object = function(g)
    local result, err = g.cluster.main_server.net_box:call(
            'crud.insert_object', {g.params.space_name, {id = 158, name = 'Augustus', age = 48}})
    t.assert_equals(err, nil)

    t.assert_equals(result.metadata, {
        {is_nullable = false, name = 'id', type = 'unsigned'},
        {is_nullable = false, name = 'bucket_id', type = 'unsigned'},
        {is_nullable = false, name = 'name', type = 'string'},
        {is_nullable = false, name = 'age', type = 'number'},
    })
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, {{id = 158, bucket_id = 8, name = 'Augustus', age = 48}})

    local conn_s1 = g.cluster:server('s1-master').net_box
    -- There is no tuple on s1 that we inserted before using crud.insert_object().
    local result = conn_s1.space[g.params.space_name]:get({158, 'Augustus'})
    t.assert_equals(result, nil)

    local conn_s2 = g.cluster:server('s2-master').net_box
    -- There is a tuple on s2 that we inserted before using crud.insert_object().
    local result = conn_s2.space[g.params.space_name]:get({158, 'Augustus'})
    t.assert_equals(result, {158, 8, 'Augustus', 48})
end

pgroup.test_insert = function(g)
    -- Insert a tuple.
    local result, err = g.cluster.main_server.net_box:call(
            'crud.insert', {g.params.space_name, {27, box.NULL, 'Ivan', 25}})
    t.assert_equals(err, nil)
    t.assert_equals(result.metadata, {
        {is_nullable = false, name = 'id', type = 'unsigned'},
        {is_nullable = false, name = 'bucket_id', type = 'unsigned'},
        {is_nullable = false, name = 'name', type = 'string'},
        {is_nullable = false, name = 'age', type = 'number'},
    })
    t.assert_equals(#result.rows, 1)
    t.assert_equals(result.rows[1], {27, 7, 'Ivan', 25})

    -- There is a tuple on s2 that we inserted before using crud.insert().
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space[g.params.space_name]:get({27, 'Ivan'})
    t.assert_equals(result, {27, 7, 'Ivan', 25})

    -- There is no tuple on s1 that we inserted before using crud.insert().
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space[g.params.space_name]:get({27, 'Ivan'})
    t.assert_equals(result, nil)
end

pgroup.test_replace_object = function(g)
    -- bucket_id is 596, storage is s-2
    local tuple = {8, 596, 'Dimitrion', 20}

    -- Put tuple to s1 replicaset.
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space[g.params.space_name]:insert(tuple)
    t.assert_not_equals(result, nil)

    -- Put tuple to s2 replicaset.
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space[g.params.space_name]:insert(tuple)
    t.assert_not_equals(result, nil)

    -- Replace an object.
    local result, err = g.cluster.main_server.net_box:call(
            'crud.replace_object', {g.params.space_name, {id = 8, name = 'John Doe', age = 25}})
    t.assert_equals(err, nil)
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, {{id = 8, bucket_id = 8, name = 'John Doe', age = 25}})

    -- There is no replaced tuple on s1 replicaset.
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space[g.params.space_name]:get({8, 'John Doe'})
    t.assert_equals(result, nil)

    -- There is replaced tuple on s2 replicaset.
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space[g.params.space_name]:get({8, 'John Doe'})
    t.assert_equals(result, {8, 8, 'John Doe', 25})
end

pgroup.test_replace = function(g)
    -- bucket_id is 596, storage is s-2
    local tuple = {71, 596, 'Dimitrion', 20}

    -- Put tuple to s1 replicaset.
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space[g.params.space_name]:insert(tuple)
    t.assert_not_equals(result, nil)

    -- Put tuple to s2 replicaset.
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space[g.params.space_name]:insert(tuple)
    t.assert_not_equals(result, nil)

    local tuple = {71, box.NULL, 'Augustus', 21}

    -- Replace a tuple.
    local result, err = g.cluster.main_server.net_box:call('crud.replace', {
        g.params.space_name, tuple
    })
    t.assert_equals(err, nil)
    t.assert_not_equals(result, nil)
    t.assert_equals(#result.rows, 1)
    t.assert_equals(result.rows[1], {71, 1, 'Augustus', 21})

    -- There is no replaced tuple on s1 replicaset.
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space[g.params.space_name]:get({71, 'Augustus'})
    t.assert_equals(result, nil)

    -- There is replaced tuple on s2 replicaset.
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space[g.params.space_name]:get({71, 'Augustus'})
    t.assert_equals(result, {71, 1, 'Augustus', 21})
end

pgroup.test_upsert_object = function(g)
    -- Upsert an object first time.
    local result, err = g.cluster.main_server.net_box:call(
        'crud.upsert_object',
        {g.params.space_name, {id = 66, name = 'Jack Sparrow', age = 25}, {{'+', 'age', 26}}}
    )
    t.assert_equals(#result.rows, 0)
    t.assert_equals(result.metadata, {
        {is_nullable = false, name = 'id', type = 'unsigned'},
        {is_nullable = false, name = 'bucket_id', type = 'unsigned'},
        {is_nullable = false, name = 'name', type = 'string'},
        {is_nullable = false, name = 'age', type = 'number'},
    })
    t.assert_equals(err, nil)

    -- There is no tuple on s1 replicaset.
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space[g.params.space_name]:get({66, 'Jack Sparrow'})
    t.assert_equals(result, nil)

    -- There is a tuple on s2 replicaset.
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space[g.params.space_name]:get({66, 'Jack Sparrow'})
    t.assert_equals(result, {66, 6, 'Jack Sparrow', 25})

    -- Upsert the same query second time when tuple exists.
    local result, err = g.cluster.main_server.net_box:call(
        'crud.upsert_object',
        {g.params.space_name, {id = 66, name = 'Jack Sparrow', age = 25}, {{'+', 'age', 26}}}
    )
    t.assert_equals(#result.rows, 0)
    t.assert_equals(err, nil)

    -- There is no tuple on s2 replicaset.
    local result = conn_s1.space[g.params.space_name]:get({66, 'Jack Sparrow'})
    t.assert_equals(result, nil)

    -- There is an updated tuple on s1 replicaset.
    local result = conn_s2.space[g.params.space_name]:get({66, 'Jack Sparrow'})
    t.assert_equals(result, {66, 6, 'Jack Sparrow', 51})
end

pgroup.test_upsert = function(g)
    local tuple = {14, box.NULL, 'John', 25}

    -- Upsert an object first time.
    local result, err = g.cluster.main_server.net_box:call('crud.upsert', {
        g.params.space_name, tuple, {}
    })
    t.assert_equals(err, nil)
    t.assert_not_equals(result, nil)
    t.assert_equals(#result.rows, 0)

    -- There is no tuple on s2 replicaset.
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space[g.params.space_name]:get({14, 'John'})
    t.assert_equals(result, nil)

    -- There is a tuple on s1 replicaset.
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space[g.params.space_name]:get({14, 'John'})
    t.assert_equals(result, {14, 4, 'John', 25})

    -- Upsert the same query second time when tuple exists.
    local result, err = g.cluster.main_server.net_box:call(
        'crud.upsert_object',
        {g.params.space_name, {id = 14, name = 'John', age = 25}, {{'+', 'age', 26}}}
    )
    t.assert_equals(#result.rows, 0)
    t.assert_equals(err, nil)

    -- There is no tuple on s2 replicaset.
    local result = conn_s1.space[g.params.space_name]:get({14, 'John'})
    t.assert_equals(result, nil)

    -- There is an updated tuple on s1 replicaset.
    local result = conn_s2.space[g.params.space_name]:get({14, 'John'})
    t.assert_equals(result, {14, 4, 'John', 51})
end

pgroup.test_select = function(g)
    -- bucket_id is id % 10 = 8
    local tuple = {18, 8, 'Ptolemy', 25}

    -- Put tuple to s2 replicaset.
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space[g.params.space_name]:insert(tuple)
    t.assert_not_equals(result, nil)

    local conditions = {{'==', 'id', 18}}
    local result, err = g.cluster.main_server.net_box:call('crud.select', {
        g.params.space_name, conditions,
    })

    t.assert_equals(err, nil)
    t.assert_equals(#result.rows, 1)
    t.assert_equals(result.rows[1], tuple)

    -- bucket_id is 2719, storage is s-1
    local tuple = {19, 2719, 'Ptolemy', 25}

    -- Put tuple to s1 replicaset.
    local conn_s2 = g.cluster:server('s1-master').net_box
    local result = conn_s2.space[g.params.space_name]:insert(tuple)
    t.assert_not_equals(result, nil)

    -- calculated bucket_id will be id % 10 = 19 % 10 = 9 ->
    -- select will be performed on s2 replicaset
    -- but tuple is on s1 replicaset -> result will be empty
    local conditions = {{'==', 'id', 19}}
    local result, err = g.cluster.main_server.net_box:call('crud.select', {
        g.params.space_name, conditions,
    })

    t.assert_equals(err, nil)
    t.assert_equals(result.rows, {})
end

pgroup.test_update = function(g)
    -- bucket_id is id % 10 = 2
    local tuple = {12, 2, 'Ivan', 10}

    local conn_s1 = g.cluster:server('s1-master').net_box
    local conn_s2 = g.cluster:server('s2-master').net_box

    -- Put tuple with to s1 replicaset.
    local result = conn_s1.space[g.params.space_name]:insert(tuple)
    t.assert_not_equals(result, nil)

    -- Put tuple with to s2 replicaset.
    local result = conn_s2.space[g.params.space_name]:insert(tuple)
    t.assert_not_equals(result, nil)

    -- Update a tuple.
    local update_operations = {
        {'+', 'age', 10},
    }
    local result, err = g.cluster.main_server.net_box:call('crud.update', {
        g.params.space_name, {12, 'Ivan'}, update_operations,
    })
    t.assert_equals(err, nil)
    t.assert_equals(#result.rows, 1)
    t.assert_equals(result.rows, {{12, 2, 'Ivan', 20}})

    -- Tuple on s1 replicaset was not updated.
    local result = conn_s1.space[g.params.space_name]:get({12, 'Ivan'})
    t.assert_equals(result, {12, 2, 'Ivan', 10})

    -- Tuple on s2 replicaset was updated.
    local result = conn_s2.space[g.params.space_name]:get({12, 'Ivan'})
    t.assert_equals(result, {12, 2, 'Ivan', 20})

    -- bucket_id is 2719, storage is s-1
    local tuple = {18, 2719, 'Ivan', 10}

    -- Put tuple with to s1 replicaset.
    local result = conn_s1.space[g.params.space_name]:insert(tuple)
    t.assert_not_equals(result, nil)

    -- Update a tuple.
    local update_operations = {
        {'+', 'age', 10},
    }
    -- calculated bucket_id will be id % 10 = 18 % 10 = 8 ->
    -- select will be performed on s2 replicaset
    -- but tuple is on s1 replicaset -> result will be empty
    local result, err = g.cluster.main_server.net_box:call('crud.update', {
        g.params.space_name, {18, 'Ivan'}, update_operations,
    })
    t.assert_equals(err, nil)
    t.assert_equals(result.rows, {})

    -- Tuple on s1 replicaset was not updated.
    local result = conn_s1.space[g.params.space_name]:get({18, 'Ivan'})
    t.assert_equals(result, {18, 2719, 'Ivan', 10})
end

pgroup.test_get = function(g)
    -- bucket_id is id % 10 = 2
    local tuple = {12, 2, 'Ivan', 20}

    -- Put tuple to s2 replicaset.
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space[g.params.space_name]:insert(tuple)
    t.assert_not_equals(result, nil)

    -- Get a tuple.
    local result, err = g.cluster.main_server.net_box:call('crud.get', {
        g.params.space_name, {12, 'Ivan'},
    })
    t.assert_equals(err, nil)
    t.assert_equals(result.rows, {{12, 2, 'Ivan', 20}})

    -- bucket_id is 2719, storage is s-1
    local tuple = {18, 2719, 'Ivan', 10}

    -- Put tuple to s1 replicaset.
    local conn_s2 = g.cluster:server('s1-master').net_box
    local result = conn_s2.space[g.params.space_name]:insert(tuple)
    t.assert_not_equals(result, nil)

    -- calculated bucket_id will be id % 10 = 18 % 10 = 8 ->
    -- select will be performed on s2 replicaset
    -- but tuple is on s1 replicaset -> result will be empty
    local result, err = g.cluster.main_server.net_box:call('crud.get', {
        g.params.space_name, {18, 'Ivan'},
    })
    t.assert_equals(err, nil)
    t.assert_equals(result.rows, {})
end

pgroup.test_delete = function(g)
    -- bucket_id is id % 10 = 2
    local tuple = {12, 2, 'Ivan', 20}

    local conn_s1 = g.cluster:server('s1-master').net_box
    local conn_s2 = g.cluster:server('s2-master').net_box

    -- Put tuple to s1 replicaset.
    local result = conn_s1.space[g.params.space_name]:insert(tuple)
    t.assert_not_equals(result, nil)

    -- Put tuple to s2 replicaset.
    local result = conn_s2.space[g.params.space_name]:insert(tuple)
    t.assert_not_equals(result, nil)

    -- Delete tuple.
    local _, err = g.cluster.main_server.net_box:call('crud.delete', {
        g.params.space_name, {12, 'Ivan'},
    })
    t.assert_equals(err, nil)

    -- There is a tuple on s1 replicaset.
    local result = conn_s1.space[g.params.space_name]:get({12, 'Ivan'})
    t.assert_equals(result, {12, 2, 'Ivan', 20})

    -- There is no tuple on s2 replicaset.
    local result = conn_s2.space[g.params.space_name]:get({12, 'Ivan'})
    t.assert_equals(result, nil)

    -- bucket_id is 2719, storage is s-1
    local tuple = {18, 2719, 'Ivan', 20}

    -- Put tuple with to s1 replicaset.
    local result = conn_s1.space[g.params.space_name]:insert(tuple)
    t.assert_not_equals(result, nil)

    -- calculated bucket_id will be id % 10 = 18 % 10 = 8 ->
    -- select will be performed on s2 replicaset
    -- but tuple is on s1 replicaset -> result will be empty
    local _, err = g.cluster.main_server.net_box:call('crud.delete', {
        g.params.space_name, {18, 'Ivan'}
    })
    t.assert_equals(err, nil)

    -- Tuple on s1 replicaset was not deleted.
    local result = conn_s1.space[g.params.space_name]:get({18, 'Ivan'})
    t.assert_equals(result, {18, 2719, 'Ivan', 20})
end

pgroup.test_count = function(g)
    -- bucket_id is id % 10 = 8
    local tuple = {18, 8, 'Ptolemy', 25}

    -- Put tuple to s2 replicaset.
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space[g.params.space_name]:insert(tuple)
    t.assert_not_equals(result, nil)

    local conditions = {{'==', 'id', 18}}
    local result, err = g.cluster.main_server.net_box:call('crud.count', {
        g.params.space_name, conditions,
    })

    t.assert_equals(err, nil)
    t.assert_equals(result, 1)

    -- bucket_id is 2719, storage is s-1
    local tuple = {19, 2719, 'Ptolemy', 25}

    -- Put tuple to s1 replicaset.
    local conn_s2 = g.cluster:server('s1-master').net_box
    local result = conn_s2.space[g.params.space_name]:insert(tuple)
    t.assert_not_equals(result, nil)

    -- calculated bucket_id will be id % 10 = 19 % 10 = 9 ->
    -- select will be performed on s2 replicaset
    -- but tuple is on s1 replicaset -> result will be empty ->
    -- count = 0
    local conditions = {{'==', 'id', 19}}
    local result, err = g.cluster.main_server.net_box:call('crud.count', {
        g.params.space_name, conditions,
    })

    t.assert_equals(err, nil)
    t.assert_equals(result, 0)
end

cache_group.test_update_cache_with_incorrect_func = function(g)
    local fieldno_sharding_func_name = 2

    -- get data from cache for space with correct sharding func
    local space_name = 'customers_G_func'

    local record_exist, err = helpers.update_sharding_func_cache(g.cluster, space_name)
    t.assert_equals(err, nil)
    t.assert_equals(record_exist, true)

    -- records for all spaces exist
    local cache_size = helpers.get_sharding_func_cache_size(g.cluster)
    t.assert_equals(cache_size, 2)

    -- no error just warning
    local space_name = 'customers_G_func'
    helpers.call_on_servers(g.cluster, {'s1-master', 's2-master'}, function(server)
        server.net_box:call('set_sharding_func', {space_name, fieldno_sharding_func_name, 'non_existent_func'})
    end)

    -- we get no error because we sent request for correct space
    local record_exist, err = helpers.update_sharding_func_cache(g.cluster, 'customers_body_func')
    t.assert_equals(err, nil)
    t.assert_equals(record_exist, true)

    -- cache['customers_G_func'] == nil (space with incorrect func)
    -- other records for correct spaces exist in cache
    cache_size = helpers.get_sharding_func_cache_size(g.cluster)
    t.assert_equals(cache_size, 1)

    -- get data from cache for space with incorrect sharding func
    local space_name = 'customers_G_func'
    helpers.call_on_servers(g.cluster, {'s1-master', 's2-master'}, function(server)
        server.net_box:call('set_sharding_func', {space_name, fieldno_sharding_func_name, 'non_existent_func'})
    end)

    -- we get an error because we sent request for incorrect space
    local record_exist, err = helpers.update_sharding_func_cache(g.cluster, space_name)
    t.assert_equals(record_exist, false)
    t.assert_str_contains(err.err,
            "Wrong sharding function specified in _ddl_sharding_func space for (customers_G_func) space")

    -- cache['customers_G_func'] == nil (space with incorrect func)
    -- other records for correct spaces exist in cache
    cache_size = helpers.get_sharding_func_cache_size(g.cluster)
    t.assert_equals(cache_size, 1)
end


local known_bucket_id_key = {1, 'Emma'}
local known_bucket_id_tuple = {
    known_bucket_id_key[1],
    box.NULL,
    known_bucket_id_key[2],
    22
}
local known_bucket_id_object = {
    id = known_bucket_id_key[1],
    bucket_id = box.NULL,
    name = known_bucket_id_key[2],
    age = 22
}
local known_bucket_id = 1111
local known_bucket_id_result_tuple = {
    known_bucket_id_key[1],
    known_bucket_id,
    known_bucket_id_key[2],
    22
}
local known_bucket_id_result = {
    s1 = nil,
    s2 = known_bucket_id_result_tuple,
}
local known_bucket_id_update = {{'+', 'age', 1}}
local known_bucket_id_updated_result = {
    s1 = nil,
    s2 = {known_bucket_id_key[1], known_bucket_id, known_bucket_id_key[2], 23},
}
local prepare_known_bucket_id_data = function(g)
    if known_bucket_id_result.s1 ~= nil then
        local conn_s1 = g.cluster:server('s1-master').net_box
        local result = conn_s1.space[g.params.space_name]:insert(known_bucket_id_result.s1)
        t.assert_equals(result, known_bucket_id_result.s1)
    end

    if known_bucket_id_result.s2 ~= nil then
        local conn_s2 = g.cluster:server('s2-master').net_box
        local result = conn_s2.space[g.params.space_name]:insert(known_bucket_id_result.s2)
        t.assert_equals(result, known_bucket_id_result.s2)
    end
end

local known_bucket_id_write_cases = {
    insert = {
        func = 'crud.insert',
        input_2 = known_bucket_id_tuple,
        input_3 = {bucket_id = known_bucket_id},
        result = known_bucket_id_result,
    },
    insert_object = {
        func = 'crud.insert_object',
        input_2 = known_bucket_id_object,
        input_3 = {bucket_id = known_bucket_id},
        result = known_bucket_id_result,
    },
    replace = {
        func = 'crud.replace',
        input_2 = known_bucket_id_tuple,
        input_3 = {bucket_id = known_bucket_id},
        result = known_bucket_id_result,
    },
    replace_object = {
        func = 'crud.replace_object',
        input_2 = known_bucket_id_object,
        input_3 = {bucket_id = known_bucket_id},
        result = known_bucket_id_result,
    },
    upsert = {
        func = 'crud.upsert',
        input_2 = known_bucket_id_tuple,
        input_3 = {},
        input_4 = {bucket_id = known_bucket_id},
        result = known_bucket_id_result,
    },
    upsert_object = {
        func = 'crud.upsert_object',
        input_2 = known_bucket_id_object,
        input_3 = {},
        input_4 = {bucket_id = known_bucket_id},
        result = known_bucket_id_result,
    },
    update = {
        before_test = prepare_known_bucket_id_data,
        func = 'crud.update',
        input_2 = known_bucket_id_key,
        input_3 = known_bucket_id_update,
        input_4 = {bucket_id = known_bucket_id},
        result = known_bucket_id_updated_result,
    },
    delete = {
        before_test = prepare_known_bucket_id_data,
        func = 'crud.delete',
        input_2 = known_bucket_id_key,
        input_3 = {bucket_id = known_bucket_id},
        result = {},
    },
}

for name, case in pairs(known_bucket_id_write_cases) do
    local test_name = ('test_gh_278_%s_with_explicit_bucket_id_and_ddl'):format(name)

    if case.before_test ~= nil then
        pgroup.before_test(test_name, case.before_test)
    end

    pgroup[test_name] = function(g)
        local obj, err = g.cluster.main_server.net_box:call(
            case.func, {
                g.params.space_name,
                case.input_2,
                case.input_3,
                case.input_4,
            })
        t.assert_equals(err, nil)
        t.assert_is_not(obj, nil)

        local conn_s1 = g.cluster:server('s1-master').net_box
        local result = conn_s1.space[g.params.space_name]:get(known_bucket_id_key)
        t.assert_equals(result, case.result.s1)

        local conn_s2 = g.cluster:server('s2-master').net_box
        local result = conn_s2.space[g.params.space_name]:get(known_bucket_id_key)
        t.assert_equals(result, case.result.s2)
    end
end

local known_bucket_id_read_cases = {
    get = {
        func = 'crud.get',
        input_2 = known_bucket_id_key,
        input_3 = {bucket_id = known_bucket_id},
    },
    select = {
        func = 'crud.select',
        input_2 = {{ '==', 'id', known_bucket_id_key}},
        input_3 = {bucket_id = known_bucket_id},
    },
}

for name, case in pairs(known_bucket_id_read_cases) do
    local test_name = ('test_gh_278_%s_with_explicit_bucket_id_and_ddl'):format(name)

    pgroup.before_test(test_name, prepare_known_bucket_id_data)

    pgroup[test_name] = function(g)
        local obj, err = g.cluster.main_server.net_box:call(
            case.func, {
                g.params.space_name,
                case.input_2,
                case.input_3,
            })
        t.assert_equals(err, nil)
        t.assert_is_not(obj, nil)
        t.assert_equals(obj.rows, {known_bucket_id_result_tuple})
    end
end

pgroup.before_test(
    'test_gh_278_pairs_with_explicit_bucket_id_and_ddl',
    prepare_known_bucket_id_data)

pgroup.test_gh_278_pairs_with_explicit_bucket_id_and_ddl = function(g)
    local obj, err = g.cluster.main_server.net_box:eval([[
        local res = {}
        for _, row in crud.pairs(...) do
            table.insert(res, row)
        end

        return res
    ]], {
        g.params.space_name,
        {{ '==', 'id', known_bucket_id_key}},
        {bucket_id = known_bucket_id}
    })

    t.assert_equals(err, nil)
    t.assert_is_not(obj, nil)
    t.assert_equals(obj, {known_bucket_id_result_tuple})
end

pgroup.before_test(
    'test_gh_278_count_with_explicit_bucket_id_and_ddl',
    prepare_known_bucket_id_data)

pgroup.test_gh_278_count_with_explicit_bucket_id_and_ddl = function(g)
    local obj, err = g.cluster.main_server.net_box:call(
        'crud.count',
        {
            g.params.space_name,
            {{ '==', 'id', known_bucket_id_key}},
            {bucket_id = known_bucket_id}
        })

    t.assert_equals(err, nil)
    t.assert_is_not(obj, nil)
    t.assert_equals(obj, 1)
end
