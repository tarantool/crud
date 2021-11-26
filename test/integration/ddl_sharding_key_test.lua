local fio = require('fio')
local crud = require('crud')
local t = require('luatest')

local helpers = require('test.helper')
local storage_stat = require('test.helpers.storage_stat')

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
    helpers.truncate_space_on_cluster(g.cluster, 'customers_name_key_uniq_index')
    helpers.truncate_space_on_cluster(g.cluster, 'customers_name_key_non_uniq_index')
    helpers.truncate_space_on_cluster(g.cluster, 'customers_secondary_idx_name_key')
    helpers.truncate_space_on_cluster(g.cluster, 'customers_age_key')
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

-- The main purpose of testcase is to verify that CRUD will calculate bucket_id
-- using secondary sharding key (name) correctly and will get tuple on storage
-- in replicaset s2.
-- bucket_id was calculated using function below:
--     function(key)
--         return require('vshard.hash').strcrc32(key) % 3000 + 1
--     end
-- where 3000 is a default number of buckets used in vshard.
pgroup.test_select = function(g)
    -- bucket_id is 234, storage is s-2
    local tuple = {8, 234, 'Ptolemy', 20}

    -- Put tuple to s2 replicaset.
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers_name_key']:insert(tuple)
    t.assert_not_equals(result, nil)

    local conditions = {{'==', 'name', 'Ptolemy'}}
    local result, err = g.cluster.main_server.net_box:call('crud.select', {
        'customers_name_key', conditions,
    })

    t.assert_equals(err, nil)
    t.assert_equals(#result.rows, 1)
    t.assert_equals(result.rows[1], tuple)
end

-- TODO: After enabling support of sharding keys that are not equal to primary
-- keys, we should handle it differently: it is not enough to look just on scan
-- value, we should traverse all conditions. Now missed cases lead to
-- map-reduce. Will be resolved in #213.
pgroup.test_select_wont_lead_map_reduce = function(g)
    local space_name = 'customers_name_key_uniq_index'

    local conn_s1 = g.cluster:server('s1-master').net_box
    local conn_s2 = g.cluster:server('s2-master').net_box

    -- bucket_id is 477, storage is s-2
    local result = conn_s2.space[space_name]:insert({1, 477, 'Viktor Pelevin', 58})
    t.assert_not_equals(result, nil)
    -- bucket_id is 401, storage is s-1
    local result = conn_s1.space[space_name]:insert({2, 401, 'Isaac Asimov', 72})
    t.assert_not_equals(result, nil)
    -- bucket_id is 2804, storage is s-2
    local result = conn_s2.space[space_name]:insert({3, 2804, 'Aleksandr Solzhenitsyn', 89})
    t.assert_not_equals(result, nil)
    -- bucket_id is 1161, storage is s-2
    local result = conn_s2.space[space_name]:insert({4, 1161, 'James Joyce', 59})
    t.assert_not_equals(result, nil)

    local stat_a = storage_stat.collect(g.cluster)

    -- Select a tuple with name 'Viktor Pelevin'.
    local result, err = g.cluster.main_server.net_box:call('crud.select', {
        space_name, {{'==', 'name', 'Viktor Pelevin'}}
    })
    t.assert_equals(err, nil)
    t.assert_not_equals(result, nil)
    t.assert_equals(#result.rows, 1)

    local stat_b = storage_stat.collect(g.cluster)

    -- Check a number of select() requests made by CRUD on cluster's storages
    -- after calling select() on a router. Make sure only a single storage has
    -- a single select() request. Otherwise we lead map-reduce.
    t.assert_equals(storage_stat.diff(stat_b, stat_a), {
        ['s-1'] = {
            select_requests = 0,
        },
        ['s-2'] = {
            select_requests = 1,
        },
    })
end

pgroup.test_select_secondary_idx = function(g)
    local tuple = {2, box.NULL, 'Ivan', 20}

    -- insert tuple
    local result, err = g.cluster.main_server.net_box:call('crud.insert', {
        'customers_secondary_idx_name_key', tuple
    })

    t.assert_equals(err, nil)
    t.assert_not_equals(result, nil)
    t.assert_equals(#result.rows, 1)

    local conditions = {{'==', 'name', 'Ivan'}}

    local result, err = g.cluster.main_server.net_box:call('crud.select', {
        'customers_secondary_idx_name_key', conditions,
    })

    t.assert_equals(err, nil)
    t.assert_equals(#result.rows, 1)
    t.assert_equals(result.rows[1], {2, 1366, 'Ivan', 20})
end

pgroup.test_select_non_unique_index = function(g)
    local space_name = 'customers_name_key_non_uniq_index'
    local customers = helpers.insert_objects(g, space_name, {
        {id = 1, name = 'Viktor Pelevin', age = 58},
        {id = 2, name = 'Isaac Asimov', age = 72},
        {id = 3, name = 'Aleksandr Solzhenitsyn', age = 89},
        {id = 4, name = 'James Joyce', age = 59},
        {id = 5, name = 'Oscar Wilde', age = 46},
        -- First tuple with name 'Ivan Bunin'.
        {id = 6, name = 'Ivan Bunin', age = 83},
        {id = 7, name = 'Ivan Turgenev', age = 64},
        {id = 8, name = 'Alexander Ostrovsky', age = 63},
        {id = 9, name = 'Anton Chekhov', age = 44},
        -- Second tuple with name 'Ivan Bunin'.
        {id = 10, name = 'Ivan Bunin', age = 83},
    })
    t.assert_equals(#customers, 10)

    local result, err = g.cluster.main_server.net_box:call('crud.select', {
        space_name, {{'==', 'name', 'Ivan Bunin'}}
    })
    t.assert_equals(err, nil)
    t.assert_not_equals(result, nil)
    t.assert_equals(#result.rows, 2)
end

pgroup.test_update = function(g)
    -- bucket_id is 1366, storage is s-2
    local tuple = {2, 1366, 'Ivan', 10}

    local conn_s1 = g.cluster:server('s1-master').net_box
    local conn_s2 = g.cluster:server('s2-master').net_box

    -- Put tuple with to s1 replicaset.
    local result = conn_s1.space['customers_name_key']:insert(tuple)
    t.assert_not_equals(result, nil)

    -- Put tuple with to s2 replicaset.
    local result = conn_s2.space['customers_name_key']:insert(tuple)
    t.assert_not_equals(result, nil)

    -- Update a tuple.
    local update_operations = {
        {'+', 'age', 10},
    }
    local result, err = g.cluster.main_server.net_box:call('crud.update', {
        'customers_name_key', {2, 'Ivan'}, update_operations,
    })
    t.assert_equals(err, nil)
    t.assert_equals(#result.rows, 1)
    t.assert_equals(result.rows, {{2, 1366, 'Ivan', 20}})

    -- Tuple on s1 replicaset was not updated.
    local result = conn_s1.space['customers_name_key']:get({2, 'Ivan'})
    t.assert_equals(result, {2, 1366, 'Ivan', 10})

    -- Tuple on s2 replicaset was updated.
    local result = conn_s2.space['customers_name_key']:get({2, 'Ivan'})
    t.assert_equals(result, {2, 1366, 'Ivan', 20})
end

pgroup.test_get = function(g)
    -- bucket_id is 596, storage is s-2
    local tuple = {7, 596, 'Dimitrion', 20}

    -- Put tuple to s2 replicaset.
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers_name_key']:insert(tuple)
    t.assert_not_equals(result, nil)

    -- Get a tuple.
    local result, err = g.cluster.main_server.net_box:call('crud.get', {
        'customers_name_key', {7, 'Dimitrion'},
    })
    t.assert_equals(err, nil)
    t.assert_equals(result.rows, {{7, 596, 'Dimitrion', 20}})
end

pgroup.test_delete = function(g)
    -- bucket_id is 596, storage is s-2
    local tuple = {7, 596, 'Dimitrion', 20}

    local conn_s1 = g.cluster:server('s1-master').net_box
    local conn_s2 = g.cluster:server('s2-master').net_box

    -- Put tuple to s1 replicaset.
    local result = conn_s1.space['customers_name_key']:insert(tuple)
    t.assert_not_equals(result, nil)

    -- Put tuple to s2 replicaset.
    local result = conn_s2.space['customers_name_key']:insert(tuple)
    t.assert_not_equals(result, nil)

    -- Delete tuple.
    local _, err = g.cluster.main_server.net_box:call('crud.delete', {
        'customers_name_key', {7, 'Dimitrion'},
    })
    t.assert_equals(err, nil)

    -- There is a tuple on s1 replicaset.
    local result = conn_s1.space['customers_name_key']:get({7, 'Dimitrion'})
    t.assert_equals(result, {7, 596, 'Dimitrion', 20})

    -- There is no tuple on s2 replicaset.
    local result = conn_s2.space['customers_name_key']:get({7, 'Dimitrion'})
    t.assert_equals(result, nil)
end

pgroup.test_delete_incomplete_sharding_key = function(g)
    local tuple = {2, box.NULL, 'Viktor Pelevin', 58}

    -- insert tuple
    local result, err = g.cluster.main_server.net_box:call('crud.insert', {
        'customers_age_key', tuple
    })

    t.assert_equals(err, nil)
    t.assert_not_equals(result, nil)
    t.assert_equals(#result.rows, 1)

    local result, err = g.cluster.main_server.net_box:call('crud.delete', {
        'customers_age_key', {58, 'Viktor Pelevin'}
    })

    t.assert_str_contains(err.err,
        "Sharding key for space \"customers_age_key\" is missed in primary index, specify bucket_id")
    t.assert_equals(result, nil)
end

pgroup.test_get_incomplete_sharding_key = function(g)
    local tuple = {2, box.NULL, 'Viktor Pelevin', 58}

    -- insert tuple
    local result, err = g.cluster.main_server.net_box:call('crud.insert', {
        'customers_age_key', tuple
    })

    t.assert_equals(err, nil)
    t.assert_not_equals(result, nil)
    t.assert_equals(#result.rows, 1)

    local result, err = g.cluster.main_server.net_box:call('crud.get', {
        'customers_age_key', {58, 'Viktor Pelevin'}
    })

    t.assert_str_contains(err.err,
        "Sharding key for space \"customers_age_key\" is missed in primary index, specify bucket_id")
    t.assert_equals(result, nil)
end

pgroup.test_update_incomplete_sharding_key = function(g)
    local tuple = {2, box.NULL, 'Viktor Pelevin', 58}

    -- insert tuple
    local result, err = g.cluster.main_server.net_box:call('crud.insert', {
        'customers_age_key', tuple
    })

    t.assert_equals(err, nil)
    t.assert_not_equals(result, nil)
    t.assert_equals(#result.rows, 1)

    local update_operations = {
        {'=', 'age', 60},
    }

    local result, err = g.cluster.main_server.net_box:call('crud.update', {
        'customers_age_key', {2, 'Viktor Pelevin'}, update_operations,
    })

    t.assert_str_contains(err.err,
        "Sharding key for space \"customers_age_key\" is missed in primary index, specify bucket_id")
    t.assert_equals(result, nil)
end

pgroup.test_get_secondary_idx = function(g)
    local tuple = {4, box.NULL, 'Leo', 44}

    -- insert tuple
    local result, err = g.cluster.main_server.net_box:call('crud.insert', {
        'customers_secondary_idx_name_key', tuple
    })

    t.assert_equals(err, nil)
    t.assert_not_equals(result, nil)
    t.assert_equals(#result.rows, 1)

    -- get
    local result, err = g.cluster.main_server.net_box:call('crud.get',
        {'customers_secondary_idx_name_key', {4, 'Leo'}})

    t.assert_str_contains(err.err,
        "Sharding key for space \"customers_secondary_idx_name_key\" is missed in primary index, specify bucket_id")
    t.assert_equals(result, nil)
end

pgroup.test_update_secondary_idx = function(g)
    local tuple = {6, box.NULL, 'Victor', 58}

    -- insert tuple
    local result, err = g.cluster.main_server.net_box:call('crud.insert', {
        'customers_secondary_idx_name_key', tuple
    })

    t.assert_equals(err, nil)
    t.assert_not_equals(result, nil)
    t.assert_equals(#result.rows, 1)

    local update_operations = {
        {'=', 'age', 58},
    }

    local result, err = g.cluster.main_server.net_box:call('crud.update', {
        'customers_secondary_idx_name_key', {6, 'Victor'}, update_operations,
    })

    t.assert_str_contains(err.err,
        "Sharding key for space \"customers_secondary_idx_name_key\" is missed in primary index, specify bucket_id")
    t.assert_equals(result, nil)
end

pgroup.test_delete_secondary_idx = function(g)
    local tuple = {8, box.NULL, 'Alexander', 37}

    -- insert tuple
    local result, err = g.cluster.main_server.net_box:call('crud.insert', {
        'customers_secondary_idx_name_key', tuple
    })

    t.assert_equals(err, nil)
    t.assert_not_equals(result, nil)
    t.assert_equals(#result.rows, 1)

    local result, err = g.cluster.main_server.net_box:call('crud.delete', {
        'customers_secondary_idx_name_key', {8, 'Alexander'}
    })

    t.assert_str_contains(err.err,
        "Sharding key for space \"customers_secondary_idx_name_key\" is missed in primary index, specify bucket_id")
    t.assert_equals(result, nil)
end
