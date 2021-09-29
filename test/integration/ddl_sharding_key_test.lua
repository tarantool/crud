local fio = require('fio')
local crud = require('crud')
local crud_utils = require('crud.common.utils')
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
end)

pgroup.after_all(function(g) helpers.stop_cluster(g.cluster) end)

pgroup.before_each(function(g)
    helpers.truncate_space_on_cluster(g.cluster, 'customers_name_key')
    helpers.truncate_space_on_cluster(g.cluster, 'customers_name_key_uniq_index')
    helpers.truncate_space_on_cluster(g.cluster, 'customers_name_key_non_uniq_index')
    helpers.truncate_space_on_cluster(g.cluster, 'customers_secondary_idx_name_key')
    helpers.truncate_space_on_cluster(g.cluster, 'customers_age_key')
    if crud_utils.tarantool_supports_jsonpath_indexes() then
        helpers.truncate_space_on_cluster(g.cluster, 'customers_jsonpath_key')
    end
end)

local function check_get(g, space_name, id, name)
    local result, err = g.cluster.main_server.net_box:call('crud.get', {
        space_name, {id, name}
    })

    t.assert_equals(err, nil)
    t.assert_not_equals(result, nil)
end

pgroup.test_insert_object_get = function(g)
    -- insert_object
    local result, err = g.cluster.main_server.net_box:call(
        'crud.insert_object', {'customers_name_key', {id = 1, name = 'Fedor', age = 59}})

    t.assert_equals(err, nil)
    t.assert_equals(result.metadata, {
        {is_nullable = false, name = 'id', type = 'unsigned'},
        {is_nullable = false, name = 'bucket_id', type = 'unsigned'},
        {is_nullable = false, name = 'name', type = 'string'},
        {is_nullable = false, name = 'age', type = 'number'},
    })
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, {{id = 1, bucket_id = 86, name = 'Fedor', age = 59}})

    -- get
    local result, err = g.cluster.main_server.net_box:call('crud.get', {'customers_name_key', {1, 'Fedor'}})

    t.assert_equals(err, nil)
    t.assert(result ~= nil)
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, {{age = 59, bucket_id = 86, id = 1, name = "Fedor"}})

    -- insert_object again
    local obj, err = g.cluster.main_server.net_box:call(
        'crud.insert_object', {'customers_name_key', {id = 1, name = 'Alexander', age = 37}})

    t.assert_equals(err, nil)
    t.assert_equals(obj.rows, {{1, 1690, "Alexander", 37}})
end

pgroup.test_insert_get = function(g)
    -- insert
    local result, err = g.cluster.main_server.net_box:call(
        'crud.insert', {'customers_name_key', {2, box.NULL, 'Ivan', 20}})

    t.assert_equals(err, nil)
    t.assert_equals(result.metadata, {
        {is_nullable = false, name = 'id', type = 'unsigned'},
        {is_nullable = false, name = 'bucket_id', type = 'unsigned'},
        {is_nullable = false, name = 'name', type = 'string'},
        {is_nullable = false, name = 'age', type = 'number'},
    })
    t.assert_equals(result.rows, {{2, 1366, "Ivan", 20}})

    -- get
    local result, err = g.cluster.main_server.net_box:call('crud.get', {'customers_name_key', {2, 'Ivan'}})

    t.assert_equals(err, nil)
    t.assert_not_equals(result, nil)
    t.assert_equals(result.rows, {{2, 1366, "Ivan", 20}})
end

pgroup.test_update = function(g)
    local tuple = {2, box.NULL, 'Ivan', 20}

    local update_operations = {
        {'+', 'age', 10},
    }

    -- insert tuple
    local result, err = g.cluster.main_server.net_box:call('crud.insert', {
        'customers_name_key', tuple
    })

    t.assert_equals(err, nil)
    t.assert_not_equals(result, nil)
    t.assert_equals(#result.rows, 1)

    local result, err = g.cluster.main_server.net_box:call('crud.update', {
        'customers_name_key', {2, 'Ivan'}, update_operations,
    })

    t.assert_equals(err, nil)
    t.assert_not_equals(result, nil)
    t.assert_equals(#result.rows, 1)

    -- get
    local result, err = g.cluster.main_server.net_box:call('crud.get', {'customers_name_key', {2, 'Ivan'}})

    t.assert_equals(err, nil)
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, {{age = 30, bucket_id = 1366, id = 2, name = "Ivan"}})
end

pgroup.test_delete = function(g)
    local tuple = {2, box.NULL, 'Ivan', 20}

    -- insert tuple
    local result, err = g.cluster.main_server.net_box:call('crud.insert', {
        'customers_name_key', tuple
    })

    t.assert_equals(err, nil)
    t.assert_not_equals(result, nil)
    t.assert_equals(#result.rows, 1)

    local result, err = g.cluster.main_server.net_box:call('crud.delete', {
        'customers_name_key', {2, 'Ivan'},
    })

    t.assert_equals(err, nil)
    t.assert_not_equals(result, nil)

    local result, err = g.cluster.main_server.net_box:call('crud.get', {
        'customers_name_key', {2, 'Ivan'}
    })

    t.assert_equals(err, nil)
    t.assert_not_equals(result, nil)
    t.assert_equals(#result.rows, 0)
end

pgroup.test_insert = function(g)
    local tuple = {2, box.NULL, 'Ivan', 20}

    -- insert
    local result, err = g.cluster.main_server.net_box:call('crud.insert', {
        'customers_name_key', tuple,
    })

    t.assert_equals(err, nil)
    t.assert_not_equals(result, nil)
    t.assert_equals(result.rows[1], {2, 1366, "Ivan", 20})

    check_get(g, 'customers_name_key', 2, 'Ivan')
end

pgroup.test_replace = function(g)
    local tuple = {2, box.NULL, 'Jane', 21}

    -- replace
    local result, err = g.cluster.main_server.net_box:call('crud.replace', {
        'customers_name_key', tuple
    })

    t.assert_equals(err, nil)
    t.assert_not_equals(result, nil)
    t.assert_equals(result.rows[1], {2, 1538, "Jane", 21})

    check_get(g, 'customers_name_key', 2, 'Jane')
end

pgroup.test_replace_object = function(g)
    -- get
    local result, err = g.cluster.main_server.net_box:call('crud.get', {'customers_name_key', {44, 'Ivan'}})

    t.assert_equals(err, nil)
    t.assert_equals(result.metadata, {
        {is_nullable = false, name = 'id', type = 'unsigned'},
        {is_nullable = false, name = 'bucket_id', type = 'unsigned'},
        {is_nullable = false, name = 'name', type = 'string'},
        {is_nullable = false, name = 'age', type = 'number'},
    })
    t.assert_equals(#result.rows, 0)

    -- replace_object
    local result, err = g.cluster.main_server.net_box:call(
        'crud.replace_object', {'customers_name_key', {id = 44, name = 'John Doe', age = 25}})

    t.assert_equals(err, nil)
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, {{id = 44, bucket_id = 1035, name = 'John Doe', age = 25}})

    -- replace_object
    local result, err = g.cluster.main_server.net_box:call(
        'crud.replace_object', {'customers_name_key', {id = 44, name = 'Jane Doe', age = 18}})

    t.assert_equals(err, nil)
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, {{id = 44, bucket_id = 2194, name = 'Jane Doe', age = 18}})
end

pgroup.test_upsert_object = function(g)
    -- upsert_object first time
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

    -- get
    local result, err = g.cluster.main_server.net_box:call('crud.get', {'customers_name_key', {66, 'Jack Sparrow'}})

    t.assert_equals(err, nil)
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, {{id = 66, bucket_id = 2719, name = 'Jack Sparrow', age = 25}})

    -- upsert_object the same query second time when tuple exists
    local result, err = g.cluster.main_server.net_box:call(
       'crud.upsert_object', {'customers_name_key', {id = 66, name = 'Jack Sparrow', age = 25}, {
            {'+', 'age', 25},
    }})

    t.assert_equals(#result.rows, 0)
    t.assert_equals(err, nil)

    -- get
    local result, err = g.cluster.main_server.net_box:call('crud.get', {'customers_name_key', {66, 'Jack Sparrow'}})

    t.assert_equals(err, nil)
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, {{age = 50, bucket_id = 2719, id = 66, name = "Jack Sparrow"}})
end

pgroup.test_upsert = function(g)
    local tuple = {1, box.NULL, 'John', 25}

    -- upsert
    local result, err = g.cluster.main_server.net_box:call('crud.upsert', {
        'customers_name_key', tuple, {}
    })

    t.assert_equals(err, nil)
    t.assert_not_equals(result, nil)
    t.assert_equals(#result.rows, 0)

    check_get(g, 'customers_name_key', 1, 'John')
end

pgroup.test_select = function(g)
    local tuple = {2, box.NULL, 'Ivan', 20}

    -- insert tuple
    local result, err = g.cluster.main_server.net_box:call('crud.insert', {
        'customers_name_key', tuple
    })

    t.assert_equals(err, nil)
    t.assert_not_equals(result, nil)
    t.assert_equals(#result.rows, 1)

    local conditions = {{'==', 'id', 2}}

    local result, err = g.cluster.main_server.net_box:call('crud.select', {
        'customers_name_key', conditions,
    })

    t.assert_equals(err, nil)
    t.assert_not_equals(result, nil)
    t.assert_equals(#result.rows, 1)
end

pgroup.test_incomplete_sharding_key_delete = function(g)
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

pgroup.test_incomplete_sharding_key_get = function(g)
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

pgroup.test_incomplete_sharding_key_update = function(g)
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

-- Right now CRUD's plan for select doesn't support sharding key and it leads
-- to map reduce (select on all replicasets). To avoid map-reduce one need to
-- add a separate index by field name, used in select's condition. We plan to
-- fix this in scope of https://github.com/tarantool/crud/issues/213
pgroup.test_select_wont_lead_map_reduce = function(g)
    local space_name = 'customers_name_key_uniq_index'
    local customers = helpers.insert_objects(g, space_name, {
        {id = 1, name = 'Viktor Pelevin', age = 58},
        {id = 2, name = 'Isaac Asimov', age = 72},
        {id = 3, name = 'Aleksandr Solzhenitsyn', age = 89},
        {id = 4, name = 'James Joyce', age = 59},
        {id = 5, name = 'Oscar Wilde', age = 46},
        {id = 6, name = 'Ivan Bunin', age = 83},
        {id = 7, name = 'Ivan Turgenev', age = 64},
        {id = 8, name = 'Alexander Ostrovsky', age = 63},
        {id = 9, name = 'Anton Chekhov', age = 44},
    })
    t.assert_equals(#customers, 9)

    -- Disable vshard's rebalancer and account current statistics of SELECT
    -- calls on storages before calling CRUD select. Rebalancer may screw up
    -- statistics of SELECT calls, so we will disable it.
    local servers = g.cluster.servers
    local select_total_counter_before = 0
    for n, _ in ipairs(servers) do
        local c = g.cluster.servers[n].net_box:eval([[
            local vshard = require('vshard')
            vshard.storage.rebalancer_disable()
            assert(vshard.storage.sync(2) == true)
            assert(vshard.storage.rebalancing_is_in_progress() == false)

            return box.stat().SELECT.total
        ]])
        select_total_counter_before = select_total_counter_before + c
    end

    -- Make a CRUD's SELECT.
    local result, err = g.cluster.main_server.net_box:call('crud.select', {
        space_name, {{'==', 'name', 'Anton Chekhov'}}
    })
    t.assert_equals(err, nil)
    t.assert_not_equals(result, nil)
    t.assert_equals(#result.rows, 1)

    -- Enable vshard's rebalancer and account current statistics of SELECT
    -- calls on storages after calling CRUD select.
    local select_total_counter_after = 0
    for n, _ in ipairs(servers) do
        local c = g.cluster.servers[n].net_box:eval([[
            local vshard = require('vshard')
            local stat = box.stat().SELECT.total
            vshard.storage.rebalancer_enable()

            return stat
        ]])
        select_total_counter_after = select_total_counter_after + c
    end

    -- Compare total counters of SELECT calls on cluster's storages before and
    -- after calling SELECT on router. Make sure no more than 1 storage changed
    -- SELECT counter. Otherwise we lead map reduce.
    local diff = select_total_counter_after - select_total_counter_before
    t.assert_le(diff, 4)
    t.assert_ge(diff, 2)
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
    t.assert_not_equals(result, nil)
    t.assert_equals(#result.rows, 1)
end

pgroup.test_non_unique_index = function(g)
    local space_name = 'customers_name_key_non_uniq_index'
    local customers = helpers.insert_objects(g, space_name, {
        {id = 1, name = 'Viktor Pelevin', age = 58},
        {id = 2, name = 'Isaac Asimov', age = 72},
        {id = 3, name = 'Aleksandr Solzhenitsyn', age = 89},
        {id = 4, name = 'James Joyce', age = 59},
        {id = 5, name = 'Oscar Wilde', age = 46},
        {id = 6, name = 'Ivan Bunin', age = 83},
        {id = 7, name = 'Ivan Turgenev', age = 64},
        {id = 8, name = 'Alexander Ostrovsky', age = 63},
        {id = 9, name = 'Anton Chekhov', age = 44},
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
    local result, err = g.cluster.main_server.net_box:call('crud.get', {'customers_secondary_idx_name_key', {'Leo'}})

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
        'customers_secondary_idx_name_key', {'Victor'}, update_operations,
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
        'customers_secondary_idx_name_key', {'Alexander'}
    })

    t.assert_str_contains(err.err,
        "Sharding key for space \"customers_secondary_idx_name_key\" is missed in primary index, specify bucket_id")
    t.assert_equals(result, nil)
end

pgroup.test_update_cache = function(g)
    local space_name = 'customers_name_key'
    local sharding_key_as_index_obj = helpers.update_cache(g.cluster, space_name)
    t.assert_equals(sharding_key_as_index_obj, {parts = {{fieldno = 3}}})

    helpers.call_on_servers(g.cluster, {'s1-master', 's2-master'}, function(server)
        server.net_box:call('set_sharding_key', {space_name, {'age'}})
    end)
    sharding_key_as_index_obj = helpers.update_cache(g.cluster, space_name)
    t.assert_equals(sharding_key_as_index_obj, {parts = {{fieldno = 4}}})

    -- Recover sharding key.
    helpers.call_on_servers(g.cluster, {'s1-master', 's2-master'}, function(server)
        server.net_box:call('set_sharding_key', {space_name, {'name'}})
    end)
    sharding_key_as_index_obj = helpers.update_cache(g.cluster, space_name)
    t.assert_equals(sharding_key_as_index_obj, {parts = {{fieldno = 3}}})
end

pgroup.test_jsonpath_insert = function(g)
    t.skip('JSONpath is unsupported, see issue #219')

    local space_name = 'customers_jsonpath_key'
    local customers = helpers.insert_objects(g, space_name, {
        {
            id = {customer_id = {unsigned = 1}},
            name = 'Viktor Pelevin',
            age = 58,
            data = {customer = {weight = 82}},
        },
        {
            id = {customer_id = {unsigned = 2}},
            name = 'Isaac Asimov',
            age = 72,
            data = {customer = {weight = 70}},
        },
        {
            id = {customer_id = {unsigned = 3}},
            name = 'Aleksandr Solzhenitsyn',
            age = 89,
            data = {customer = {weight = 78}},
        },
        {
            id = {customer_id = {unsigned = 4}},
            name = 'James Joyce',
            age = 59,
            data = {customer = {weight = 82}},
        },
        {
            id = {customer_id = {unsigned = 5}},
            name = 'Oscar Wilde',
            age = 46,
            data = {customer = {weight = 79}},
        },
    })
    t.assert_equals(#customers, 5)
end

pgroup.test_jsonpath_delete = function(g)
    t.skip('JSONpath is unsupported, see issue #219')

    local space_name = 'customers_jsonpath_key'
    local customers = helpers.insert_objects(g, space_name, {
        {
            id = {customer_id = {unsigned = 1}},
            name = 'Viktor Pelevin',
            age = 58,
            data = {customer = {weight = 82}},
        },
        {
            id = {customer_id = {unsigned = 2}},
            name = 'Isaac Asimov',
            age = 72,
            data = {customer = {weight = 70}},
        },
        {
            id = {customer_id = {unsigned = 3}},
            name = 'Aleksandr Solzhenitsyn',
            age = 89,
            data = {customer = {weight = 78}},
        },
        {
            id = {customer_id = {unsigned = 4}},
            name = 'James Joyce',
            age = 59,
            data = {customer = {weight = 82}},
        },
        {
            id = {customer_id = {unsigned = 5}},
            name = 'Oscar Wilde',
            age = 46,
            data = {customer = {weight = 79}},
        },
    })
    t.assert_equals(#customers, 5)
end
