local fio = require('fio')
local t = require('luatest')

local sharding_utils = require('crud.common.sharding.utils')

local helpers = require('test.helper')

local ok = pcall(require, 'ddl')
if not ok then
    t.skip('Lua module ddl is required to run test')
end

local pgroup_storage = t.group('ddl_storage_sharding_info', {
    {engine = 'memtx'},
    {engine = 'vinyl'},
})

local pgroup_new_space = t.group('ddl_sharding_info_on_new_space', {
    {engine = 'memtx'},
    {engine = 'vinyl'},
})

local pgroup_key_change = t.group('ddl_sharding_key_after_schema_change', {
    {engine = 'memtx'},
    {engine = 'vinyl'},
})

local pgroup_func_change = t.group('ddl_sharding_func_after_schema_change', {
    {engine = 'memtx'},
    {engine = 'vinyl'},
})

local function start_cluster(g)
    g.cluster = helpers.Cluster:new({
        datadir = fio.tempdir(),
        server_command = helpers.entrypoint('srv_ddl_reload'),
        use_vshard = true,
        replicasets = helpers.get_test_replicasets(),
        env = {
            ['ENGINE'] = g.params.engine,
        },
    })
    g.cluster:start()
end

local function stop_cluster(g)
    helpers.stop_cluster(g.cluster)
end

pgroup_storage.before_all(start_cluster)
pgroup_new_space.before_all(start_cluster)
pgroup_key_change.before_all(start_cluster)
pgroup_func_change.before_all(start_cluster)

pgroup_storage.after_all(stop_cluster)
pgroup_new_space.after_all(stop_cluster)
pgroup_key_change.after_all(stop_cluster)
pgroup_func_change.after_all(stop_cluster)

pgroup_storage.before_each(function(g)
    helpers.call_on_storages(g.cluster, function(server)
        server.net_box:call('reset_to_default_schema')
    end)
end)

pgroup_new_space.before_each(function(g)
    helpers.drop_space_on_cluster(g.cluster, 'customers')
    helpers.drop_space_on_cluster(g.cluster, 'customers_new')

    helpers.call_on_storages(g.cluster, function(server)
        server.net_box:call('reset_to_default_schema')
    end)

    -- Fetch metadata schema.
    local obj, err = g.cluster.main_server.net_box:call(
       'crud.insert', {'customers', {0, box.NULL, 'Emma', 22}}
    )

    t.assert_is_not(obj, nil)
    t.assert_equals(err, nil)

    -- Assert space doesn't exist.
    local obj, err = g.cluster.main_server.net_box:call(
       'crud.insert', {'customers_new', {1, box.NULL, 'Emma', 22}}
    )

    t.assert_equals(obj, nil)
    t.assert_is_not(err, nil)
    t.assert_str_contains(err.err, "Space \\-\"customers_new\\-\" doesn't exist", true)
end)

pgroup_key_change.before_each(function(g)
    -- Clean up.
    helpers.truncate_space_on_cluster(g.cluster, 'customers')

    helpers.call_on_storages(g.cluster, function(server)
        server.net_box:call('reset_to_default_schema')
    end)

    -- Assert schema is default: insert is sharded with default ddl info.
    local obj, err = g.cluster.main_server.net_box:call(
       'crud.insert', {'customers', {0, box.NULL, 'Emma', 22}}
    )
    t.assert_is_not(obj, nil)
    t.assert_equals(err, nil)

    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(0)
    t.assert_equals(result, {0, 2861, 'Emma', 22})

    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:get(0)
    t.assert_equals(result, nil)
end)

pgroup_func_change.before_each(function(g)
    -- Clean up.
    helpers.truncate_space_on_cluster(g.cluster, 'customers_pk')

    helpers.call_on_storages(g.cluster, function(server)
        server.net_box:call('reset_to_default_schema')
    end)

    -- Assert schema is default: insert is sharded with default ddl info.
    local obj, err = g.cluster.main_server.net_box:call(
       'crud.insert', {'customers_pk', {0, box.NULL, 'Emma', 22}}
    )
    t.assert_is_not(obj, nil)
    t.assert_equals(err, nil)

    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers_pk']:get(0)
    t.assert_equals(result, nil)

    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers_pk']:get(0)
    t.assert_equals(result, {0, 1, 'Emma', 22})
end)


-- Test storage sharding metainfo.

local function get_hash(storage, func_name, space_name)
    return storage:eval([[
        local func_name, space_name = ...
        local storage_cache = require('crud.common.sharding.storage_metadata_cache')
        return storage_cache[func_name](space_name)
    ]], {func_name, space_name})
end

local sharding_cases = {
    sharding_func_hash = {
        eval_func = 'get_sharding_func_hash',
        ddl_space = '_ddl_sharding_func',
        test_space = 'customers_pk',
        test_case = 'test_sharding_func_hash_is_updated_when_ddl_is_updated',
    },
    sharding_key_hash = {
        eval_func = 'get_sharding_key_hash',
        ddl_space = '_ddl_sharding_key',
        test_space = 'customers',
        test_case = 'test_sharding_key_hash_is_updated_when_ddl_is_updated',
    },
}

pgroup_storage.test_sharding_key_hash_is_updated_when_ddl_is_updated = function(g)
    local storage = g.cluster:server('s1-master')
    local space = sharding_cases.sharding_func_hash.test_space

    -- Set up sharding key (equal to default one).
    local sharding_key_v1 = {'name'}
    local _, err = storage:call('set_sharding_key', {space, sharding_key_v1})
    t.assert_equals(err, nil)

    local hash, err = get_hash(storage, 'get_sharding_key_hash', space)

    t.assert_equals(err, nil)
    t.assert_equals(hash, sharding_utils.compute_hash(sharding_key_v1))

    -- Change sharding key value.
    local sharding_key_v2 = {'age'}
    local _, err = storage:call('set_sharding_key', {space, sharding_key_v2})
    t.assert_equals(err, nil)

    local hash, err = get_hash(storage, 'get_sharding_key_hash', space)

    t.assert_equals(err, nil)
    t.assert_equals(hash, sharding_utils.compute_hash(sharding_key_v2))
end

pgroup_storage.test_sharding_func_hash_is_updated_when_ddl_is_updated = function(g)
    local storage = g.cluster:server('s1-master')
    local space = sharding_cases.sharding_key_hash.test_space

    -- Set up sharding func (equal to default one).
    local sharding_func_name = 'customers_module.sharding_func_default'
    local _, err = storage:call('set_sharding_func_name', {space, sharding_func_name})
    t.assert_equals(err, nil)

    local hash, err = get_hash(storage, 'get_sharding_func_hash', space)

    t.assert_equals(err, nil)
    t.assert_equals(hash, sharding_utils.compute_hash(sharding_func_name))

    -- Change sharding func type and value.
    local sharding_func_body = 'function() return 1 end'
    local _, err = storage:call('set_sharding_func_body', {space, sharding_func_body})
    t.assert_equals(err, nil)

    local hash, err = get_hash(storage, 'get_sharding_func_hash', space)

    t.assert_equals(err, nil)
    t.assert_equals(hash, sharding_utils.compute_hash({body = sharding_func_body}))
end


-- Test storage hash metadata mechanisms are ok after code reload.

local reload_cases = {
    module_reload = 'reload_package',
    roles_reload = 'reload_roles'
}

for sharding_case_name, sharding_case in pairs(sharding_cases) do
    for reload_case_name, reload_case in pairs(reload_cases) do

        -- Test code reload do not break trigger logic.

        local test_name = ('test_%s_do_not_break_%s_update'):format(
                           reload_case_name, sharding_case_name)

        pgroup_storage[test_name] = function(g)
            local storage = g.cluster:server('s1-master')

            -- Init the cache.
            local _, err = get_hash(storage, sharding_case.eval_func,
                                    sharding_case.test_space)
            t.assert_equals(err, nil)

            -- Reload the code.
            helpers[reload_case](storage)

            -- Execute test case from above to check that logic wasn't broken.
            g[sharding_case.test_case](g)
        end
    end
end

for _, sharding_case in pairs(sharding_cases) do
    for reload_case_name, reload_case in pairs(reload_cases) do

        -- Test code reload cleans up redundant triggers.

        local test_name = ('test_redundant_%s_triggers_cleaned_up_on_%s'):format(
                           sharding_case.ddl_space, reload_case_name)

        pgroup_storage[test_name] = function(g)
            local storage = g.cluster:server('s1-master')

            -- Init the cache.
            local _, err = get_hash(storage, sharding_case.eval_func,
                                    sharding_case.test_space)
            t.assert_equals(err, nil)

            local before_count = helpers.count_on_replace_triggers(storage,
                                                                   sharding_case.ddl_space)

            -- Reload the code.
            helpers[reload_case](storage)

            -- Reinit the cache.
            local _, err = get_hash(storage, sharding_case.eval_func,
                                    sharding_case.test_space)
            t.assert_equals(err, nil)

            local after_count = helpers.count_on_replace_triggers(storage,
                                                                  sharding_case.ddl_space)
            t.assert_equals(after_count, before_count)
        end
    end
end


-- Test using outdated metainfo for a new space returns error.

local test_tuple = {1, box.NULL, 'Emma', 22}
local test_object = { id = 1, bucket_id = box.NULL, name = 'Emma', age = 22 }

local new_space_cases = {
    insert = {
        func = 'crud.insert',
        input = {'customers_new', test_tuple},
    },
    insert_object = {
        func = 'crud.insert_object',
        input = {'customers_new', test_object},
    },
    replace = {
        func = 'crud.replace',
        input = {'customers_new', test_tuple},
    },
    replace_object = {
        func = 'crud.replace_object',
        input = {'customers_new', test_object},
    },
    upsert = {
        func = 'crud.upsert',
        input = {'customers_new', test_tuple, {}},
    },
    upsert_object = {
        func = 'crud.upsert_object',
        input = {'customers_new', test_object, {}},
    },
}

for name, case in pairs(new_space_cases) do
    local test_name = ('test_%s_with_outdated_info_returns_error'):format(name)

    pgroup_new_space[test_name] = function(g)
        -- Create space 'customers_new', sharded by 'name'.
        helpers.call_on_storages(g.cluster, function(server)
            server.net_box:call('create_new_space')
        end)

        local obj, err = g.cluster.main_server.net_box:call(case.func, case.input)
        t.assert_equals(obj, nil)
        t.assert_type(err, 'table')
        t.assert_str_contains(err.str,
                              "Please refresh sharding data and retry your request")
    end
end


-- Test using outdated sharding key info returns error.

-- Sharded by "age".
local test_customers_age_result = {
    s1 = nil,
    s2 = {1, 655, 'Emma', 22},
}

local function setup_customers_migrated_data(g)
    if test_customers_age_result.s1 ~= nil then
        local conn_s1 = g.cluster:server('s1-master').net_box
        conn_s1.space['customers']:insert(test_customers_age_result.s1)
    end
    if test_customers_age_result.s2 ~= nil then
        local conn_s2 = g.cluster:server('s2-master').net_box
        conn_s2.space['customers']:insert(test_customers_age_result.s2)
    end
end

local schema_change_sharding_key_cases = {
    insert = {
        func = 'crud.insert',
        input = {'customers', test_tuple},
    },
    insert_object = {
        func = 'crud.insert_object',
        input = {'customers', test_object},
    },
    replace = {
        func = 'crud.replace',
        input = {'customers', test_tuple},
    },
    replace_object = {
        func = 'crud.replace_object',
        input = {'customers', test_object},
    },
    upsert = {
        func = 'crud.upsert',
        input = {'customers', test_tuple, {}},
    },
    upsert_object = {
        func = 'crud.upsert_object',
        input = {'customers', test_object, {}},
    },
    select = {
        before_test = setup_customers_migrated_data,
        func = 'crud.select',
        input = {
            'customers',
            {{'==', 'id', 1}, {'==', 'name', 'Emma'}, {'==', 'age', 22}}
        },
    },
    count = {
        before_test = setup_customers_migrated_data,
        func = 'crud.count',
        input = {
            'customers',
            {{'==', 'id', 1}, {'==', 'name', 'Emma'}, {'==', 'age', 22}}
        },
    },
}

for name, case in pairs(schema_change_sharding_key_cases) do
    local test_name = ('test_%s_with_outdated_info_returns_error'):format(name)

    if case.before_test ~= nil then
        pgroup_key_change.before_test(test_name, case.before_test)
    end

    pgroup_key_change[test_name] = function(g)
        -- Change schema to shard 'customers' by 'age'.
        helpers.call_on_storages(g.cluster, function(server)
            server.net_box:call('set_sharding_key', {'customers', {'age'}})
        end)

        local obj, err = g.cluster.main_server.net_box:call(case.func, case.input)
        t.assert_equals(obj, nil)
        t.assert_type(err, 'table')
        t.assert_str_contains(err.str,
                              "Please refresh sharding data and retry your request")
    end
end

local pairs_eval = [[
    local res = {}
    for _, v in crud.pairs(...) do
        table.insert(res, v)
    end
    return res
]]

pgroup_key_change.before_test('test_pairs_with_outdated_info_throws_error',
                              setup_customers_migrated_data)

pgroup_key_change.test_pairs_with_outdated_info_throws_error = function(g)
    -- Change schema to shard 'customers' by 'age'.
    helpers.call_on_storages(g.cluster, function(server)
        server.net_box:call('set_sharding_key', {'customers', {'age'}})
    end)

    t.assert_error_msg_contains(
        "Sharding hash mismatch for space customers",
        g.cluster.main_server.net_box.eval,
        g.cluster.main_server.net_box,
        pairs_eval,
        {
            'customers',
            {{'==', 'id', 1}, {'==', 'name', 'Emma'}, {'==', 'age', 22}}
        })
end


-- Test using outdated sharding func info returns error.

-- Sharded by 'id' with custom sharding function.
local test_customers_pk_func = {
    s1 = nil,
    s2 = {1, 44, "Emma", 22},
}

local function setup_customers_pk_migrated_data(g)
    if test_customers_pk_func.s1 ~= nil then
        local conn_s1 = g.cluster:server('s1-master').net_box
        conn_s1.space['customers_pk']:insert(test_customers_pk_func.s1)
    end
    if test_customers_pk_func.s2 ~= nil then
        local conn_s2 = g.cluster:server('s2-master').net_box
        conn_s2.space['customers_pk']:insert(test_customers_pk_func.s2)
    end
end

local schema_change_sharding_func_cases = {
    insert = {
        func = 'crud.insert',
        input = {'customers_pk', test_tuple},
    },
    insert_object = {
        func = 'crud.insert_object',
        input = {'customers_pk', test_object},
    },
    replace = {
        func = 'crud.replace',
        input = {'customers_pk', test_tuple},
    },
    replace_object = {
        func = 'crud.replace_object',
        input = {'customers_pk', test_object},
    },
    upsert = {
        func = 'crud.upsert',
        input = {'customers_pk', test_tuple, {}},
    },
    upsert_object = {
        func = 'crud.upsert_object',
        input = {'customers_pk', test_object, {}},
    },
    get = {
        before_test = setup_customers_pk_migrated_data,
        func = 'crud.get',
        input = {'customers_pk', 1},
    },
    delete = {
        before_test = setup_customers_pk_migrated_data,
        func = 'crud.delete',
        input = {'customers_pk', 1},
    },
    update = {
        before_test = setup_customers_pk_migrated_data,
        func = 'crud.update',
        input = {'customers_pk', 1, {{'+', 4, 1}}},
    },
    select = {
        before_test = setup_customers_pk_migrated_data,
        func = 'crud.select',
        input = {'customers_pk', {{'==', 'id', 1}}},
    },
    count = {
        before_test = setup_customers_pk_migrated_data,
        func = 'crud.count',
        input = {'customers_pk', {{'==', 'id', 1}}},
    },
}

for name, case in pairs(schema_change_sharding_func_cases) do
    local test_name = ('test_%s_with_outdated_info_returns_error'):format(name)

    if case.before_test ~= nil then
        pgroup_func_change.before_test(test_name, case.before_test)
    end

    pgroup_func_change[test_name] = function(g)
        -- Change schema to shard 'customers_pk' with another sharding function.
        helpers.call_on_storages(g.cluster, function(server)
            server.net_box:call('set_sharding_func_name',
                                {'customers_pk', 'customers_module.sharding_func_new'})
        end)

        local obj, err = g.cluster.main_server.net_box:call(case.func, case.input)
        t.assert_equals(obj, nil)
        t.assert_type(err, 'table')
        t.assert_str_contains(err.str,
                              "Please refresh sharding data and retry your request")
    end
end

pgroup_func_change.before_test('test_pairs_with_outdated_info_throws_error',
                               setup_customers_migrated_data)

pgroup_func_change.test_pairs_with_outdated_info_throws_error = function(g)
    -- Change schema to shard 'customers_pk' with another sharding function.
    helpers.call_on_storages(g.cluster, function(server)
        server.net_box:call('set_sharding_func_name',
                            {'customers_pk', 'customers_module.sharding_func_new'})
    end)

    t.assert_error_msg_contains(
        "Please refresh sharding data and retry your request",
        g.cluster.main_server.net_box.eval,
        g.cluster.main_server.net_box,
        pairs_eval,
        {'customers_pk', {{'==', 'id', 1}}})
end
