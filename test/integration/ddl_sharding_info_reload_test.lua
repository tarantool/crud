local t = require('luatest')

local sharding_utils = require('crud.common.sharding.utils')

local helpers = require('test.helper')

local ok = pcall(require, 'ddl')
if not ok then
    t.skip('Lua module ddl is required to run test')
end

local pgroup_storage = t.group('ddl_storage_sharding_info', helpers.backend_matrix({
    {engine = 'memtx'},
    {engine = 'vinyl'},
}))

local pgroup_new_space = t.group('ddl_sharding_info_on_new_space', helpers.backend_matrix({
    {engine = 'memtx'},
    {engine = 'vinyl'},
}))

local pgroup_key_change = t.group('ddl_sharding_key_reload_after_schema_change', helpers.backend_matrix({
    {engine = 'memtx'},
    {engine = 'vinyl'},
}))

local pgroup_func_change = t.group('ddl_sharding_func_reload_after_schema_change', helpers.backend_matrix({
    {engine = 'memtx'},
    {engine = 'vinyl'},
}))

local select_limit = 100

local function start_cluster(g)
    helpers.start_default_cluster(g, 'srv_ddl_reload')
end

local function stop_cluster(g)
    helpers.stop_cluster(g.cluster, g.params.backend)
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

    conn_s1.space['customers']:delete(0)
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

    conn_s2.space['customers_pk']:delete(0)
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

pgroup_storage.test_gh_310_ddl_key_record_delete_removes_cache_entry = function(g)
    local storage = g.cluster:server('s1-master')
    local space_name = sharding_cases.sharding_key_hash.test_space

    -- Init cache by fetching sharding info.
    local _, err = get_hash(storage, 'get_sharding_key_hash', space_name)
    t.assert_equals(err, nil)

    -- Drop space together with sharding info.
    local _, err = storage:eval([[
        local ddl = require('ddl')

        local space_name = ...

        local current_schema, err = ddl.get_schema()
        if err ~= nil then
            error(err)
        end

        box.space[space_name]:drop()
        box.space['_ddl_sharding_key']:delete(space_name)

        current_schema.spaces[space_name] = nil

        local _, err = ddl.set_schema(current_schema)
        if err ~= nil then
            error(err)
        end
    ]], {space_name})
    t.assert_equals(err, nil)

    local hash, err = get_hash(storage, 'get_sharding_key_hash', space_name)
    t.assert_equals(err, nil)
    t.assert_equals(hash, nil)
end

pgroup_storage.test_gh_310_ddl_func_record_delete_removes_cache_entry = function(g)
    local storage = g.cluster:server('s1-master')
    local space_name = sharding_cases.sharding_func_hash.test_space

    -- Init cache by fetching sharding info.
    local _, err = get_hash(storage, 'get_sharding_func_hash', space_name)
    t.assert_equals(err, nil)

    -- Drop space together with sharding info.
    local _, err = storage:eval([[
        local ddl = require('ddl')

        local space_name = ...

        local current_schema, err = ddl.get_schema()
        if err ~= nil then
            error(err)
        end

        box.space[space_name]:drop()
        box.space['_ddl_sharding_func']:delete(space_name)

        current_schema.spaces[space_name] = nil

        local _, err = ddl.set_schema(current_schema)
        if err ~= nil then
            error(err)
        end
    ]], {space_name})
    t.assert_equals(err, nil)

    local hash, err = get_hash(storage, 'get_sharding_func_hash', space_name)
    t.assert_equals(err, nil)
    t.assert_equals(hash, nil)
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
            helpers.skip_not_cartridge_backend(g.params.backend)
            t.skip_if(
                ((reload_case == 'reload_roles')
                and not helpers.is_cartridge_hotreload_supported()),
                "Cartridge roles reload is not supported")
            helpers.skip_old_tarantool_cartridge_hotreload()

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
            helpers.skip_not_cartridge_backend(g.params.backend)
            t.skip_if(
                ((reload_case == 'reload_roles')
                and not helpers.is_cartridge_hotreload_supported()),
                "Cartridge roles reload is not supported")
            helpers.skip_old_tarantool_cartridge_hotreload()

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


-- Test metainfo is updated on router if new space added to ddl.

local test_tuple = {1, box.NULL, 'Emma', 22}
local test_object = { id = 1, bucket_id = box.NULL, name = 'Emma', age = 22 }

local test_tuples_batch = {
    {1, box.NULL, 'Emma', 22},
    {2, box.NULL, 'Anton', 19},
    {3, box.NULL, 'Petra', 27},
}
local test_objects_batch = {
    { id = 1, bucket_id = box.NULL, name = 'Emma', age = 22 },
    { id = 2, bucket_id = box.NULL, name = 'Anton', age = 19 },
    { id = 3, bucket_id = box.NULL, name = 'Petra', age = 27 },
}

local upsert_many_operations = { {}, {}, {} }
local test_tuples_operation_batch = helpers.complement_tuples_batch_with_operations(
        test_tuples_batch,
        upsert_many_operations)
local test_objects_operation_batch = helpers.complement_tuples_batch_with_operations(
        test_objects_batch,
        upsert_many_operations)

-- Sharded by "name" and computed with custom sharding function.
local test_customers_new_result = {
    s1 = {{1, 2861, 'Emma', 22}},
    s2 = {},
}

local test_customers_new_batching_result = {
    s1 = {{1, 2861, 'Emma', 22}, {2, 2276, 'Anton', 19}},
    s2 = {{3, 910, 'Petra', 27}},
}

local new_space_cases = {
    insert = {
        func = 'crud.insert',
        input = {'customers_new', test_tuple},
        result = test_customers_new_result,
    },
    insert_object = {
        func = 'crud.insert_object',
        input = {'customers_new', test_object},
        result = test_customers_new_result,
    },
    insert_many = {
        func = 'crud.insert_many',
        input = {'customers_new', test_tuples_batch},
        result = test_customers_new_batching_result,
    },
    insert_object_many = {
        func = 'crud.insert_object_many',
        input = {'customers_new', test_objects_batch},
        result = test_customers_new_batching_result,
    },
    replace = {
        func = 'crud.replace',
        input = {'customers_new', test_tuple},
        result = test_customers_new_result,
    },
    replace_object = {
        func = 'crud.replace_object',
        input = {'customers_new', test_object},
        result = test_customers_new_result,
    },
    replace_many = {
        func = 'crud.replace_many',
        input = {'customers_new', test_tuples_batch},
        result = test_customers_new_batching_result,
    },
    replace_object_many = {
        func = 'crud.replace_object_many',
        input = {'customers_new', test_objects_batch},
        result = test_customers_new_batching_result,
    },
    upsert = {
        func = 'crud.upsert',
        input = {'customers_new', test_tuple, {}},
        result = test_customers_new_result,
    },
    upsert_object = {
        func = 'crud.upsert_object',
        input = {'customers_new', test_object, {}},
        result = test_customers_new_result,
    },
    upsert_many = {
        func = 'crud.upsert_many',
        input = {'customers_new', test_tuples_operation_batch},
        result = test_customers_new_batching_result,
    },
    upsert_object_many = {
        func = 'crud.upsert_object_many',
        input = {'customers_new', test_objects_operation_batch},
        result = test_customers_new_batching_result,
    },
}

for name, case in pairs(new_space_cases) do
    local test_name = ('test_%s'):format(name)

    pgroup_new_space[test_name] = function(g)
        -- Create space 'customers_new', sharded by 'name'.
        helpers.call_on_storages(g.cluster, function(server)
            server.net_box:call('create_new_space')
        end)

        -- Assert it is now possible to call opertions for a new space.
        local obj, err = g.cluster.main_server.net_box:call(case.func, case.input)
        t.assert_is_not(obj, nil)
        t.assert_equals(err, nil)

        -- Assert it is sharded based on updated ddl info.
        local conn_s1 = g.cluster:server('s1-master').net_box
        local result = conn_s1.space['customers_new']:select(nil, {limit = select_limit})
        t.assert_equals(result, case.result.s1)

        local conn_s2 = g.cluster:server('s2-master').net_box
        local result = conn_s2.space['customers_new']:select(nil, {limit = select_limit})
        t.assert_equals(result, case.result.s2)
    end
end


-- Test using outdated sharding key info returns error.

-- Sharded by "age".
local test_customers_age_tuple = {1, 655, 'Emma', 22}
local test_customers_age_result = {
    s1 = {},
    s2 = {test_customers_age_tuple},
}

local test_customers_age_batching_result = {
    s1 = {{3, 1811, 'Petra', 27}},
    s2 = {{1, 655, 'Emma', 22}, {2, 1325, 'Anton', 19}},
}

local function setup_customers_migrated_data(g)
    if test_customers_age_result.s1 ~= nil and next(test_customers_age_result.s1) then
        local conn_s1 = g.cluster:server('s1-master').net_box
        conn_s1.space['customers']:insert(unpack(test_customers_age_result.s1))
    end
    if test_customers_age_result.s2 ~= nil and next(test_customers_age_result.s2) then
        local conn_s2 = g.cluster:server('s2-master').net_box
        conn_s2.space['customers']:insert(unpack(test_customers_age_result.s2))
    end
end

local schema_change_sharding_key_cases = {
    insert = {
        func = 'crud.insert',
        input = {'customers', test_tuple},
        result = test_customers_age_result,
    },
    insert_object = {
        func = 'crud.insert_object',
        input = {'customers', test_object},
        result = test_customers_age_result,
    },
    insert_many = {
        func = 'crud.insert_many',
        input = {'customers', test_tuples_batch},
        result = test_customers_age_batching_result,
    },
    insert_object_many = {
        func = 'crud.insert_object_many',
        input = {'customers', test_objects_batch},
        result = test_customers_age_batching_result,
    },
    replace = {
        func = 'crud.replace',
        input = {'customers', test_tuple},
        result = test_customers_age_result,
    },
    replace_object = {
        func = 'crud.replace_object',
        input = {'customers', test_object},
        result = test_customers_age_result,
    },
    replace_many = {
        func = 'crud.replace_many',
        input = {'customers', test_tuples_batch},
        result = test_customers_age_batching_result,
    },
    replace_object_many = {
        func = 'crud.replace_object_many',
        input = {'customers', test_objects_batch},
        result = test_customers_age_batching_result,
    },
    upsert = {
        func = 'crud.upsert',
        input = {'customers', test_tuple, {}},
        result = test_customers_age_result,
    },
    upsert_object = {
        func = 'crud.upsert_object',
        input = {'customers', test_object, {}},
        result = test_customers_age_result,
    },
    upsert_many = {
        func = 'crud.upsert_many',
        input = {'customers', test_tuples_operation_batch},
        result = test_customers_age_batching_result,
    },
    upsert_object_many = {
        func = 'crud.upsert_object_many',
        input = {'customers', test_objects_operation_batch},
        result = test_customers_age_batching_result,
    },
}

for name, case in pairs(schema_change_sharding_key_cases) do
    local test_name = ('test_%s'):format(name)

    pgroup_key_change[test_name] = function(g)
        -- Change schema to shard 'customers' by 'age'.
        helpers.call_on_storages(g.cluster, function(server)
            server.net_box:call('set_sharding_key', {'customers', {'age'}})
        end)

        -- Assert operation bucket_id is computed based on updated ddl info.
        local obj, err = g.cluster.main_server.net_box:call(case.func, case.input)
        t.assert_is_not(obj, nil)
        t.assert_equals(err, nil)

        local conn_s1 = g.cluster:server('s1-master').net_box
        local result = conn_s1.space['customers']:select(nil, {limit = select_limit})
        t.assert_equals(result, case.result.s1)

        local conn_s2 = g.cluster:server('s2-master').net_box
        local result = conn_s2.space['customers']:select(nil, {limit = select_limit})
        t.assert_equals(result, case.result.s2)
    end
end

pgroup_key_change.before_test('test_select', setup_customers_migrated_data)

pgroup_key_change.test_select = function(g)
    -- Change schema to shard 'customers' by 'age'.
    helpers.call_on_storages(g.cluster, function(server)
        server.net_box:call('set_sharding_key', {'customers', {'age'}})
    end)

    -- Assert operation bucket_id is computed based on updated ddl info.
    local obj, err = g.cluster.main_server.net_box:call(
        'crud.select',
        {
            'customers',
            {{'==', 'id', 1}, {'==', 'name', 'Emma'}, {'==', 'age', 22}},
            {mode = 'write'},
        })
    t.assert_equals(err, nil)
    t.assert_equals(obj.rows, {test_customers_age_tuple})
end

pgroup_key_change.before_test('test_count', setup_customers_migrated_data)

pgroup_key_change.test_count = function(g)
    -- Change schema to shard 'customers' by 'age'.
    helpers.call_on_storages(g.cluster, function(server)
        server.net_box:call('set_sharding_key', {'customers', {'age'}})
    end)

    -- Assert operation bucket_id is computed based on updated ddl info.
    local obj, err = g.cluster.main_server.net_box:call(
        'crud.count',
        {
            'customers',
            {{'==', 'id', 1}, {'==', 'name', 'Emma'}, {'==', 'age', 22}},
            {mode = 'write'},
        })
    t.assert_equals(err, nil)
    t.assert_equals(obj, 1)
end

local pairs_eval = [[
    local res = {}
    for _, v in crud.pairs(...) do
        table.insert(res, v)
    end
    return res
]]

pgroup_key_change.before_test('test_pairs', setup_customers_migrated_data)

pgroup_key_change.test_pairs = function(g)
    -- Change schema to shard 'customers' by 'age'.
    helpers.call_on_storages(g.cluster, function(server)
        server.net_box:call('set_sharding_key', {'customers', {'age'}})
    end)

    -- First pairs request fails and reloads sharding info.
    t.assert_error_msg_contains(
        "Please retry your request",
        g.cluster.main_server.net_box.eval,
        g.cluster.main_server.net_box,
        pairs_eval,
        {
            'customers',
            {{'==', 'id', 1}, {'==', 'name', 'Emma'}, {'==', 'age', 22}},
            {mode = 'write'},
        })

    -- Assert operation bucket_id is computed based on updated ddl info.
    local obj, err = g.cluster.main_server.net_box:eval(
        pairs_eval,
        {
            'customers',
            {{'==', 'id', 1}, {'==', 'name', 'Emma'}, {'==', 'age', 22}},
            {mode = 'write'},
        })
    t.assert_equals(err, nil)
    t.assert_equals(obj, {test_customers_age_tuple})
end


-- Test using outdated sharding func info returns error.

-- Sharded by 'id' with custom sharding function.
local test_customers_pk_func_tuple = {1, 44, "Emma", 22}
local test_customers_pk_func = {
    s1 = {},
    s2 = {test_customers_pk_func_tuple},
}

local test_customers_pk_batching_func = {
    s1 = {},
    s2 = {
        {1, 44, 'Emma', 22},
        {2, 45, 'Anton', 19},
        {3, 46, 'Petra', 27},
    },
}

local function setup_customers_pk_migrated_data(g)
    if test_customers_pk_func.s1 ~= nil and next(test_customers_pk_func.s1) then
        local conn_s1 = g.cluster:server('s1-master').net_box
        conn_s1.space['customers_pk']:insert(unpack(test_customers_pk_func.s1))
    end
    if test_customers_pk_func.s2 ~= nil and next(test_customers_pk_func.s2) then
        local conn_s2 = g.cluster:server('s2-master').net_box
        conn_s2.space['customers_pk']:insert(unpack(test_customers_pk_func.s2))
    end
end

local schema_change_sharding_func_cases = {
    insert = {
        func = 'crud.insert',
        input = {'customers_pk', test_tuple},
        result = test_customers_pk_func,
    },
    insert_object = {
        func = 'crud.insert_object',
        input = {'customers_pk', test_object},
        result = test_customers_pk_func,
    },
    insert_many = {
        func = 'crud.insert_many',
        input = {'customers_pk', test_tuples_batch},
        result = test_customers_pk_batching_func,
    },
    insert_object_many = {
        func = 'crud.insert_object_many',
        input = {'customers_pk', test_objects_batch},
        result = test_customers_pk_batching_func,
    },
    replace = {
        func = 'crud.replace',
        input = {'customers_pk', test_tuple},
        result = test_customers_pk_func,
    },
    replace_object = {
        func = 'crud.replace_object',
        input = {'customers_pk', test_object},
        result = test_customers_pk_func,
    },
    replace_many = {
        func = 'crud.replace_many',
        input = {'customers_pk', test_tuples_batch},
        result = test_customers_pk_batching_func,
    },
    replace_object_many = {
        func = 'crud.replace_object_many',
        input = {'customers_pk', test_objects_batch},
        result = test_customers_pk_batching_func,
    },
    upsert = {
        func = 'crud.upsert',
        input = {'customers_pk', test_tuple, {}},
        result = test_customers_pk_func,
    },
    upsert_object = {
        func = 'crud.upsert_object',
        input = {'customers_pk', test_object, {}},
        result = test_customers_pk_func,
    },
    upsert_many = {
        func = 'crud.upsert_many',
        input = {'customers_pk', test_tuples_operation_batch},
        result = test_customers_pk_batching_func,
    },
    upsert_object_many = {
        func = 'crud.upsert_object_many',
        input = {'customers_pk', test_objects_operation_batch},
        result = test_customers_pk_batching_func,
    },
    delete = {
        before_test = setup_customers_pk_migrated_data,
        func = 'crud.delete',
        input = {'customers_pk', 1},
        result = {
            s1 = {},
            s2 = {},
        },
    },
    update = {
        before_test = setup_customers_pk_migrated_data,
        func = 'crud.update',
        input = {'customers_pk', 1, {{'+', 4, 1}}},
        result = {
            s1 = {},
            s2 = {{1, 44, "Emma", 23}},
        },
    },
}

for name, case in pairs(schema_change_sharding_func_cases) do
    local test_name = ('test_%s'):format(name)

    if case.before_test ~= nil then
        pgroup_func_change.before_test(test_name, case.before_test)
    end

    pgroup_func_change[test_name] = function(g)
        -- Change schema to shard 'customers_pk' with another sharding function.
        helpers.call_on_storages(g.cluster, function(server)
            server.net_box:call('set_sharding_func_name',
                                {'customers_pk', 'customers_module.sharding_func_new'})
        end)

        -- Assert operation bucket_id is computed based on updated ddl info.
        local obj, err = g.cluster.main_server.net_box:call(case.func, case.input)
        t.assert_is_not(obj, nil)
        t.assert_equals(err, nil)

        local conn_s1 = g.cluster:server('s1-master').net_box
        local result = conn_s1.space['customers_pk']:select(nil, {limit = select_limit})
        t.assert_equals(result, case.result.s1)

        local conn_s2 = g.cluster:server('s2-master').net_box
        local result = conn_s2.space['customers_pk']:select(nil, {limit = select_limit})
        t.assert_equals(result, case.result.s2)
    end
end

pgroup_func_change.before_test('test_select', setup_customers_pk_migrated_data)

pgroup_func_change.test_select = function(g)
    -- Change schema to shard 'customers_pk' with another sharding function.
    helpers.call_on_storages(g.cluster, function(server)
        server.net_box:call('set_sharding_func_name',
                            {'customers_pk', 'customers_module.sharding_func_new'})
    end)

    -- Assert operation bucket_id is computed based on updated ddl info.
    local obj, err = g.cluster.main_server.net_box:call('crud.select', {
        'customers_pk', {{'==', 'id', 1}}, {mode = 'write'},
    })
    t.assert_equals(err, nil)
    t.assert_equals(obj.rows, {test_customers_pk_func_tuple})
end

pgroup_func_change.before_test('test_get', setup_customers_pk_migrated_data)

pgroup_func_change.test_get = function(g)
    -- Change schema to shard 'customers_pk' with another sharding function.
    helpers.call_on_storages(g.cluster, function(server)
        server.net_box:call('set_sharding_func_name',
                            {'customers_pk', 'customers_module.sharding_func_new'})
    end)

    -- Assert operation bucket_id is computed based on updated ddl info.
    local obj, err = g.cluster.main_server.net_box:call('crud.get', {
        'customers_pk', 1, {mode = 'write'},
    })
    t.assert_equals(err, nil)
    t.assert_equals(obj.rows, {test_customers_pk_func_tuple})
end

pgroup_func_change.before_test('test_count', setup_customers_pk_migrated_data)

pgroup_func_change.test_count = function(g)
    -- Change schema to shard 'customers_pk' with another sharding function.
    helpers.call_on_storages(g.cluster, function(server)
        server.net_box:call('set_sharding_func_name',
                            {'customers_pk', 'customers_module.sharding_func_new'})
    end)

    -- Assert operation bucket_id is computed based on updated ddl info.
    local obj, err = g.cluster.main_server.net_box:call('crud.count', {
        'customers_pk', {{'==', 'id', 1}}, {mode = 'write'},
    })
    t.assert_equals(err, nil)
    t.assert_equals(obj, 1)
end

pgroup_func_change.before_test('test_pairs', setup_customers_pk_migrated_data)

pgroup_func_change.test_pairs = function(g)
    -- Change schema to shard 'customers_pk' with another sharding function.
    helpers.call_on_storages(g.cluster, function(server)
        server.net_box:call('set_sharding_func_name',
                            {'customers_pk', 'customers_module.sharding_func_new'})
    end)

    t.assert_error_msg_contains(
        "Please retry your request",
        g.cluster.main_server.net_box.eval,
        g.cluster.main_server.net_box,
        pairs_eval,
        {'customers_pk', {{'==', 'id', 1}}, {mode = 'write'}})

    -- Assert operation bucket_id is computed based on updated ddl info.
    local obj, err = g.cluster.main_server.net_box:eval(
        pairs_eval,
        {'customers_pk', {{'==', 'id', 1}}, {mode = 'write'}})
    t.assert_equals(err, nil)
    t.assert_equals(obj, {test_customers_pk_func_tuple})
end
