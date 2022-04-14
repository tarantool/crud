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

pgroup_storage.after_all(stop_cluster)

pgroup_storage.before_each(function(g)
    helpers.call_on_storages(g.cluster, function(server)
        server.net_box:call('reset_to_default_schema')
    end)
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
