require('strict').on()

local t = require('luatest')

local log = require('log')
local checks = require('checks')
local digest = require('digest')
local fio = require('fio')

local crud = require('crud')

if os.getenv('DEV') == nil then
    os.setenv('DEV', 'ON')
end

local helpers = {}

local ok, cartridge_helpers = pcall(require, 'cartridge.test-helpers')
if not ok then
    log.error('Please, install cartridge rock to run tests')
    os.exit(1)
end

for name, value in pairs(cartridge_helpers) do
    helpers[name] = value
end

helpers.project_root = fio.dirname(debug.sourcedir())

local __fio_tempdir = fio.tempdir
fio.tempdir = function(base)
    base = base or os.getenv('TMPDIR')
    if base == nil or base == '/tmp' then
        return __fio_tempdir()
    else
        local random = digest.urandom(9)
        local suffix = digest.base64_encode(random, {urlsafe = true})
        local path = fio.pathjoin(base, 'tmp.cartridge.' .. suffix)
        fio.mktree(path)
        return path
    end
end

function helpers.entrypoint(name)
    local path = fio.pathjoin(
        helpers.project_root,
        'test', 'entrypoint',
        string.format('%s.lua', name)
    )
    if not fio.path.exists(path) then
        error(path .. ': no such entrypoint', 2)
    end
    return path
end

function helpers.table_keys(t)
    checks('table')
    local keys = {}
    for key in pairs(t) do
        table.insert(keys, key)
    end
    return keys
end

function helpers.get_results_list(results_map)
    checks('table')
    local results_list = {}
    for _, res in pairs(results_map) do
        table.insert(results_list, res)
    end
    return results_list
end

function helpers.box_cfg()
    if type(box.cfg) ~= 'function' then
        return
    end

    local tempdir = fio.tempdir()
    box.cfg({
        memtx_dir = tempdir,
        wal_mode = 'none',
    })
    fio.rmtree(tempdir)
end

function helpers.insert_objects(g, space_name, objects)
    local inserted_objects = {}

    for _, obj in ipairs(objects) do
        local result, err = g.cluster.main_server.net_box:call('crud.insert_object', {space_name, obj})

        t.assert_equals(err, nil)

        local objects, err = crud.unflatten_rows(result.rows, result.metadata)
        t.assert_equals(err, nil)
        t.assert_equals(#objects, 1)
        table.insert(inserted_objects, objects[1])
    end

    return inserted_objects
end

function helpers.get_objects_by_idxs(objects, idxs)
    local results = {}
    for _, idx in ipairs(idxs) do
        table.insert(results, objects[idx])
    end
    return results
end

function helpers.stop_cluster(cluster)
    assert(cluster ~= nil)
    cluster:stop()
    fio.rmtree(cluster.datadir)
end

function helpers.drop_space_on_cluster(cluster, space_name)
    assert(cluster ~= nil)
    for _, server in ipairs(cluster.servers) do
        server.net_box:eval([[
            local space_name = ...
            local space = box.space[space_name]
            if space ~= nil and not box.cfg.read_only then
                space:drop()
            end
        ]], {space_name})
    end
end

function helpers.truncate_space_on_cluster(cluster, space_name)
    assert(cluster ~= nil)
    for _, server in ipairs(cluster.servers) do
        server.net_box:eval([[
            local space_name = ...
            local space = box.space[space_name]
            if space ~= nil and not box.cfg.read_only then
                space:truncate()
            end
        ]], {space_name})
    end
end

function helpers.reset_sequence_on_cluster(cluster, sequence_name)
    assert(cluster ~= nil)
    for _, server in ipairs(cluster.servers) do
        server.net_box:eval([[
            local sequence_name = ...
            local sequence = box.sequence[sequence_name]
            if sequence ~= nil and not box.cfg.read_only then
                sequence:reset()
            end
        ]], {sequence_name})
    end
end

function helpers.get_test_replicasets()
    return {
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
    }
end

function helpers.call_on_servers(cluster, aliases, func)
    for _, alias in ipairs(aliases) do
        local server = cluster:server(alias)
        func(server)
    end
end

-- Call given function for each server with the 'crud-storage'
-- role.
--
-- 'func' accepts a server object, a replicaset config and all
-- arguments passed after 'func'.
--
-- Usage example:
--
--  | local res = {}
--  | helpers.call_on_storages(g.cluster, function(server, replicaset, res)
--  |     local instance_res = server.net_box:call(<...>)
--  |     res[replicaset.alias] = res[replicaset.alias] + instance_res
--  | end)
--  | t.assert_equals(res, {['s-1'] = 5, ['s-2'] = 6})
function helpers.call_on_storages(cluster, func, ...)
    -- Accumulate storages into a map from the storage alias to
    -- the replicaset object. Only storages, skip other instances.
    --
    -- Example:
    --
    --  | {
    --  |     ['s1-master'] = {
    --  |         alias = 's-1',
    --  |         roles = <...>,
    --  |         servers = {
    --  |             {
    --  |                 alias = 's1-master',
    --  |                 env = <...>,
    --  |                 instance_uuid = <...>,
    --  |             },
    --  |             <...>
    --  |         },
    --  |         uuid = <...>,
    --  |     }
    --  |     ['s1-replica'] = <...>,
    --  |     ['s2-master'] = <...>,
    --  |     ['s2-replica'] = <...>,
    --  | }
    --
    -- NB: The 'servers' field contains server configs. They are
    -- not the same as server objects: say, there is no 'net_box'
    -- field here.
    local alias_map = {}
    for _, replicaset in ipairs(cluster.replicasets) do
        -- Whether it is a storage replicaset?
        local has_crud_storage_role = false
        for _, role in ipairs(replicaset.roles) do
            if role == 'crud-storage' then
                has_crud_storage_role = true
                break
            end
        end

        -- If so, add servers of the replicaset into the mapping.
        if has_crud_storage_role then
            for _, server in ipairs(replicaset.servers) do
                alias_map[server.alias] = replicaset
            end
        end
    end

    -- Call given function for each storage node.
    for _, server in ipairs(cluster.servers) do
        local replicaset_alias = alias_map[server.alias]
        if replicaset_alias ~= nil then
            func(server, replicaset_alias, ...)
        end
    end
end

function helpers.assert_ge(actual, expected, message)
    if not (actual >= expected) then
        local err = string.format('expected: %s >= %s', actual, expected)
        if message ~= nil then
            err = message .. '\n' .. err
        end
        error(err, 2)
    end
end

function helpers.get_other_storage_bucket_id(cluster, bucket_id)
    return cluster.main_server.net_box:eval([[
        local vshard = require('vshard')

        local bucket_id = ...

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
    ]], {bucket_id})
end

function helpers.tarantool_version_at_least(wanted_major, wanted_minor,
        wanted_patch)
    -- Borrowed from `determine_enabled_features()` from
    -- crud/common/utils.lua.
    local major_minor_patch = _TARANTOOL:split('-', 1)[1]
    local major_minor_patch_parts = major_minor_patch:split('.', 2)

    local major = tonumber(major_minor_patch_parts[1])
    local minor = tonumber(major_minor_patch_parts[2])
    local patch = tonumber(major_minor_patch_parts[3])

    if major < (wanted_major or 0) then return false end
    if major > (wanted_major or 0) then return true end

    if minor < (wanted_minor or 0) then return false end
    if minor > (wanted_minor or 0) then return true end

    if patch < (wanted_patch or 0) then return false end
    if patch > (wanted_patch or 0) then return true end

    return true
end

function helpers.update_sharding_key_cache(cluster, space_name)
    return cluster.main_server.net_box:eval([[
        local sharding_key = require('crud.common.sharding_key')

        local space_name = ...
        return sharding_key.update_cache(space_name)
    ]], {space_name})
end

function helpers.get_sharding_key_cache(cluster)
    return cluster.main_server.net_box:eval([[
        local vshard = require('vshard')
        local sharding_metadata_cache = require('crud.common.sharding.router_metadata_cache')

        local vshard_router = vshard.router.static
        local cache = sharding_metadata_cache.get_instance(vshard_router)

        return cache[sharding_metadata_cache.SHARDING_KEY_MAP_NAME]
    ]])
end

-- it is not possible to get function or table with function
-- object through net.box that's why we get a sign of record
-- existence of cache but not the cache itself
function helpers.update_sharding_func_cache(cluster, space_name)
    return cluster.main_server.net_box:eval([[
        local sharding_func = require('crud.common.sharding_func')

        local space_name = ...
        local sharding_func, err = sharding_func.update_cache(space_name)
        if sharding_func == nil then
            return false, err
        end

        return true, err
    ]], {space_name})
end

-- it is not possible to get function or table with function
-- object through net.box that's why we get size of cache
-- but not the cache itself
function helpers.get_sharding_func_cache_size(cluster)
    return cluster.main_server.net_box:eval([[
        local vshard = require('vshard')
        local sharding_metadata_cache = require('crud.common.sharding.router_metadata_cache')

        local vshard_router = vshard.router.static
        local instance_cache = sharding_metadata_cache.get_instance(vshard_router)

        local cache, err = instance_cache[sharding_metadata_cache.SHARDING_FUNC_MAP_NAME]
        if cache == nil then
            return nil, err
        end

        local cnt = 0
        for _, _ in pairs(cache) do
            cnt = cnt + 1
        end

        return cnt, err
    ]])
end

function helpers.simple_functions_params()
    return {
        sleep_time = 0.01,
        error = { err = 'err' },
        error_msg = 'throw me',
    }
end

function helpers.prepare_simple_functions(router)
    local params = helpers.simple_functions_params()

    local _, err = router:eval([[
        local clock = require('clock')
        local fiber = require('fiber')

        local params = ...
        local sleep_time = params.sleep_time
        local error_table = params.error
        local error_msg = params.error_msg

        -- Using `fiber.sleep(time)` between two `clock.monotonic()`
        -- may return diff less than `time`.
        sleep_for = function(time)
            local start = clock.monotonic()
            while (clock.monotonic() - start) < time do
                fiber.sleep(time / 10)
            end
        end

        return_true = function(space_name)
            sleep_for(sleep_time)
            return true
        end

        return_err = function(space_name)
            sleep_for(sleep_time)
            return nil, error_table
        end

        throws_error = function()
            sleep_for(sleep_time)
            error(error_msg)
        end
    ]], { params })

    t.assert_equals(err, nil)
end

function helpers.is_space_exist(router, space_name)
    local res, err = router:eval([[
        local vshard = require('vshard')
        local utils = require('crud.common.utils')

        local space, err = utils.get_space(..., vshard.router.routeall())
        if err ~= nil then
            return nil, err
        end
        return space ~= nil
    ]], { space_name })

    t.assert_equals(err, nil)
    return res
end

function helpers.reload_package(srv)
    srv.net_box:eval([[
        local function startswith(text, prefix)
            return text:find(prefix, 1, true) == 1
        end

        for k, _ in pairs(package.loaded) do
            if startswith(k, 'crud') then
                package.loaded[k] = nil
            end
        end

        crud = require('crud')
    ]])
end

function helpers.reload_roles(srv)
    local ok, err = srv.net_box:eval([[
        return require('cartridge.roles').reload()
    ]])

    t.assert_equals({ok, err}, {true, nil})
end

function helpers.get_map_reduces_stat(router, space_name)
    return router:eval([[
        local stats = require('crud').stats(...)
        if stats.select == nil then
            return 0
        end
        return stats.select.details.map_reduces
    ]], { space_name })
end

function helpers.disable_dev_checks()
    os.setenv('DEV', 'OFF')
end

function helpers.count_on_replace_triggers(server, space_name)
    return server:eval([[
        local space = box.space[...]
        assert(space ~= nil)
        return #space:on_replace()
    ]], {space_name})
end

-- 'Timeout exceeded' or 'timed out'.
--
-- See https://github.com/tarantool/tarantool/pull/6538.
function helpers.assert_timeout_error(value, message)
    t.assert_type(value, 'string', nil, 2)

    local err_1 = 'Timeout exceeded'
    local err_2 = 'timed out'

    if string.find(value, err_1) or string.find(value, err_2) then
        return
    end

    local err = string.format('Could not find %q or %q in string %q', err_1,
        err_2, value)
    if message ~= nil then
        err = message .. '\n' .. err
    end
    error(err, 2)
end

function helpers.fflush_main_server_stdout(cluster, capture)
    -- Sometimes we have a delay here. This hack helps to wait for the end of
    -- the output. It shouldn't take much time.
    cluster.main_server.net_box:eval([[
        require('log').error("crud fflush stdout message")
    ]])
    local captured = ""
    while not string.find(captured, "crud fflush stdout message", 1, true) do
        captured = captured .. (capture:flush().stdout or "")
    end
    return captured
end

function helpers.complement_tuples_batch_with_operations(tuples, operations)

    local tuples_operation_data = {}
    for i, tuple in ipairs(tuples) do
        table.insert(tuples_operation_data, {tuple, operations[i]})
    end

    return tuples_operation_data
end

return helpers
