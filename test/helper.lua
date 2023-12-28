require('strict').on()

local t = require('luatest')
local vtest = require('test.vshard_helpers.vtest')

local luatest_capture = require('luatest.capture')
local luatest_helpers = require('luatest.helpers')
local luatest_utils = require('luatest.utils')

local checks = require('checks')
local digest = require('digest')
local fio = require('fio')

local crud = require('crud')
local crud_utils = require('crud.common.utils')
local cartridge_installed, cartridge_helpers = pcall(require, 'cartridge.test-helpers')

if os.getenv('TARANTOOL_CRUD_ENABLE_INTERNAL_CHECKS') == nil then
    os.setenv('TARANTOOL_CRUD_ENABLE_INTERNAL_CHECKS', 'ON')
end

local helpers = {
    backend = {
        VSHARD = 'vshard',
        CARTRIDGE = 'cartridge',
    },
}

local function is_cartridge_supported()
    local tarantool_version = luatest_utils.get_tarantool_version()
    local unsupported_version = luatest_utils.version(3, 0, 0)
    return not luatest_utils.version_ge(tarantool_version, unsupported_version)
end

local function is_cartridge_installed()
    return cartridge_installed
end

if is_cartridge_supported() and is_cartridge_installed() then
    for name, value in pairs(cartridge_helpers) do
        helpers[name] = value
    end
else
    for name, value in pairs(luatest_helpers) do
        helpers[name] = value
    end
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

function helpers.entrypoint_cartridge(name)
    local path = fio.pathjoin(
        helpers.project_root,
        'test', 'entrypoint', name, 'cartridge_init.lua'
    )
    if not fio.path.exists(path) then
        error(path .. ': no such cartridge entrypoint', 2)
    end
    return path
end

local function entrypoint_vshard(name, entrypoint, err)
    local path = fio.pathjoin(
        'test', 'entrypoint', name, entrypoint
    )
    if not fio.path.exists(path .. ".lua") then
        if err then
            error(path .. ': no such entrypoint', 2)
        end
        return nil, false
    end
    return path, true
end

function helpers.entrypoint_vshard_storage(name)
    return entrypoint_vshard(name, 'storage_init', true)
end

function helpers.entrypoint_vshard_router(name)
    return entrypoint_vshard(name, 'router_init', true)
end

function helpers.entrypoint_vshard_all(name)
    return entrypoint_vshard(name, 'all_init', true)
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

function helpers.stop_cartridge_cluster(cluster)
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

function helpers.get_test_cartridge_replicasets()
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
                { instance_uuid = helpers.uuid('b', 10), alias = 's1-replica' },
            },
        },
        {
            uuid = helpers.uuid('c'),
            alias = 's-2',
            roles = { 'customers-storage', 'crud-storage' },
            servers = {
                { instance_uuid = helpers.uuid('c', 1), alias = 's2-master' },
                { instance_uuid = helpers.uuid('c', 10), alias = 's2-replica' },
            },
        }
    }
end

function helpers.get_test_vshard_sharding()
    local sharding = {
        ['s-1'] = {
            replicas = {
                ['s1-master'] = {
                    instance_uuid = helpers.uuid('b', 1),
                    master = true,
                },
                ['s1-replica'] = {
                    instance_uuid = helpers.uuid('b', 10),
                },
            },
        },
        ['s-2'] = {
            replicas = {
                ['s2-master'] = {
                    instance_uuid = helpers.uuid('c', 1),
                    master = true,
                },
                ['s2-replica'] = {
                    instance_uuid = helpers.uuid('c', 10),
                },
            },
        },
    }
    return sharding
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
    if cluster.replicasets ~= nil then
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
    else
        for _, server in ipairs(cluster.servers) do
            if server.vtest and server.vtest.is_storage then
                func(server, server.vtest.replicaset, ...)
            end
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

        local other_replicaset
        for _, replicaset in pairs(replicasets) do
            local stat, err = replicaset:callrw('vshard.storage.bucket_stat', {bucket_id})

            if err ~= nil and err.name == 'WRONG_BUCKET' then
                other_replicaset = replicaset
                break
            end

            if err ~= nil then
                return nil, string.format(
                    'vshard.storage.bucket_stat returned unexpected error: %s',
                    require('json').encode(err)
                )
            end
        end

        if other_replicaset == nil then
            return nil, 'Other replicaset is not found'
        end

        local buckets_info = other_replicaset:callrw('vshard.storage.buckets_info')
        local res_bucket_id = next(buckets_info)

        return res_bucket_id
    ]], {bucket_id})
end

helpers.tarantool_version_at_least = crud_utils.tarantool_version_at_least

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

        local space, err = utils.get_space(..., vshard.router)
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
    os.setenv('TARANTOOL_CRUD_ENABLE_INTERNAL_CHECKS', 'OFF')
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

function helpers.get_command_log(router, backend, call, args)
    local capture
    local logfile
    if backend == helpers.backend.CARTRIDGE then
        capture = luatest_capture:new()
        capture:enable()
    elseif backend == helpers.backend.VSHARD then
        local logpath = router.net_box:eval('return box.cfg.log')
        logfile = fio.open(logpath, {'O_RDONLY', 'O_NONBLOCK'})
        logfile:read()
    end

    local _, err = router.net_box:call(call, args)
    if err ~= nil then
        if backend == helpers.backend.CARTRIDGE then
            capture:disable()
        elseif backend == helpers.backend.VSHARD then
            logfile:close()
        end
        return nil, err
    end

    -- Sometimes we have a delay here. This hack helps to wait for the end of
    -- the output. It shouldn't take much time.
    router.net_box:eval([[
        require('log').error("crud fflush message")
    ]])
    local captured = ""
    while not string.find(captured, "crud fflush message", 1, true) do
        if backend == helpers.backend.CARTRIDGE then
            captured = captured .. (capture:flush().stdout or "")
        elseif backend == helpers.backend.VSHARD then
            captured = captured .. (logfile:read() or "")
        end
    end

    if backend == helpers.backend.CARTRIDGE then
        capture:disable()
    elseif backend == helpers.backend.VSHARD then
        logfile:close()
    end
    return captured, nil
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

function helpers.is_metrics_0_12_0_or_older()
    local metrics = require('metrics')

    -- metrics 0.13.0 introduced VERSION, but it is likely to be deprecated in the future:
    -- https://github.com/tarantool/metrics/commit/a7e666f50d23c3e1a11b9bc9882edddec2f4c67e
    -- metrics 0.16.0 introduced _VERSION which is likely going to replace VERSION:
    -- https://github.com/tarantool/metrics/commit/8f9b667f9db59ceff8e5d26e458244e2d67838da
    if (metrics.VERSION == nil) and metrics._VERSION == nil then
        return true
    end

    return false
end

function helpers.skip_cartridge_unsupported()
    t.skip_if(not is_cartridge_supported(),
        "Cartridge is not supported on Tarantool 3+")
    t.skip_if(not is_cartridge_installed(),
        "Cartridge is not installed")
end

function helpers.skip_not_cartridge_backend(backend)
    t.skip_if(backend ~= helpers.backend.CARTRIDGE, "The test is for cartridge only")
end

function helpers.is_cartridge_hotreload_supported()
    return crud_utils.is_cartridge_hotreload_supported()
end

function helpers.skip_old_tarantool_cartridge_hotreload()
    -- Cartridge hotreload tests stuck for vshard 0.1.22+ on Tarantool 1.10.6, 2.2, 2.3 and 2.4.
    -- Logs display a lot of following errors:
    -- main/137/vshard.recovery util.lua:103 E> recovery_f has been failed: .../.rocks/share/tarantool/vshard/storage/init.lua:1268: assertion failed!
    -- main/136/vshard.gc util.lua:103 E> gc_bucket_f has been failed: .../.rocks/share/tarantool/vshard/storage/init.lua:2530: assertion failed!
    local tarantool_version = luatest_utils.get_tarantool_version()
    t.skip_if(luatest_utils.version_ge(luatest_utils.version(1, 10, 13), tarantool_version),
        "Cartridge hotreload tests stuck for vshard 0.1.22+ on Tarantool 1.10.6")
    t.skip_if(luatest_utils.version_ge(tarantool_version, luatest_utils.version(2, 0, 0))
          and luatest_utils.version_ge(luatest_utils.version(2, 5, 1), tarantool_version),
        "Cartridge hotreload tests stuck for vshard 0.1.22+ on Tarantool 2.2, 2.3 and 2.4")
end

function helpers.start_default_cluster(g, srv_name)
    local cartridge_cfg = {
        datadir = fio.tempdir(),
        server_command = helpers.entrypoint_cartridge(srv_name),
        use_vshard = true,
        replicasets = helpers.get_test_cartridge_replicasets(),
    }
    local vshard_cfg = {
        sharding = helpers.get_test_vshard_sharding(),
        bucket_count = 3000,
        storage_init = entrypoint_vshard(srv_name, 'storage_init', false),
        router_init = entrypoint_vshard(srv_name, 'router_init', false),
        all_init = entrypoint_vshard(srv_name, 'all_init', false),
        crud_init = true,
    }

    helpers.start_cluster(g, cartridge_cfg, vshard_cfg)
end

function helpers.start_cluster(g, cartridge_cfg, vshard_cfg)
    if g.params.backend == helpers.backend.CARTRIDGE then
        helpers.skip_cartridge_unsupported()

        local cfg = table.deepcopy(cartridge_cfg)
        cfg.env = {
            ['ENGINE'] = g.params.engine
        }

        g.cfg = cfg
        g.cluster = helpers.Cluster:new(cfg)
        g.cluster:start()
    elseif g.params.backend == helpers.backend.VSHARD then
        local cfg = table.deepcopy(vshard_cfg)
        cfg.engine = g.params.engine

        g.cfg = vtest.config_new(cfg, g.params.backend_cfg)
        vtest.cluster_new(g, g.cfg)
        g.cfg.engine = nil
    end
end

function helpers.stop_cluster(cluster, backend)
    if backend == helpers.backend.CARTRIDGE then
        helpers.stop_cartridge_cluster(cluster)
    elseif backend == helpers.backend.VSHARD then
        cluster:drop()
    end
end

function helpers.get_router(cluster, backend)
    if backend == helpers.backend.CARTRIDGE then
        return cluster:server('router')
    elseif backend == helpers.backend.VSHARD then
        return cluster.main_server
    end
end

function helpers.parse_module_version(str)
    -- https://github.com/tarantool/luatest/blob/f37b353b77be50a1f1ce87c1ff2edf0c1b96d5d1/luatest/utils.lua#L166-L173
    local splitstr = str:split('.')
    local major = tonumber(splitstr[1]:match('%d+'))
    local minor = tonumber(splitstr[2]:match('%d+'))
    local patch = tonumber(splitstr[3]:match('%d+'))
    return luatest_utils.version(major, minor, patch)
end

function helpers.is_name_supported_as_vshard_id()
    local vshard_version = helpers.parse_module_version(require('vshard')._VERSION)
    local is_vshard_supports = luatest_utils.version_ge(vshard_version,
        luatest_utils.version(0, 1, 25))

    local tarantool_version = luatest_utils.get_tarantool_version()
    local is_tarantool_supports = luatest_utils.version_ge(tarantool_version,
        luatest_utils.version(3, 0, 0))
    return is_vshard_supports and is_tarantool_supports
end

function helpers.backend_matrix(base_matrix)
    base_matrix = base_matrix or {{}}
    local backend_params = {
        {
            backend = helpers.backend.CARTRIDGE,
            backend_cfg = nil,
        },
    }

    if helpers.is_name_supported_as_vshard_id() then
        table.insert(backend_params, {
            backend = helpers.backend.VSHARD,
            backend_cfg = {identification_mode = 'uuid_as_key'},
        })
        table.insert(backend_params, {
            backend = helpers.backend.VSHARD,
            backend_cfg = {identification_mode = 'name_as_key'},
        })
    else
        table.insert(backend_params, {
            backend = helpers.backend.VSHARD,
            backend_cfg = nil,
        })
    end

    local matrix = {}
    for _, params in ipairs(backend_params) do
        for _, base in ipairs(base_matrix) do
            base = table.deepcopy(base)
            base.backend = params.backend
            base.backend_cfg = params.backend_cfg
            table.insert(matrix, base)
        end
    end
    return matrix
end

function helpers.schema_compatibility(schema)
    -- https://github.com/tarantool/tarantool/issues/4091
    if not helpers.tarantool_version_at_least(2, 2, 1) then
        for _, s in pairs(schema) do
            for _, i in pairs(s.indexes) do
                i.unique = false
            end
        end
    end

    -- https://github.com/tarantool/tarantool/commit/17c9c034933d726925910ce5bf8b20e8e388f6e3
    if not helpers.tarantool_version_at_least(2, 8, 1) then
        for _, s in pairs(schema) do
            for _, i in pairs(s.indexes) do
                for _, p in pairs(i.parts) do
                    p.exclude_null = nil
                end
            end
        end
    end

    return schema
end

function helpers.string_replace(base, old_fragment, new_fragment)
    local i, j = base:find(old_fragment)

    if i == nil then
        return base
    end

    local prefix = ''
    if i > 1 then
        prefix = base:sub(1, i - 1)
    end

    local suffix = ''
    if j < base:len() then
        suffix = base:sub(j + 1, base:len())
    end

    return prefix .. new_fragment .. suffix
end

function helpers.assert_str_contains_pattern_with_replicaset_id(str, pattern)
    local uuid_pattern = "%w+%-0000%-0000%-0000%-00000000000%d"
    local name_pattern = "s%-%d" -- All existing test clusters use this pattern, but it may change in the future.

    local found = false
    for _, id_pattern in pairs({uuid_pattern, name_pattern}) do
        -- pattern is expected to be like "Failed for [replicaset_id]".
        local full_pattern = helpers.string_replace('[replicaset_id]', id_pattern)
        if str:find(full_pattern) ~= nil then
            found = true
            break
        end
    end

    t.assert(found, ("string %q does not contain pattern %q"):format(str, pattern))
end

return helpers
