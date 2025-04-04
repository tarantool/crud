require('strict').on()

local t = require('luatest')

local vtest = require('test.vshard_helpers.vtest')
local vclock_utils = require('test.vshard_helpers.vclock')
local tarantool3_config = require('test.tarantool3_helpers.config')
local tarantool3_cluster = require('test.tarantool3_helpers.cluster')

local luatest_capture = require('luatest.capture')
local luatest_helpers = require('luatest.helpers')
local luatest_utils = require('luatest.utils')

local checks = require('checks')
local clock = require('clock')
local digest = require('digest')
local fiber = require('fiber')
local json = require('json')
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
        CONFIG = 'config',
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

function helpers.entrypoint_vshard(name, entrypoint, err)
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
    return helpers.entrypoint_vshard(name, 'storage', true)
end

function helpers.entrypoint_vshard_router(name)
    return helpers.entrypoint_vshard(name, 'router', true)
end

function helpers.entrypoint_vshard_all(name)
    return helpers.entrypoint_vshard(name, 'all', true)
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

function helpers.box_cfg(opts)
    opts = opts or {}

    if opts.wait_rw == nil then
        opts.wait_rw = true
    end

    if type(box.cfg) ~= 'function' then
        return
    end

    local tempdir = fio.tempdir()
    box.cfg({
        memtx_dir = tempdir,
        wal_mode = 'none',
        listen = opts.listen,
    })
    fio.rmtree(tempdir)

    if opts.wait_rw then
        t.helpers.retrying(
            {timeout = 60, delay = 0.1},
            t.assert_equals, box.info.ro, false
        )
    end
end

function helpers.insert_objects(g, space_name, objects)
    local inserted_objects = {}

    for _, obj in ipairs(objects) do
        local result, err = g.router:call('crud.insert_object', {space_name, obj})

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

function helpers.get_test_config_groups()
    local groups = {
        routers = {
            sharding = {
                roles = {'router'},
            },
            roles = {'roles.crud-router'},
            replicasets = {
                ['router'] = {
                    leader = 'router',
                    instances = {
                        ['router'] = {},
                    },
                },
            },
        },
        storages = {
            sharding = {
                roles = {'storage'},
            },
            roles = {'roles.crud-storage'},
            replicasets = {
                ['s-1'] = {
                    leader = 's1-master',
                    instances = {
                        ['s1-master'] = {},
                        ['s1-replica'] = {},
                    },
                },
                ['s-2'] = {
                    leader = 's2-master',
                    instances = {
                        ['s2-master'] = {},
                        ['s2-replica'] = {},
                    },
                },
            },
        },
    }
    return groups
end

function helpers.call_on_servers(cluster, aliases, func)
    for _, alias in ipairs(aliases) do
        local server = cluster:server(alias)
        func(server)
    end
end

function helpers.exec_on_cluster(cluster, func, ...)
    for _, server in ipairs(cluster.servers) do
        server:exec(func, ...)
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
    if cluster.replicasets ~= nil then -- CARTRIDGE backend
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
    elseif cluster.config ~= nil then -- CONFIG backend
        for _, group in pairs(cluster.config.groups) do
            for rs_id, rs in pairs(group.replicasets) do
                for alias, _ in pairs(rs.instances) do
                    local server = cluster:server(alias)

                    if server:is_storage() then
                        func(server, rs_id, ...)
                    end
                end
            end
        end
    else -- VSHARD backend
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
    return cluster:server('router'):eval([[
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
    return cluster:server('router'):eval([[
        local sharding_key = require('crud.common.sharding_key')

        local space_name = ...
        return sharding_key.update_cache(space_name)
    ]], {space_name})
end

function helpers.get_sharding_key_cache(cluster)
    return cluster:server('router'):eval([[
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
    return cluster:server('router'):eval([[
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
    return cluster:server('router'):eval([[
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

local function inherit(self, object)
    setmetatable(object, self)
    self.__index = self
    return object
end

-- Implements log capture from log file.
local FileCapture = {inherit = inherit}

function FileCapture:new(object)
    checks('table', {
        server = 'table',
    })

    self:inherit(object)
    object:initialize()
    return object
end

function FileCapture:initialize()
    local box_cfg_log = self.server:exec(function()
        return box.cfg.log
    end)

    local logpath = helpers.string_replace(box_cfg_log, 'file:', '')

    local relative_logpath = logpath:sub(1, 1) ~= '/'
    if relative_logpath then
        logpath = fio.pathjoin(self.server.chdir, logpath)
    end

    local logfile, err = fio.open(logpath, {'O_RDONLY', 'O_NONBLOCK'})
    assert(err == nil, err)

    logfile:read()

    self.logfile = logfile
end

function FileCapture:read()
    return self.logfile:read()
end

function FileCapture:close()
    self.logfile:close()
end

-- Implements wrapper over built-in Luatest capture.
local LuatestCapture = {inherit = inherit}

function LuatestCapture:new()
    local object = {}
    self:inherit(object)
    object:initialize()
    return object
end

function LuatestCapture:initialize()
    self.capture = luatest_capture:new()
    self.capture:enable()
end

function LuatestCapture:read()
    return self.capture:flush().stdout
end

function LuatestCapture:close()
    self.capture:disable()
end

local function assert_class_implements_interface(class, interface)
    for _, method in ipairs(interface) do
        assert(type(class[method]) == 'function', ('class implements %q method'):format(method))
    end
end

local Capture = {
    'new',
    'read',
    'close',
}

assert_class_implements_interface(FileCapture, Capture)
assert_class_implements_interface(LuatestCapture, Capture)

function helpers.get_command_log(router, call, args)
    local capture = LuatestCapture:new()

    local _, err = router.net_box:call(call, args)
    if err ~= nil then
        capture:close()
        return nil, err
    end

    -- Sometimes we have a delay here. This hack helps to wait for the end of
    -- the output. It shouldn't take much time.
    router.net_box:eval([[
        require('log').error("crud fflush message")
    ]])
    local captured = ""
    local start_time = fiber.time()
    local timeout = 2.0

    while not string.find(captured, "crud fflush message", 1, true) do
        captured = captured .. (capture:read() or "")
        if fiber.time() - start_time > timeout then
            capture:close()
            t.skip("Log message not received in time")
        end
        fiber.sleep(0.01)
    end

    capture:close()
    return captured, nil
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

    -- Flaky tests for Tarantool 1.10.15.
    -- s2-replica | E> ER_READONLY: Can't modify data because this instance is in read-only mode.
    -- s2-replica | E> ApplyConfigError: Can't modify data because this instance is in read-only mode.
    t.skip_if(luatest_utils.version_ge(luatest_utils.version(2, 0, 0), tarantool_version),
        "Flaky tests for Tarantool 1.10.15")
end

function helpers.build_default_cartridge_cfg(srv_name)
    return {
        datadir = fio.tempdir(),
        server_command = helpers.entrypoint_cartridge(srv_name),
        use_vshard = true,
        replicasets = helpers.get_test_cartridge_replicasets(),
    }
end

function helpers.build_default_vshard_cfg(srv_name)
    return {
        sharding = helpers.get_test_vshard_sharding(),
        bucket_count = 3000,
        storage_entrypoint = helpers.entrypoint_vshard(srv_name, 'storage', false),
        router_entrypoint = helpers.entrypoint_vshard(srv_name, 'router', false),
        all_entrypoint = helpers.entrypoint_vshard(srv_name, 'all', false),
        crud_init = true,
    }
end

function helpers.build_default_tarantool3_cluster_cfg(srv_name)
    return {
        groups = helpers.get_test_config_groups(),
        bucket_count = 3000,
        storage_entrypoint = helpers.entrypoint_vshard(srv_name, 'storage', false),
        router_entrypoint = helpers.entrypoint_vshard(srv_name, 'router', false),
        all_entrypoint = helpers.entrypoint_vshard(srv_name, 'all', false),
    }
end

function helpers.start_default_cluster(g, srv_name)
    local cartridge_cfg = helpers.build_default_cartridge_cfg(srv_name)
    local vshard_cfg =  helpers.build_default_vshard_cfg(srv_name)
    local tarantool3_cluster_cfg = helpers.build_default_tarantool3_cluster_cfg(srv_name)

    helpers.start_cluster(g, cartridge_cfg, vshard_cfg, tarantool3_cluster_cfg)
end

function helpers.start_cartridge_cluster(g, cfg)
    local cfg = table.deepcopy(cfg)
    cfg.env = {
        ['ENGINE'] = (g.params and g.params.engine)
    }

    g.cfg = cfg
    g.cluster = helpers.Cluster:new(cfg)

    for k, server in ipairs(g.cluster.servers) do
        local mt = getmetatable(server)

        local extended_mt = table.deepcopy(mt)
        extended_mt.__index = vclock_utils.extend_with_vclock_methods(extended_mt.__index)

        g.cluster.servers[k] = setmetatable(server, extended_mt)
    end
    g.cluster.main_server = g.cluster.servers[1]

    g.cluster:start()
end

function helpers.start_vshard_cluster(g, cfg)
    local cfg = table.deepcopy(cfg)
    cfg.engine = (g.params and g.params.engine)

    g.cfg = vtest.config_new(cfg, g.params.backend_cfg)
    vtest.cluster_new(g, g.cfg)
    g.cfg.engine = nil
end

function helpers.start_tarantool3_cluster(g, cfg)
    local cfg = table.deepcopy(cfg)
    cfg.env = {
        ['ENGINE'] = (g.params and g.params.engine),
    }
    cfg = tarantool3_config.new(cfg)

    g.cfg = cfg
    g.cluster = tarantool3_cluster:new(cfg)
    g.cluster:start()
end

function helpers.start_cluster(g, cartridge_cfg, vshard_cfg, tarantool3_cluster_cfg, opts)
    checks('table', '?table', '?table', '?table', {
        wait_crud_is_ready = '?boolean',
        backend = '?string',
        retries = '?number',
    })

    opts = opts or {}

    if opts.wait_crud_is_ready == nil then
        opts.wait_crud_is_ready = true
    end

    if opts.backend == nil then
        opts.backend = g.params.backend
    end
    assert(opts.backend ~= nil, 'Please, provide backend')

    local DEFAULT_RETRIES = 3
    if opts.retries == nil then
        opts.retries = DEFAULT_RETRIES
    end

    local current_attempt = 0
    while true do
        current_attempt = current_attempt + 1

        if opts.backend == helpers.backend.CARTRIDGE then
            helpers.skip_cartridge_unsupported()

            helpers.start_cartridge_cluster(g, cartridge_cfg)
        elseif opts.backend == helpers.backend.VSHARD then
            helpers.start_vshard_cluster(g, vshard_cfg)
        elseif opts.backend == helpers.backend.CONFIG then
            helpers.skip_if_tarantool3_crud_roles_unsupported()

            helpers.start_tarantool3_cluster(g, tarantool3_cluster_cfg)
        end

        g.router = g.cluster:server('router')
        assert(g.router ~= nil, 'router found')

        local ok, err = false, nil -- luacheck: ignore
        if opts.wait_crud_is_ready then
            ok, err = pcall(helpers.wait_crud_is_ready_on_cluster, g, {backend = opts.backend})
        else
            ok = true
        end

        if ok then
            break
        end

        helpers.stop_cluster(g.cluster, opts.backend)

        if current_attempt == opts.retries then
            error(err)
        end
    end
end

local function count_storages_in_topology(g, backend, vshard_group, storage_roles)
    local storages_in_topology = 0
    if backend == helpers.backend.CARTRIDGE then
        for _, replicaset in ipairs(g.cfg.replicasets) do
            local is_storage = helpers.does_replicaset_have_one_of_cartridge_roles(replicaset, storage_roles)
            local is_part_of_vshard_group = replicaset.vshard_group == vshard_group

            if is_storage and is_part_of_vshard_group then
                storages_in_topology = storages_in_topology + #replicaset.servers
            end
        end
    elseif backend == helpers.backend.VSHARD then
        for _, storage_replicaset in pairs(g.cfg.sharding) do
            for _, _ in pairs(storage_replicaset.replicas) do
                storages_in_topology = storages_in_topology + 1
            end
        end
    elseif backend == helpers.backend.CONFIG then
        error('not implemented yet')
    end

    return storages_in_topology
end

function helpers.does_replicaset_have_one_of_cartridge_roles(replicaset, expected_roles)
    for _, actual_role in ipairs(replicaset.roles) do
        for _, expected_role in ipairs(expected_roles) do
            if expected_role == actual_role then
                return true
            end
        end
    end

    return false
end

local function assert_expected_number_of_storages_is_running(g, vshard_group, expected_number)
    local res, err = g.router:call('crud.storage_info', {{vshard_router = vshard_group}})
    assert(
        err == nil,
        ('crud is not bootstrapped: error on getting storage info: %s'):format(err)
    )

    local running_storages = 0
    for _, storage in pairs(res) do
        if storage.status == 'running' then
            running_storages = running_storages + 1
        end
    end

    assert(
        running_storages == expected_number,
        ('crud is not bootstrapped: expected %d running storages, got the following storage info: %s'):format(
            expected_number, json.encode(res))
    )

    return true
end

function helpers.wait_crud_is_ready_on_cluster(g, opts)
    checks('table', {
        backend = '?string',
        vshard_group = '?string',
        storage_roles = '?table',
    })

    opts = opts or {}

    if opts.backend == nil then
        opts.backend = (g.params and g.params.backend)
    end
    assert(opts.backend ~= nil)

    if opts.storage_roles == nil then
        opts.storage_roles = {'crud-storage'}
    end

    local default_impl = function()
        local storages_in_topology = count_storages_in_topology(
            g,
            opts.backend,
            opts.vshard_group,
            opts.storage_roles
        )

        local WAIT_TIMEOUT = 60
        local DELAY = 0.1
        t.helpers.retrying(
            {timeout = WAIT_TIMEOUT, delay = DELAY},
            assert_expected_number_of_storages_is_running,
            g, opts.vshard_group, storages_in_topology
        )
    end

    if g.cluster.wait_crud_is_ready_on_cluster ~= nil then
        g.cluster:wait_crud_is_ready_on_cluster()
    else
        default_impl()
    end
end

function helpers.stop_cluster(cluster, backend)
    if backend == helpers.backend.CARTRIDGE then
        helpers.stop_cartridge_cluster(cluster)
    elseif backend == helpers.backend.VSHARD then
        cluster:drop()
    elseif backend == helpers.backend.CONFIG then
        cluster:drop()
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

function helpers.is_master_discovery_supported_in_vshard()
    local vshard_version = helpers.parse_module_version(require('vshard')._VERSION)
    local is_vshard_supports = luatest_utils.version_ge(vshard_version,
        luatest_utils.version(0, 1, 25))

    local tarantool_version = luatest_utils.get_tarantool_version()
    local is_tarantool_supports = luatest_utils.version_ge(tarantool_version,
        luatest_utils.version(3, 0, 0))
    return is_vshard_supports and is_tarantool_supports
end

function helpers.remove_array_keys(arr, keys)
    local ascending_keys = table.deepcopy(keys)
    table.sort(ascending_keys)

    for i = #ascending_keys, 1, -1 do
        local key = ascending_keys[i]
        table.remove(arr, key)
    end

    return arr
end

function helpers.extend_vshard_matrix(backend_params, backend_cfg_key, backend_cfg_vals, opts)
    assert(type(opts) == 'table')
    assert(opts.mode == 'replace' or opts.mode == 'extend')

    local old_vshard_backend_keys = {}
    local old_vshard_backends = {}

    for k, v in ipairs(backend_params) do
        if v.backend == helpers.backend.VSHARD then
            table.insert(old_vshard_backend_keys, k)
            table.insert(old_vshard_backends, v)
        end
    end

    if opts.mode == 'replace' then
        helpers.remove_array_keys(backend_params, old_vshard_backend_keys)
    end

    for _, v in ipairs(old_vshard_backends) do
        for _, cfg_v in ipairs(backend_cfg_vals) do
            local new_v = table.deepcopy(v)

            new_v.backend_cfg = new_v.backend_cfg or {}
            new_v.backend_cfg[backend_cfg_key] = cfg_v

            table.insert(backend_params, new_v)
        end
    end

    return backend_params
end

function helpers.is_cartridge_suite_supported()
    local is_module_provided = pcall(require, 'cartridge')

    local tarantool_version = luatest_utils.get_tarantool_version()
    local is_tarantool_supports = not luatest_utils.version_ge(tarantool_version,
        luatest_utils.version(3, 0, 0))
    return is_module_provided and is_tarantool_supports
end

function helpers.backend_matrix(base_matrix)
    base_matrix = base_matrix or {{}}
    local backend_params = {
        {
            backend = helpers.backend.VSHARD,
            backend_cfg = nil,
        },
    }

    if helpers.is_cartridge_suite_supported() then
        table.insert(backend_params,
            {
                backend = helpers.backend.CARTRIDGE,
                backend_cfg = nil,
            }
        )
    end

    if helpers.is_name_supported_as_vshard_id() then
        backend_params = helpers.extend_vshard_matrix(
            backend_params,
            'identification_mode',
            {'uuid_as_key', 'name_as_key'},
            {mode = 'replace'}
        )
    end

    if helpers.is_master_discovery_supported_in_vshard() then
        backend_params = helpers.extend_vshard_matrix(
            backend_params,
            'master',
            {'auto'},
            {mode = 'extend'}
        )
    end

    if helpers.is_tarantool3_crud_roles_supported() then
        table.insert(backend_params,
            {
                backend = helpers.backend.CONFIG,
                backend_cfg = nil,
            }
        )
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

function helpers.prepare_ordered_data(g, space, expected_objects, bucket_id, order_condition)
    helpers.insert_objects(g, space, expected_objects)

    local resp, err = g.router:call('crud.select', {
        space,
        {order_condition},
        {bucket_id = bucket_id, mode = 'write'},
    })
    t.assert_equals(err, nil)

    local objects = crud.unflatten_rows(resp.rows, resp.metadata)
    t.assert_equals(objects, expected_objects)
end

function helpers.is_decimal_supported()
    return crud_utils.tarantool_supports_decimals()
end

function helpers.is_uuid_supported()
    return crud_utils.tarantool_supports_uuids()
end

function helpers.is_datetime_supported()
    return crud_utils.tarantool_supports_datetimes()
end

function helpers.is_interval_supported()
    return crud_utils.tarantool_supports_intervals()
end

function helpers.skip_decimal_unsupported()
    t.skip_if(not helpers.is_decimal_supported(), 'decimal is not supported')
end

function helpers.skip_datetime_unsupported()
    t.skip_if(not helpers.is_datetime_supported(), 'datetime is not supported')
end

function helpers.skip_interval_unsupported()
    t.skip_if(not helpers.is_interval_supported(), 'interval is not supported')
end

function helpers.merge_tables(t1, t2, ...)
    if t2 == nil then
        return t1
    end

    local res = {}

    for k, v in pairs(t1) do
        res[k] = v
    end

    for k, v in pairs(t2) do
        res[k] = v
    end

    return helpers.merge_tables(res, ...)
end

function helpers.wait_cluster_replication_finished(g)
    if g.params.backend == helpers.backend.CARTRIDGE then
        for _, replicaset in ipairs(g.cfg.replicasets) do
            local server_names = {}
            for _, server in ipairs(replicaset.servers) do
                table.insert(server_names, server.alias)
            end

            helpers.wait_replicaset_replication_finished(g, server_names)
        end
    elseif g.params.backend == helpers.backend.VSHARD then
        for _, storage_replicaset in pairs(g.cfg.sharding) do
            local server_names = {}
            for name, _ in pairs(storage_replicaset.replicas) do
                table.insert(server_names, name)
            end

            helpers.wait_replicaset_replication_finished(g, server_names)
        end
    end
end

function helpers.wait_replicaset_replication_finished(g, server_names)
    for _, etalon_server_name in ipairs(server_names) do
        local etalon_server = g.cluster:server(etalon_server_name)

        for _, current_server_name in ipairs(server_names) do
            local current_server = g.cluster:server(current_server_name)

            if current_server ~= etalon_server then
                current_server:wait_vclock_of(etalon_server)
            end
        end
    end
end

function helpers.is_lua_persistent_func_supported()
    -- https://github.com/tarantool/tarantool/commit/200a492aa771e50af86a4754b41a5e373fa7a354
    local tarantool_version = luatest_utils.get_tarantool_version()
    return luatest_utils.version_ge(tarantool_version, luatest_utils.version(2, 2, 1))
end

helpers.SCHEMA_INIT_STARTED_FLAG = '_schema_init_started'
helpers.SCHEMA_READY_FLAG = '_is_schema_ready'

function helpers.set_func_flag(flag)
    box.schema.func.create(flag, {
        language = 'LUA',
        body = 'function() return true end',
        if_not_exists = true,
    })
end

function helpers.is_func_flag(flag)
    local status, ready = pcall(box.schema.func.call, flag)
    return status and ready
end

function helpers.wait_func_flag(flag)
    local TIMEOUT = 60
    local start = clock.monotonic()

    while clock.monotonic() - start < TIMEOUT do
        if helpers.is_func_flag(flag) then
            return true
        end

        fiber.sleep(0.05)
    end

    error('timeout while waiting for a flag')
end

function helpers.wrap_schema_init(init_func)
    -- Do not implement waiting for Tarantool 1.10.
    if not helpers.is_lua_persistent_func_supported() then
        return function()
            if box.info.ro then
                return
            end

            init_func()
        end
    end

    local function wrapped_init_func()
        if box.info.ro then
            return
        end

        -- Do not call init several times: it may break the tests which alter schema.
        if helpers.is_func_flag(helpers.SCHEMA_INIT_STARTED_FLAG) then
            return
        end

        helpers.set_func_flag(helpers.SCHEMA_INIT_STARTED_FLAG)

        init_func()

        helpers.set_func_flag(helpers.SCHEMA_READY_FLAG)
    end

    if rawget(box, 'watch') ~= nil then
        return function()
            box.watch('box.status', wrapped_init_func)
        end
    else
        return function()
            fiber.create(function()
                fiber.self():name('schema_init')

                while true do
                    wrapped_init_func()

                    fiber.sleep(0.05)
                end
            end)
        end
    end
end

function helpers.wait_schema_init()
    -- Do not implement waiting for Tarantool 1.10.
    if not helpers.is_lua_persistent_func_supported() then
        return true
    end

    return helpers.wait_func_flag(helpers.SCHEMA_READY_FLAG)
end

function helpers.is_box_watch_supported()
    return crud_utils.tarantool_supports_box_watch()
end

function helpers.skip_if_box_watch_unsupported()
    t.skip_if(not helpers.is_box_watch_supported(), 'box.watch is not supported')
end

function helpers.skip_if_box_watch_supported()
    t.skip_if(helpers.is_box_watch_supported(), 'box.watch is supported')
end

function helpers.is_tarantool_config_supported()
    local tarantool_version = luatest_utils.get_tarantool_version()
    return luatest_utils.version_ge(tarantool_version, luatest_utils.version(3, 0, 0))
end

function helpers.skip_if_tarantool_config_unsupported()
    -- box.info.version fails before box.cfg on old versions.
    local version = rawget(_G, '_TARANTOOL')
    t.skip_if(not helpers.is_tarantool_config_supported(),
              ("Tarantool %s does not support starting from config"):format(version))
end

function helpers.is_tarantool3_crud_roles_supported()
    return crud_utils.tarantool_supports_config_get_inside_roles()
           and crud_utils.tarantool_role_privileges_not_revoked()
end

function helpers.skip_if_tarantool3_crud_roles_unsupported()
    -- box.info.version fails before box.cfg on old versions.
    local version = rawget(_G, '_TARANTOOL')
    t.skip_if(not helpers.is_tarantool3_crud_roles_supported(),
              ("Tarantool %s does not support crud roles"):format(version))
end

function helpers.skip_if_not_config_backend(backend)
    t.skip_if(backend ~= helpers.backend.CONFIG, "The test is for Tarantool 3 with config only")
end

function helpers.reset_call_cache(cluster)
    helpers.call_on_storages(cluster, function(server)
        server:exec(function()
            local call_cache = require('crud.common.call_cache')
            call_cache.reset()
        end)
    end)
end

return helpers
