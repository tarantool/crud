local fio = require('fio')
local helpers = require('test.helper')
local t = require('luatest')
local server = require('luatest.server')

local pgroup = t.group('not-initialized', helpers.backend_matrix({
    {},
}))

local vshard_cfg_template = {
    sharding = {
        {
            replicas = {
                storage = {
                    master = true,
                },
            },
        },
    },
    bucket_count = 20,
    storage_init = helpers.entrypoint_vshard_storage('srv_not_initialized'),
}

local cartridge_cfg_template = {
    datadir = fio.tempdir(),
    server_command = helpers.entrypoint_cartridge('srv_not_initialized'),
    use_vshard = true,
    replicasets = {
        {
            uuid = helpers.uuid('a'),
            alias = 'router',
            roles = { 'vshard-router' },
            servers = {
                { instance_uuid = helpers.uuid('a', 1), alias = 'router' },
            },
        },
        {
            uuid = helpers.uuid('b'),
            alias = 's-1',
            roles = { 'customers-storage' },
            servers = {
                { instance_uuid = helpers.uuid('b', 1), alias = 's1-master' },
            },
        },
    },
}

pgroup.before_all(function(g)
    helpers.start_cluster(g, cartridge_cfg_template, vshard_cfg_template)
end)

pgroup.after_all(function(g)
    helpers.stop_cluster(g.cluster, g.params.backend)
end)

pgroup.test_insert = function(g)
    local results, err = g.cluster.main_server.net_box:eval([[
        local crud = require('crud')
        return crud.insert('customers', {id = 1, name = 'Fedor', age = 15})
    ]])

    t.assert_equals(results, nil)
    t.assert_str_contains(err.err, "Failed for %w+%-0000%-0000%-0000%-00000000000%d", true)
    t.assert_str_contains(err.err, "crud isn't initialized on replicaset")
end

pgroup.test_no_box_cfg = function()
    t.assert_error_msg_contains('box.cfg() must be called first', function()
        require('crud').init_storage()
    end)
end

pgroup.before_test('test_no_vshard_storage_cfg', function(g)
    g.test_server = server:new({alias = 'master'})
    g.test_server:start({wait_until_ready = true})

    local appdir = fio.abspath(debug.sourcedir() .. '/../../')
    g.test_server:exec(function(appdir)
        if package.setsearchroot ~= nil then
            package.setsearchroot(appdir)
        else
            package.path = package.path .. appdir .. '/?.lua;'
            package.path = package.path .. appdir .. '/?/init.lua;'
            package.path = package.path .. appdir .. '/.rocks/share/tarantool/?.lua;'
            package.path = package.path .. appdir .. '/.rocks/share/tarantool/?/init.lua;'
            package.cpath = package.cpath .. appdir .. '/?.so;'
            package.cpath = package.cpath .. appdir .. '/?.dylib;'
            package.cpath = package.cpath .. appdir .. '/.rocks/lib/tarantool/?.so;'
            package.cpath = package.cpath .. appdir .. '/.rocks/lib/tarantool/?.dylib;'
        end
    end, {appdir})
end)

pgroup.test_no_vshard_storage_cfg = function(g)
    t.assert_error_msg_contains('vshard.storage.cfg() must be called first', function()
        g.test_server:exec(function()
            require('crud').init_storage()
        end)
    end)
end

pgroup.after_test('test_no_vshard_storage_cfg', function(g)
    g.test_server:stop()
    g.test_server = nil
end)
