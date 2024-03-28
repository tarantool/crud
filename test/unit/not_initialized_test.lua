local fio = require('fio')
local helpers = require('test.helper')
local t = require('luatest')
local server = require('luatest.server')

local pgroup = t.group('not-initialized', helpers.backend_matrix({
    {},
}))

local vshard_cfg_template = {
    sharding = {
        storages = {
            replicas = {
                storage = {
                    master = true,
                },
            },
        },
    },
    bucket_count = 20,
    storage_entrypoint = helpers.entrypoint_vshard_storage('srv_not_initialized'),
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

local tarantool3_cluster_cfg_template = {
    groups = {
        routers = {
            sharding = {
                roles = {'router'},
            },
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
            replicasets = {
                ['s-1'] = {
                    leader = 's1-master',
                    instances = {
                        ['s1-master'] = {},
                    },
                },
            },
        },
    },
    bucket_count = 20,
    storage_entrypoint = helpers.entrypoint_vshard_storage('srv_not_initialized'),
}

pgroup.before_all(function(g)
    helpers.start_cluster(g,
        cartridge_cfg_template,
        vshard_cfg_template,
        tarantool3_cluster_cfg_template,
        {wait_crud_is_ready = false}
    )

    g.router = g.cluster:server('router')
end)

pgroup.after_all(function(g)
    helpers.stop_cluster(g.cluster, g.params.backend)
end)

pgroup.test_insert = function(g)
    local results, err = g.router:eval([[
        local crud = require('crud')
        return crud.insert('customers', {id = 1, name = 'Fedor', age = 15})
    ]])

    t.assert_equals(results, nil)
    helpers.assert_str_contains_pattern_with_replicaset_id(err.err, "Failed for [replicaset_id]")
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
            require('crud').init_storage{async = false}
        end)
    end)
end

pgroup.after_test('test_no_vshard_storage_cfg', function(g)
    g.test_server:stop()
    g.test_server = nil
end)
