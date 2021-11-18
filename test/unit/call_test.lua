local fio = require('fio')

local t = require('luatest')
local g = t.group('call')

local helpers = require('test.helper')

g.before_all = function()
    g.cluster = helpers.Cluster:new({
        datadir = fio.tempdir(),
        server_command = helpers.entrypoint('srv_say_hi'),
        use_vshard = true,
        replicasets = {
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
                roles = { 'crud-storage' },
                servers = {
                    { instance_uuid = helpers.uuid('b', 1), alias = 's1-master' },
                    { instance_uuid = helpers.uuid('b', 2), alias = 's1-replica' },
                },
            },
            {
                uuid = helpers.uuid('c'),
                alias = 's-2',
                roles = { 'crud-storage' },
                servers = {
                    { instance_uuid = helpers.uuid('c', 1), alias = 's2-master' },
                    { instance_uuid = helpers.uuid('c', 2), alias = 's2-replica' },
                },
            }
        },
    })
    g.cluster:start()

    g.clear_vshard_calls = function()
        g.cluster.main_server.net_box:call('clear_vshard_calls')
    end

    g.get_vshard_calls = function()
        return g.cluster.main_server.net_box:eval('return _G.vshard_calls')
    end

    -- patch vshard.router.call* functions
    local vshard_call_names = {'callro', 'callbro', 'callre', 'callbre', 'callrw'}
    g.cluster.main_server.net_box:call('patch_vshard_calls', {vshard_call_names})
end

g.after_all = function()
    g.cluster:stop()
    fio.rmtree(g.cluster.datadir)
end

g.test_map_non_existent_func = function()
    local results, err = g.cluster.main_server.net_box:eval([[
        local call = require('crud.common.call')
        return call.map('non_existent_func', nil, {mode = 'write'})
    ]])

    t.assert_equals(results, nil)
    t.assert_str_contains(err.err, "Failed for %w+%-0000%-0000%-0000%-000000000000", true)
    t.assert_str_contains(err.err, "Function non_existent_func is not registered")
end

g.test_single_non_existent_func = function()
    local results, err = g.cluster.main_server.net_box:eval([[
        local call = require('crud.common.call')
        return call.single(1, 'non_existent_func', nil, {mode = 'write'})
    ]])

    t.assert_equals(results, nil)
    t.assert_str_contains(err.err, "Failed for %w+%-0000%-0000%-0000%-000000000000", true)
    t.assert_str_contains(err.err, "Function non_existent_func is not registered")
end

g.test_map_no_args = function()
    local results_map, err  = g.cluster.main_server.net_box:eval([[
        local call = require('crud.common.call')
        return call.map('say_hi_politely', nil, {mode = 'write'})
    ]])

    t.assert_equals(err, nil)
    local results = helpers.get_results_list(results_map)
    t.assert_equals(#results, 2)
    t.assert_items_include(results, {{"HI, handsome! I am s1-master"}, {"HI, handsome! I am s2-master"}})
end

g.test_args = function()
    local results_map, err = g.cluster.main_server.net_box:eval([[
        local call = require('crud.common.call')
        return call.map('say_hi_politely', {'dokshina'}, {mode = 'write'})
    ]])

    t.assert_equals(err, nil)
    local results = helpers.get_results_list(results_map)
    t.assert_equals(#results, 2)
    t.assert_items_include(results, {{"HI, dokshina! I am s1-master"}, {"HI, dokshina! I am s2-master"}})
end

g.test_timeout = function()
    local timeout = 0.2

    local results, err = g.cluster.main_server.net_box:eval([[
        local call = require('crud.common.call')

        local say_hi_timeout, call_timeout = ...

        return call.map('say_hi_sleepily', {say_hi_timeout}, {
            mode = 'write',
            timeout = call_timeout,
        })
    ]], {timeout + 0.1, timeout})

    t.assert_equals(results, nil)
    t.assert_str_contains(err.err, "Failed for %w+%-0000%-0000%-0000%-000000000000", true)
    t.assert_str_contains(err.err, "Timeout exceeded")
end

local function check_single_vshard_call(g, exp_vshard_call, opts)
    g.clear_vshard_calls()
    local _, err = g.cluster.main_server.net_box:eval([[
        local call = require('crud.common.call')
        local opts = ...
        return call.single(1, 'say_hi_politely', {'dokshina'}, opts)
    ]], {opts})
    t.assert_equals(err, nil)
    local vshard_calls = g.get_vshard_calls()
    t.assert_equals(vshard_calls, {exp_vshard_call})
end

local function check_map_vshard_call(g, exp_vshard_call, opts)
    g.clear_vshard_calls()
    local _, err = g.cluster.main_server.net_box:eval([[
        local call = require('crud.common.call')
        local opts = ...
        return call.map('say_hi_politely', {'dokshina'}, opts)
    ]], {opts})
    t.assert_equals(err, nil)
    local vshard_calls = g.get_vshard_calls()
    t.assert_equals(vshard_calls, {exp_vshard_call, exp_vshard_call})
end

g.test_single_vshard_calls = function()
    -- mode: write

    check_single_vshard_call(g, 'callrw', {
        mode = 'write',
    })

    -- mode: read

    -- not prefer_replica, not balance -> callro
    check_single_vshard_call(g, 'callro', {
        mode = 'read',
    })
    check_single_vshard_call(g, 'callro', {
        mode = 'read', prefer_replica = false, balance = false,
    })

    -- not prefer_replica, balance -> callbro
    check_single_vshard_call(g, 'callbro', {
        mode = 'read', balance = true,
    })
    check_single_vshard_call(g, 'callbro', {
        mode = 'read', prefer_replica = false, balance = true,
    })

    -- prefer_replica, not balance -> callre
    check_single_vshard_call(g, 'callre', {
        mode = 'read', prefer_replica = true,
    })
    check_single_vshard_call(g, 'callre', {
        mode = 'read', prefer_replica = true, balance = false,
    })

    -- prefer_replica, balance -> callbre
    check_single_vshard_call(g, 'callbre', {
        mode = 'read', prefer_replica = true, balance = true,
    })
end

g.test_map_vshard_calls = function()
    -- mode: write

    check_map_vshard_call(g, 'callrw', {
        mode = 'write'
    })

    -- mode: read

    -- not prefer_replica, not balance -> callro
    check_map_vshard_call(g, 'callro', {
        mode = 'read',
    })
    check_map_vshard_call(g, 'callro', {
        mode = 'read', prefer_replica = false, balance = false,
    })

    -- -- not prefer_replica, balance -> callbro
    check_map_vshard_call(g, 'callbro', {
        mode = 'read', balance = true,
    })
    check_map_vshard_call(g, 'callbro', {
        mode = 'read', prefer_replica = false, balance = true,
    })

    -- prefer_replica, not balance -> callre
    check_map_vshard_call(g, 'callre', {
        mode = 'read', prefer_replica = true,
    })
    check_map_vshard_call(g, 'callre', {
        mode = 'read', prefer_replica = true, balance = false,
    })

    -- prefer_replica, balance -> callbre
    check_map_vshard_call(g, 'callbre', {
        mode = 'read', prefer_replica = true, balance = true,
    })
end

g.test_any_vshard_call = function()
    g.clear_vshard_calls()
    local results, err = g.cluster.main_server.net_box:eval([[
        local call = require('crud.common.call')
        return call.any('say_hi_politely', {'dude'}, {})
    ]])

    t.assert_equals(results, 'HI, dude! I am s2-master')
    t.assert_equals(err, nil)
end

g.test_any_vshard_call_timeout = function()
    local timeout = 0.2

    local results, err = g.cluster.main_server.net_box:eval([[
        local call = require('crud.common.call')

        local say_hi_timeout, call_timeout = ...

        return call.any('say_hi_sleepily', {say_hi_timeout}, {
            timeout = call_timeout,
        })
    ]], {timeout + 0.1, timeout})

    t.assert_equals(results, nil)
    t.assert_str_contains(err.err, "Failed for %w+%-0000%-0000%-0000%-000000000000", true)
    t.assert_str_contains(err.err, "Timeout exceeded")
end
