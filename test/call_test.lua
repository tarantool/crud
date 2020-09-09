local fio = require('fio')

local t = require('luatest')
local g = t.group('call')

local helpers = require('test.helper')

g.before_all = function()
    g.cluster = helpers.Cluster:new({
        datadir = fio.tempdir(),
        server_command = helpers.entrypoint('srv_say_hi'),
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
                roles = { 'vshard-storage' },
                servers = {
                    { instance_uuid = helpers.uuid('b', 1), alias = 's1-master' },
                    { instance_uuid = helpers.uuid('b', 2), alias = 's1-replica' },
                },
            },
            {
                uuid = helpers.uuid('c'),
                alias = 's-2',
                roles = { 'vshard-storage' },
                servers = {
                    { instance_uuid = helpers.uuid('c', 1), alias = 's2-master' },
                    { instance_uuid = helpers.uuid('c', 2), alias = 's2-replica' },
                },
            }
        },
    })
    g.cluster:start()
end

g.after_all = function()
    g.cluster:stop()
    fio.rmtree(g.cluster.datadir)
end

g.test_non_existent_func = function()
    local results, err = g.cluster.main_server.net_box:eval([[
        local call = require('elect.call')
        return call.ro({
            func_name = 'non_existent_func',
        })
    ]])

    t.assert_equals(results, nil)
    t.assert_str_contains(err.err, "Failed for %w+%-0000%-0000%-0000%-000000000000", true)
    t.assert_str_contains(err.err, "Function non_existent_func is not registered")
end

g.test_no_args = function()
    local results_map, err  = g.cluster.main_server.net_box:eval([[
        local call = require('elect.call')
        return call.ro({
            func_name = 'say_hi_politely',
        })
    ]])

    t.assert_equals(err, nil)
    local results = helpers.get_results_list(results_map)
    t.assert_equals(#results, 2)
    t.assert_items_include(results, {"HI, handsome! I am s1-master", "HI, handsome! I am s2-master"})
end

g.test_args = function()
    local results_map, err = g.cluster.main_server.net_box:eval([[
        local call = require('elect.call')
        return call.ro({
            func_name = 'say_hi_politely',
            func_args = {'dokshina'},
        })
    ]])

    t.assert_equals(err, nil)
    local results = helpers.get_results_list(results_map)
    t.assert_equals(#results, 2)
    t.assert_items_include(results, {"HI, dokshina! I am s1-master", "HI, dokshina! I am s2-master"})
end

g.test_callrw = function()
    local results_map, err = g.cluster.main_server.net_box:eval([[
        local call = require('elect.call')
        return call.rw({
            func_name = 'say_hi_politely',
            func_args = {'dokshina'},
        })
    ]])

    t.assert_equals(err, nil)
    local results = helpers.get_results_list(results_map)
    t.assert_equals(#results, 2)
    t.assert_items_include(results, {"HI, dokshina! I am s1-master", "HI, dokshina! I am s2-master"})
end

g.test_timeout = function()
    local timeout = 0.2

    local results, err = g.cluster.main_server.net_box:eval(string.format([[
        local call = require('elect.call')
        return call.ro({
            func_name = 'say_hi_sleepily',
            func_args = {%s},
            timeout = %s,
        })
    ]], timeout + 0.1, timeout))

    t.assert_equals(results, nil)
    t.assert_str_contains(err.err, "Failed for %w+%-0000%-0000%-0000%-000000000000", true)
    t.assert_str_contains(err.err, "Timeout exceeded")
end
