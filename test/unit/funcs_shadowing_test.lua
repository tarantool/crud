local fio = require('fio')

local t = require('luatest')
local g = t.group('funcs-shadowing')

local helpers = require('test.helper')

g.before_all = function()
    g.cluster = helpers.Cluster:new({
        datadir = fio.tempdir(),
        server_command = helpers.entrypoint('srv_shadowing'),
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
                roles = { 'crud-storage' },
                servers = {
                    { instance_uuid = helpers.uuid('b', 1), alias = 's1-master' },
                },
            },
        },
    })
    g.cluster:start()
end

g.after_all = function()
    g.cluster:stop()
    fio.rmtree(g.cluster.datadir)
end

g.test_call_global = function()
    local results_map, err = g.cluster.main_server.net_box:eval([[
        local call = require('crud.common.call')
        return call.ro({
            func_name = 'global_func',
        })
    ]])

    t.assert_equals(err, nil)
    local results = helpers.get_results_list(results_map)
    t.assert_equals(results, {"global"})
end

g.test_call_registered = function()
    local results_map, err = g.cluster.main_server.net_box:eval([[
        local call = require('crud.common.call')
        return call.ro({
            func_name = 'registered_func',
        })
    ]])

    t.assert_equals(err, nil)
    local results = helpers.get_results_list(results_map)
    t.assert_equals(results, {"registered"})
end

g.test_call_common = function()
    local results_map, err = g.cluster.main_server.net_box:eval([[
        local call = require('crud.common.call')
        return call.ro({
            func_name = 'common_func',
        })
    ]])

    t.assert_equals(err, nil)
    local results = helpers.get_results_list(results_map)
    t.assert_equals(results, {"common-registered"})
end
