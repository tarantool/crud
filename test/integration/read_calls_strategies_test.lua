local fio = require('fio')

local t = require('luatest')
local g = t.group('read_calls_strategies')

local helpers = require('test.helper')

g.before_all(function()
    g.cluster = helpers.Cluster:new({
        datadir = fio.tempdir(),
        server_command = helpers.entrypoint('srv_read_calls_strategies'),
        use_vshard = true,
        replicasets = helpers.get_test_replicasets(),
    })

    g.cluster:start()

    g.space_format = g.cluster.servers[2].net_box.space.customers:format()

    g.clear_vshard_calls = function()
        g.cluster.main_server.net_box:call('clear_vshard_calls')
    end

    g.get_vshard_calls = function()
        return g.cluster.main_server.net_box:eval('return _G.vshard_calls')
    end

    -- patch vshard.router.call* functions
    local vshard_call_names = {'callro', 'callbro', 'callre', 'callbre', 'callrw'}
    g.cluster.main_server.net_box:call('patch_vshard_calls', {vshard_call_names})
end)

g.after_all(function()
    helpers.stop_cluster(g.cluster)
end)

g.before_each(function()
    helpers.truncate_space_on_cluster(g.cluster, 'customers')
end)

local function check_get(g, exp_vshard_call, opts)
    g.clear_vshard_calls()
    local _, err = g.cluster.main_server.net_box:call('crud.get', {'customers', 1, opts})
    t.assert_equals(err, nil)
    local vshard_calls = g.get_vshard_calls('call_single_impl')
    t.assert_equals(vshard_calls, {exp_vshard_call})
end

local function check_select(g, exp_vshard_call, opts)
    g.clear_vshard_calls()
    local _, err = g.cluster.main_server.net_box:call('crud.select', {'customers', nil, opts})
    t.assert_equals(err, nil)
    local vshard_calls = g.get_vshard_calls('call_impl')
    t.assert_equals(vshard_calls, {exp_vshard_call, exp_vshard_call})
end

local function check_pairs(g, exp_vshard_call, opts)
    g.clear_vshard_calls()
    local _, err = g.cluster.main_server.net_box:eval([[
        local crud = require('crud')

        local opts = ...
        for _, _ in crud.pairs('customers', nil, opts) do end
    ]], {opts})
    t.assert_equals(err, nil)
    local vshard_calls = g.get_vshard_calls('call_impl')
    t.assert_equals(vshard_calls, {exp_vshard_call, exp_vshard_call})
end

g.test_get = function()
    -- mode: write
    check_get(g, 'callrw', {mode = 'write'})

    -- mode: read

    -- no params -> callro
    check_get(g, 'callro')

    -- not prefer_replica, not balance -> callro
    check_get(g, 'callro', {prefer_replica = false, balance = false})

    -- not prefer_replica, balance -> callbro
    check_get(g, 'callbro', {prefer_replica = false, balance = true})
    check_get(g, 'callbro', {balance = true})

    -- prefer_replica, not balance -> callre
    check_get(g, 'callre', {prefer_replica = true, balance = false})
    check_get(g, 'callre', {prefer_replica = true})

    -- prefer_replica, balance -> callbre
    check_get(g, 'callbre', {prefer_replica = true, balance = true})
end

g.test_select = function()
    -- mode: write
    check_select(g, 'callrw', {mode = 'write'})

    -- mode: read

    -- no params -> callro
    check_select(g, 'callro')

    -- not prefer_replica, not balance -> callro
    check_select(g, 'callro', {prefer_replica = false, balance = false})

    -- not prefer_replica, balance -> callbro
    check_select(g, 'callbro', {prefer_replica = false, balance = true})
    check_select(g, 'callbro', {balance = true})

    -- prefer_replica, not balance -> callre
    check_select(g, 'callre', {prefer_replica = true, balance = false})
    check_select(g, 'callre', {prefer_replica = true})

    -- prefer_replica, balance -> callbre
    check_select(g, 'callbre', {prefer_replica = true, balance = true})
end

g.test_pairs = function()
    -- mode: write
    check_pairs(g, 'callrw', {mode = 'write'})

    -- mode: read

    -- no params -> callro
    check_pairs(g, 'callro')

    -- not prefer_replica, not balance -> callro
    check_pairs(g, 'callro', {prefer_replica = false, balance = false})

    -- not prefer_replica, balance -> callbro
    check_pairs(g, 'callbro', {prefer_replica = false, balance = true})
    check_pairs(g, 'callbro', {balance = true})

    -- prefer_replica, not balance -> callre
    check_pairs(g, 'callre', {prefer_replica = true, balance = false})
    check_pairs(g, 'callre', {prefer_replica = true})

    -- prefer_replica, balance -> callbre
    check_pairs(g, 'callbre', {prefer_replica = true, balance = true})
end
