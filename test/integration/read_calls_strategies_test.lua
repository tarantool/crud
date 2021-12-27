local fio = require('fio')

local t = require('luatest')

local pgroup = t.group('read_calls_strategies', {
    -- mode: write
    {exp_vshard_call = 'callrw', mode = 'write'},

    -- mode: read

    -- no params -> callro
    {exp_vshard_call = 'callro'},

    -- not prefer_replica, not balance -> callro
    {exp_vshard_call = 'callro', prefer_replica = false, balance = false},

    -- not prefer_replica, balance -> callbro
    {exp_vshard_call = 'callbro', prefer_replica = false, balance = true},
    {exp_vshard_call = 'callbro', balance = true},

    -- prefer_replica, not balance -> callre
    {exp_vshard_call = 'callre', prefer_replica = true, balance = false},
    {exp_vshard_call = 'callre', prefer_replica = true},

    -- prefer_replica, balance -> callbre
    {exp_vshard_call = 'callbre', prefer_replica = true, balance = true},
})

local helpers = require('test.helper')

pgroup.before_all(function(g)
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

pgroup.after_all(function(g)
    helpers.stop_cluster(g.cluster)
end)

pgroup.before_each(function(g)
    helpers.truncate_space_on_cluster(g.cluster, 'customers')
end)

pgroup.test_get = function(g)
    g.clear_vshard_calls()
    local _, err = g.cluster.main_server.net_box:call('crud.get', {'customers', 1, {
        mode = g.params.mode,
        balance = g.params.balance,
        prefer_replica = g.params.prefer_replica
    }})
    t.assert_equals(err, nil)
    local vshard_calls = g.get_vshard_calls('call_single_impl')
    t.assert_equals(vshard_calls, {g.params.exp_vshard_call})
end

pgroup.test_select = function(g)
    g.clear_vshard_calls()
    local _, err = g.cluster.main_server.net_box:call('crud.select', {'customers', nil, {
        mode = g.params.mode,
        balance = g.params.balance,
        prefer_replica = g.params.prefer_replica
    }})
    t.assert_equals(err, nil)
    local vshard_calls = g.get_vshard_calls('call_impl')
    t.assert_equals(vshard_calls, {g.params.exp_vshard_call, g.params.exp_vshard_call})
end

pgroup.test_pairs = function(g)
    g.clear_vshard_calls()

    local opts = {
        mode = g.params.mode,
        balance = g.params.balance,
        prefer_replica = g.params.prefer_replica
    }

    local _, err = g.cluster.main_server.net_box:eval([[
        local crud = require('crud')

        local opts = ...
        for _, _ in crud.pairs('customers', nil, opts) do end
    ]], {opts})
    t.assert_equals(err, nil)
    local vshard_calls = g.get_vshard_calls('call_impl')
    t.assert_equals(vshard_calls, {g.params.exp_vshard_call, g.params.exp_vshard_call})
end
