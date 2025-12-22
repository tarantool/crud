local t = require('luatest')

local helpers = require('test.helper')

local pgroup = t.group('call', helpers.backend_matrix())

pgroup.before_all(function(g)
    helpers.start_default_cluster(g, 'srv_say_hi')

    g.clear_vshard_calls = function()
        g.router:call('clear_vshard_calls')
    end

    g.get_vshard_calls = function()
        return g.router:eval('return _G.vshard_calls')
    end

    -- patch vshard.router.call* functions
    local vshard_call_names = {'callro', 'callbro', 'callre', 'callbre', 'callrw'}
    g.router:call('patch_vshard_calls', {vshard_call_names})
end)

pgroup.after_all(function(g)
    helpers.stop_cluster(g.cluster, g.params.backend)
end)

pgroup.test_map_non_existent_func = function(g)
    local results, err = g.router:eval([[
        local vshard = require('vshard')
        local call = require('crud.common.call')

        return call.map(vshard.router.static, 'non_existent_func', nil, {mode = 'write'})
    ]])

    t.assert_equals(results, nil)
    helpers.assert_str_contains_pattern_with_replicaset_id(err.err, "Failed for [replicaset_id]")
    t.assert_str_contains(err.err, "Function 'non_existent_func' is not registered")
end

pgroup.test_single_non_existent_func = function(g)
    local results, err = g.router:eval([[
        local vshard = require('vshard')
        local call = require('crud.common.call')

        return call.single(vshard.router.static, 1, 'non_existent_func', nil, {mode = 'write'})
    ]])

    t.assert_equals(results, nil)
    helpers.assert_str_contains_pattern_with_replicaset_id(err.err, "Failed for [replicaset_id]")
    t.assert_str_contains(err.err, "Function 'non_existent_func' is not registered")
end

pgroup.test_map_invalid_mode = function(g)
    local results, err = g.router:eval([[
        local vshard = require('vshard')
        local call = require('crud.common.call')

        return call.map(vshard.router.static, 'say_hi_politely', nil, {mode = 'invalid'})
    ]])

    t.assert_equals(results, nil)
    t.assert_str_contains(err.err, "Unknown call mode: invalid")
end

pgroup.test_single_invalid_mode = function(g)
    local results, err = g.router:eval([[
        local vshard = require('vshard')
        local call = require('crud.common.call')

        return call.single(vshard.router.static, 1, 'say_hi_politely', nil, {mode = 'invalid'})
    ]])

    t.assert_equals(results, nil)
    t.assert_str_contains(err.err, "Unknown call mode: invalid")
end

pgroup.test_map_no_args = function(g)
    local results_map, err  = g.router:eval([[
        local vshard = require('vshard')
        local call = require('crud.common.call')

        return call.map(vshard.router.static, 'say_hi_politely', nil, {mode = 'write'})
    ]])

    t.assert_equals(err, nil)
    local results = helpers.get_results_list(results_map)
    t.assert_equals(#results, 2)
    t.assert_items_include(results, {{"HI, handsome! I am 1"}, {"HI, handsome! I am 1"}})
end

pgroup.test_args = function(g)
    local results_map, err = g.router:eval([[
        local vshard = require('vshard')
        local call = require('crud.common.call')

        return call.map(vshard.router.static, 'say_hi_politely', {'dokshina'}, {mode = 'write'})
    ]])

    t.assert_equals(err, nil)
    local results = helpers.get_results_list(results_map)
    t.assert_equals(#results, 2)
    t.assert_items_include(results, {{"HI, dokshina! I am 1"}, {"HI, dokshina! I am 1"}})
end

pgroup.test_timeout = function(g)
    local timeout = 0.2

    local results, err = g.router:eval([[
        local vshard = require('vshard')
        local call = require('crud.common.call')

        local say_hi_timeout, call_timeout = ...

        return call.map(vshard.router.static, 'say_hi_sleepily', {say_hi_timeout}, {
            mode = 'write',
            timeout = call_timeout,
        })
    ]], {timeout + 0.1, timeout})

    t.assert_equals(results, nil)
    helpers.assert_str_contains_pattern_with_replicaset_id(err.err, "Failed for [replicaset_id]")
    helpers.assert_timeout_error(err.err)
end

local function check_single_vshard_call(g, exp_vshard_call, opts)
    g.clear_vshard_calls()
    local _, err = g.router:eval([[
        local vshard = require('vshard')
        local call = require('crud.common.call')

        local opts = ...
        return call.single(vshard.router.static, 1, 'say_hi_politely', {'dokshina'}, opts)
    ]], {opts})
    t.assert_equals(err, nil)
    local vshard_calls = g.get_vshard_calls()
    t.assert_equals(vshard_calls, {exp_vshard_call})
end

local function check_map_vshard_call(g, exp_vshard_call, opts)
    g.clear_vshard_calls()
    local _, err = g.router:eval([[
        local vshard = require('vshard')
        local call = require('crud.common.call')

        local opts = ...
        return call.map(vshard.router.static, 'say_hi_politely', {'dokshina'}, opts)
    ]], {opts})
    t.assert_equals(err, nil)
    local vshard_calls = g.get_vshard_calls()
    t.assert_equals(vshard_calls, {exp_vshard_call, exp_vshard_call})
end

pgroup.test_single_vshard_calls = function(g)
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

pgroup.test_map_vshard_calls = function(g)
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

pgroup.test_any_vshard_call = function(g)
    g.clear_vshard_calls()
    local results, err = g.router:eval([[
        local vshard = require('vshard')
        local call = require('crud.common.call')

        return call.any(vshard.router.static, 'say_hi_politely', {'dude'}, {})
    ]])

    t.assert_equals(results, 'HI, dude! I am 1')
    t.assert_equals(err, nil)
end

pgroup.test_any_vshard_call_timeout = function(g)
    helpers.call_on_storages(g.cluster, function(server)
        server.net_box:eval([[
            require('crud.common.rebalance').safe_mode_disable()
        ]])
    end)
    local timeout = 0.2

    local results, err = g.router:eval([[
        local vshard = require('vshard')
        local call = require('crud.common.call')

        local say_hi_timeout, call_timeout = ...

        return call.any(vshard.router.static, 'say_hi_sleepily', {say_hi_timeout}, {
            timeout = call_timeout,
        })
    ]], {timeout + 0.1, timeout})

    t.assert_equals(results, nil)
    helpers.assert_str_contains_pattern_with_replicaset_id(err.err, "Failed for [replicaset_id]")
    helpers.assert_timeout_error(err.err)
end
