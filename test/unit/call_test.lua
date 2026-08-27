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
    local vshard_call_names = {'callro', 'callbro', 'callre', 'callbre', 'callrw',
                               'update_master'}
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

    t.assert_str_contains(results, 'HI, dude!')
    t.assert_equals(err, nil)

    local vshard_calls = g.get_vshard_calls()
    t.assert_equals(vshard_calls, {'callro'})
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

pgroup.before_test('test_any_vshard_call_fallback', function(g)
    -- Mock callro to fail permanently on a single replicaset.
    -- This guarantees that call.any triggers its fallback loop.
    g.router:eval([[
        local vshard = require('vshard')
        local errors = require('errors')

        local replicasets = vshard.router.static:routeall()

        rawset(_G, '__original_callros', {})
        local originals = rawget(_G, '__original_callros')

        local ids = {}
        for replicaset_id in pairs(replicasets) do
            table.insert(ids, replicaset_id)
        end
        table.sort(ids)
        local broken_replicaset_id = ids[1]

        for replicaset_id, replicaset in pairs(replicasets) do
            originals[replicaset_id] = replicaset.callro

            if replicaset_id == broken_replicaset_id then
                replicaset.callro = function(self, ...)
                    return nil, errors.new_class('ClientError'):new('Temporary failure')
                end
            end
        end
    ]])
end)

pgroup.after_test('test_any_vshard_call_fallback', function(g)
    g.router:eval([[
        local vshard = require('vshard')

        local replicasets = vshard.router.static:routeall()
        local originals = rawget(_G, '__original_callros')

        for replicaset_id, original_fun in pairs(originals) do
            if replicasets[replicaset_id] ~= nil then
                replicasets[replicaset_id].callro = original_fun
            end
        end
        rawset(_G, '__original_callros', nil)
    ]])
end)

pgroup.test_any_vshard_call_fallback = function(g)
    g.clear_vshard_calls()

    local results, err = g.router:eval([[
        local vshard = require('vshard')
        local call = require('crud.common.call')

        return call.any(vshard.router.static, 'say_hi_politely', {'survivor'}, {})
    ]])

    -- Ensure call.any successfully routes around the initial failure
    -- and uses the available fallback.
    t.assert_str_contains(results, 'HI, survivor!')
    t.assert_equals(err, nil)
end

local function is_vshard_backend(g)
    return g.params.backend == helpers.backend.VSHARD
end

local function cfg_router_drop_masters(g)
    g.router:exec(function(cfg)
        local vshard = require('vshard')

        for _, rs in pairs(cfg.sharding) do
            rs.master = nil
            for _, replica in pairs(rs.replicas) do
                replica.master = nil
            end
        end
        vshard.router.cfg(cfg)
    end, {g.cfg})
end

local function cfg_router_move_masters_to_replicas(g)
    g.router:exec(function(cfg)
        local vshard = require('vshard')

        -- The master is marked with 'master' in a static config and with
        -- 'read_only = false' in the 'master = auto' one (test/vshard_helpers/vtest.lua).
        for _, rs in pairs(cfg.sharding) do
            local master_name
            for replica_name, replica in pairs(rs.replicas) do
                if replica.master or (replica.read_only ~= nil and not replica.read_only) then
                    master_name = replica_name
                    replica.master = nil
                    break
                end
            end
            for replica_name, replica in pairs(rs.replicas) do
                if replica_name ~= master_name then
                    replica.master = true
                    break
                end
            end
            rs.master = nil
        end
        vshard.router.cfg(cfg)
    end, {g.cfg})
end

local function cfg_router_restore(g)
    g.router:exec(function(cfg)
        local vshard = require('vshard')
        local fiber = require('fiber')

        vshard.router.cfg(cfg)

        -- The master search is asynchronous, but the next tests need masters.
        local deadline = fiber.clock() + 10
        while fiber.clock() < deadline do
            local is_found = true
            for _, rs in pairs(vshard.router.routeall()) do
                if rs.master == nil then
                    is_found = false
                end
            end
            if is_found then
                return
            end
            vshard.router.master_search_wakeup()
            fiber.sleep(0.05)
        end
        error('masters are not found after the router cfg restore')
    end, {g.cfg})
end


pgroup.before_test('test_single_retry_on_missing_master', function(g)
    if not is_vshard_backend(g) then
        return
    end
    -- The router cannot discover a bucket while the masters are gone, so the
    -- route is cached in advance: router_cfg() keeps the route map.
    g.router:exec(function()
        require('vshard').router.static:route(1)
    end)
    cfg_router_drop_masters(g)
    -- locate_master is patched and unpatched for this test only: with the
    -- master = 'auto' backend config the background master search calls it
    -- too, which would pollute the calls of other tests.
    g.router:call('patch_vshard_calls', {{'locate_master'}})
end)

pgroup.after_test('test_single_retry_on_missing_master', function(g)
    if not is_vshard_backend(g) then
        return
    end
    g.router:call('unpatch_vshard_calls', {{'locate_master'}})
    cfg_router_restore(g)
end)

pgroup.test_single_retry_on_missing_master = function(g)
    t.skip_if(not is_vshard_backend(g),
        'the router re-configuration is available on the vshard backend only')

    g.clear_vshard_calls()
    local results, err = g.router:eval([[
        local vshard = require('vshard')
        local call = require('crud.common.call')

        return call.single(vshard.router.static, 1, 'say_hi_politely', {'Jonh'},
            {mode = 'write'})
    ]])

    t.assert_equals(results, nil)
    t.assert(err ~= nil)
    helpers.assert_str_contains_pattern_with_replicaset_id(err.err, "Failed for [replicaset_id]")
    t.assert_str_contains(err.err, 'Master is not configured')

    -- The master discovery is performed, but the config has no master at all:
    -- nothing is found, so the call is not retried.
    t.assert_equals(g.get_vshard_calls(), {'callrw', 'locate_master'})
end

pgroup.before_test('test_single_non_master_no_retry', function(g)
    if not is_vshard_backend(g) then
        return
    end
    -- The router cannot discover a bucket while the masters are gone, so the
    -- route is cached in advance: router_cfg() keeps the route map.
    g.router:exec(function()
        require('vshard').router.static:route(1)
    end)
    helpers.set_safe_mode(g.cluster, true)
    cfg_router_move_masters_to_replicas(g)
end)

pgroup.after_test('test_single_non_master_no_retry', function(g)
    if not is_vshard_backend(g) then
        return
    end
    cfg_router_restore(g)
    helpers.set_safe_mode(g.cluster, g.params.safe_mode)
end)

pgroup.test_single_non_master_no_retry = function(g)
    t.skip_if(not is_vshard_backend(g),
        'the router re-configuration is available on the vshard backend only')

    g.clear_vshard_calls()
    local results, err = g.router:eval([[
        local vshard = require('vshard')
        local call = require('crud.common.call')

        return call.single(vshard.router.static, 1, 'bucket_ref_rw', {1}, {mode = 'write'})
    ]])

    t.assert_equals(results, nil)
    t.assert(err ~= nil)
    helpers.assert_str_contains_pattern_with_replicaset_id(err.err, "Failed for [replicaset_id]")
    t.assert_str_contains(err.err, 'NON_MASTER')

    -- The master is updated, but the retry makes no sense: the master is
    -- assigned to a replica until the configuration is changed.
    t.assert_equals(g.get_vshard_calls(), {'callrw', 'update_master'})
end

pgroup.before_test('test_single_wrong_bucket_retry_with_reroute', function(g)
    if not is_vshard_backend(g) then
        return
    end
    helpers.set_safe_mode(g.cluster, true)
end)

pgroup.after_test('test_single_wrong_bucket_retry_with_reroute', function(g)
    if not is_vshard_backend(g) then
        return
    end
    helpers.set_safe_mode(g.cluster, g.params.safe_mode)
end)

pgroup.test_single_wrong_bucket_retry_with_reroute = function(g)
    t.skip_if(not is_vshard_backend(g),
        'the router re-configuration is available on the vshard backend only')

    g.clear_vshard_calls()
    local results, err = g.router:eval([[
        local vshard = require('vshard')
        local call = require('crud.common.call')

        local bucket_count = ...

        local router = vshard.router.static
        local replicaset = router:route(1)
        local bucket_id
        for id = 1, bucket_count do
            if router:route(id) ~= replicaset then
                bucket_id = id
                break
            end
        end
        assert(bucket_id ~= nil, 'no bucket on another replicaset')

        -- The call is routed by bucket 1, but the bucket ref is taken for a
        -- bucket from another replicaset: the route is stale.
        return call.single(vshard.router.static, 1, 'bucket_ref_rw', {bucket_id},
            {mode = 'write'})
    ]], {g.cfg.bucket_count})

    t.assert_equals(err, nil)
    t.assert_equals(results, true)

    -- The bucket route is reset and the call is retried on the replicaset
    -- the bucket is routed to now.
    t.assert_equals(g.get_vshard_calls(), {'callrw', 'callrw'})
end

pgroup.before_test('test_map_retry_on_missing_master', function(g)
    if not is_vshard_backend(g) then
        return
    end
    cfg_router_drop_masters(g)
    -- See the comment in before_test('test_single_retry_on_missing_master').
    g.router:call('patch_vshard_calls', {{'locate_master'}})
end)

pgroup.after_test('test_map_retry_on_missing_master', function(g)
    if not is_vshard_backend(g) then
        return
    end
    g.router:call('unpatch_vshard_calls', {{'locate_master'}})
    cfg_router_restore(g)
end)

pgroup.test_map_retry_on_missing_master = function(g)
    t.skip_if(not is_vshard_backend(g),
        'the router re-configuration is available on the vshard backend only')

    g.clear_vshard_calls()
    local results, errs = g.router:eval([[
        local vshard = require('vshard')
        local call = require('crud.common.call')

        return call.map(vshard.router.static, 'say_hi_politely', {'Jonh'},
            {mode = 'write'})
    ]])

    t.assert_equals(results, nil)
    t.assert(errs ~= nil)
    helpers.assert_str_contains_pattern_with_replicaset_id(errs.err, "Failed for [replicaset_id]")
    t.assert_str_contains(errs.err, 'Master is not configured')

    -- The requests are async, so vshard does not search for a master itself.
    -- The master discovery is performed by crud, but the config has no master
    -- at all: the search finds nothing, so the call is not retried.
    -- The map call exits early on the first failed replicaset.
    t.assert_equals(g.get_vshard_calls(), {'callrw', 'locate_master'})
end
