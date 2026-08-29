local t = require('luatest')
local fiber = require('fiber')
local net_box = require('net.box')

local helpers = require('test.helper')

local group = t.group('storage_call', helpers.backend_matrix({{
    engine = 'memtx',
}}, {
    skip_safe_mode = true,
}))

local function install_test_functions()
    if not box.info.ro and rawget(_G, 'crud') ~= nil then
        box.session.su('admin', function()
            box.schema.user.create('storage_call_test_user', {
                password = 'secret',
                if_not_exists = true,
            })
            if _TARANTOOL >= '3.0.0' then
                box.schema.user.grant(
                    'storage_call_test_user', 'execute', 'lua_call',
                    'crud.storage_call', {if_not_exists = true}
                )
                box.schema.user.grant(
                    'storage_call_test_user', 'execute', 'lua_call',
                    'crud.storage_call_many', {if_not_exists = true}
                )
            else
                box.schema.func.create('crud.storage_call', {
                    if_not_exists = true,
                })
                box.schema.func.create('crud.storage_call_many', {
                    if_not_exists = true,
                })
                box.schema.user.grant(
                    'storage_call_test_user', 'execute', 'function',
                    'crud.storage_call', {if_not_exists = true}
                )
                box.schema.user.grant(
                    'storage_call_test_user', 'execute', 'function',
                    'crud.storage_call_many', {if_not_exists = true}
                )
            end
        end)
    end

    if box.space._bucket == nil then
        return
    end

    rawset(_G, 'storage_call_test_non_persistent', function()
        return true
    end)

    rawset(_G, 'storage_call_test_values', {})
    rawset(_G, 'storage_call_test_sleep_calls', 0)
    rawset(_G, 'storage_call_test_target_calls', 0)

    if box.info.ro then
        return
    end

    local test_func_bodies = {
        storage_call_test_returns = [[
            function(...)
                return select(1, ...), nil, false, box.info.id,
                    box.session.effective_user()
            end
        ]],
        storage_call_test_error = [[
            function()
                error('storage call test error')
            end
        ]],
        storage_call_test_unserializable = [[
            function()
                return function() end
            end
        ]],
        storage_call_test_order = [[
            function(value)
                table.insert(_G.storage_call_test_values, value)
                return #_G.storage_call_test_values
            end
        ]],
        storage_call_test_sleep = [[
            function(delay)
                _G.storage_call_test_sleep_calls =
                    _G.storage_call_test_sleep_calls + 1
                require('fiber').sleep(delay)
                return true
            end
        ]],
        storage_call_test_open_transaction = [[
            function()
                box.begin()
                return true
            end
        ]],
        storage_call_test_transaction_write_and_error = [[
            function(id, value)
                box.begin()
                box.space.storage_call_test_transactions:replace({
                    id, value,
                })
                error('storage call transaction error')
            end
        ]],
        storage_call_test_transaction_commit = [[
            function(id, value)
                box.begin()
                box.space.storage_call_test_transactions:replace({
                    id, value,
                })
                box.commit()
                return true
            end
        ]],
        storage_call_test_transaction_get = [[
            function(id)
                local tuple = box.space.storage_call_test_transactions:get({
                    id,
                })
                if tuple == nil then
                    return nil
                end
                return tuple[2]
            end
        ]],
        storage_call_test_counted = [[
            function(value)
                _G.storage_call_test_target_calls =
                    _G.storage_call_test_target_calls + 1
                return value
            end
        ]],
        storage_call_test_location = [[
            function(value)
                _G.storage_call_test_target_calls =
                    _G.storage_call_test_target_calls + 1
                return value, box.info.uuid
            end
        ]],
        storage_call_test_wait_for_release = [[
            function()
                _G.storage_call_test_wait_started = true
                while not _G.storage_call_test_wait_release do
                    require('fiber').sleep(0.001)
                end
                _G.storage_call_test_wait_finished = true
                return true
            end
        ]],
        storage_call_test_access_denied_inside = [[
            function()
                _G.storage_call_test_target_calls =
                    _G.storage_call_test_target_calls + 1
                return box.func.storage_call_test_error:call({})
            end
        ]],
    }
    for func_name, body in pairs(test_func_bodies) do
        box.schema.func.create(func_name, {
            body = body,
            is_sandboxed = false,
            if_not_exists = true,
        })
    end

    local transaction_space = box.schema.space.create(
        'storage_call_test_transactions',
        {if_not_exists = true}
    )
    transaction_space:format({
        {name = 'id', type = 'unsigned'},
        {name = 'value', type = 'string'},
    })
    transaction_space:create_index('primary', {if_not_exists = true})

    box.schema.func.create('storage_call_test_non_persistent', {
        if_not_exists = true,
    })
    box.schema.func.create('storage_call_test_effective_user', {
        body = [[
            function()
                return box.session.effective_user()
            end
        ]],
        is_sandboxed = false,
        setuid = true,
        if_not_exists = true,
    })

    box.session.su('admin', function()
        box.schema.user.create('storage_call_test_user', {
            password = 'secret',
            if_not_exists = true,
        })
        box.schema.user.grant(
            'storage_call_test_user', 'execute', 'function',
            'storage_call_test_returns', {if_not_exists = true}
        )
        box.schema.user.grant(
            'storage_call_test_user', 'execute', 'function',
            'storage_call_test_effective_user', {if_not_exists = true}
        )
        box.schema.user.grant(
            'storage_call_test_user', 'execute', 'function',
            'storage_call_test_access_denied_inside', {if_not_exists = true}
        )
        box.schema.user.grant(
            'storage_call_test_user', 'execute', 'function',
            '_crud.storage_call_on_storage', {if_not_exists = true}
        )
        box.schema.user.grant(
            'storage_call_test_user', 'execute', 'function',
            '_crud.storage_call_many_on_storage', {if_not_exists = true}
        )
    end)
end

local function reset_test_state()
    if box.space._bucket == nil then
        return
    end

    rawset(_G, 'storage_call_test_values', {})
    rawset(_G, 'storage_call_test_sleep_calls', 0)
    rawset(_G, 'storage_call_test_target_calls', 0)
    rawset(_G, 'storage_call_test_wait_started', false)
    rawset(_G, 'storage_call_test_wait_release', false)
    rawset(_G, 'storage_call_test_wait_finished', false)
    rawset(_G, 'storage_call_test_bucket_send_started', false)
    rawset(_G, 'storage_call_test_bucket_send_done', false)
    rawset(_G, 'storage_call_test_bucket_send_error', nil)
    if not box.info.ro and box.space.storage_call_test_transactions ~= nil then
        box.space.storage_call_test_transactions:truncate()
    end
end

local function get_buckets_from_different_replicasets(router)
    return router:eval([[
        local utils = require('crud.common.utils')
        local vshard = require('vshard')

        local buckets = {}
        for bucket_id = 1, 3000 do
            local replicaset = vshard.router.static:route(bucket_id)
            local replicaset_id = utils.get_replicaset_id(
                vshard.router.static,
                replicaset
            )
            buckets[replicaset_id] = buckets[replicaset_id] or bucket_id
        end

        local result = {}
        for _, bucket_id in pairs(buckets) do
            table.insert(result, bucket_id)
        end
        table.sort(result)
        return result
    ]])
end

local function get_buckets_from_same_replicaset(router)
    return router:eval([[
        local utils = require('crud.common.utils')
        local vshard = require('vshard')

        local buckets = {}
        for bucket_id = 1, 3000 do
            local replicaset = vshard.router.static:route(bucket_id)
            local replicaset_id = utils.get_replicaset_id(
                vshard.router.static,
                replicaset
            )
            local replicaset_buckets = buckets[replicaset_id] or {}
            table.insert(replicaset_buckets, bucket_id)
            buckets[replicaset_id] = replicaset_buckets
            if #replicaset_buckets == 2 then
                return replicaset_buckets
            end
        end
    ]])
end

local function install_rpc_counter(g)
    g.router:eval([[
        local utils = require('crud.common.utils')
        local router = assert(utils.get_vshard_router_instance())

        rawset(_G, 'storage_call_test_original_callrw', {})
        rawset(_G, 'storage_call_test_callrw_count', 0)

        for _, replicaset in pairs(router:routeall()) do
            _G.storage_call_test_original_callrw[replicaset] =
                replicaset.callrw
            replicaset.callrw = function(self, ...)
                _G.storage_call_test_callrw_count =
                    _G.storage_call_test_callrw_count + 1
                local original_callrw =
                    _G.storage_call_test_original_callrw[self]
                return original_callrw(self, ...)
            end
        end
    ]])
end

local function remove_rpc_counter(g)
    g.router:eval([[
        for replicaset, original_callrw in
            pairs(_G.storage_call_test_original_callrw or {}) do
            replicaset.callrw = original_callrw
        end

        rawset(_G, 'storage_call_test_original_callrw', nil)
        rawset(_G, 'storage_call_test_callrw_count', nil)
    ]])
end

local function get_rpc_count(router)
    return router:eval([[
        return _G.storage_call_test_callrw_count or 0
    ]])
end

local function install_map_callrw_spy(g)
    g.router:eval([[
        local utils = require('crud.common.utils')
        local router = assert(utils.get_vshard_router_instance())

        rawset(_G, 'storage_call_test_original_map_callrw', router.map_callrw)
        rawset(_G, 'storage_call_test_map_callrw_count', 0)
        rawset(_G, 'storage_call_test_map_callrw_args', {})

        router.map_callrw = function(self, func_name, args, opts)
            _G.storage_call_test_map_callrw_count =
                _G.storage_call_test_map_callrw_count + 1

            local bucket_calls = {}
            for bucket_id, calls in pairs(opts.bucket_ids) do
                bucket_calls[bucket_id] = {}
                for i, call_data in ipairs(calls) do
                    bucket_calls[bucket_id][i] = call_data.func_name
                end
            end
            rawset(_G, 'storage_call_test_map_callrw_args', {
                func_name = func_name,
                bucket_calls = bucket_calls,
            })

            return _G.storage_call_test_original_map_callrw(
                self,
                func_name,
                args,
                opts
            )
        end
    ]])
end

local function remove_map_callrw_spy(g)
    g.router:eval([[
        local utils = require('crud.common.utils')
        local router = assert(utils.get_vshard_router_instance())
        router.map_callrw = _G.storage_call_test_original_map_callrw
        rawset(_G, 'storage_call_test_original_map_callrw', nil)
        rawset(_G, 'storage_call_test_map_callrw_count', nil)
        rawset(_G, 'storage_call_test_map_callrw_args', nil)
    ]])
end

local function get_map_callrw_spy(router)
    return router:eval([[
        return {
            count = _G.storage_call_test_map_callrw_count,
            args = _G.storage_call_test_map_callrw_args,
        }
    ]])
end

local function get_bucket_count(router)
    return router:eval([[
        local utils = require('crud.common.utils')
        local vshard_router = assert(utils.get_vshard_router_instance())
        return vshard_router:bucket_count()
    ]])
end

local storage_masters = {'s1-master', 's2-master'}

local function wait_storage_masters_synced(g)
    t.helpers.retrying({timeout = 10, delay = 0.01}, function()
        helpers.call_on_servers(g.cluster, storage_masters, function(server)
            local state = server:exec(function()
                local internal = require('vshard.storage').internal
                return {
                    is_master = internal.is_master,
                    is_bucket_in_sync = internal.is_bucket_in_sync,
                }
            end)
            t.assert(state.is_master)
            t.assert(state.is_bucket_in_sync)
        end)
    end)
end

local function set_rebalancer(g, enabled)
    helpers.call_on_servers(g.cluster, storage_masters, function(server)
        server:exec(function(enable)
            local storage = require('vshard.storage')
            if enable then
                storage.rebalancer_enable()
            else
                storage.rebalancer_disable()
            end
        end, {enabled})
    end)
end

local function set_discovery(router, mode)
    return router:eval([[
        local utils = require('crud.common.utils')
        local vshard_router = assert(utils.get_vshard_router_instance())
        local previous_mode = vshard_router.discovery_mode
        vshard_router:discovery_set(...)
        return previous_mode
    ]], {mode})
end

local function bucket_is_writable(server, bucket_id)
    return server:exec(function(id)
        local stat = require('vshard.storage').bucket_stat(id)
        return stat ~= nil
            and (stat.status == 'active' or stat.status == 'pinned')
    end, {bucket_id})
end

local function replicaset_id(server)
    return server:exec(function()
        return require('vshard.storage').internal.this_replicaset.id
    end)
end

local function find_transfer_endpoints(g, bucket_id)
    local first = g.cluster:server(storage_masters[1])
    local second = g.cluster:server(storage_masters[2])
    if bucket_is_writable(first, bucket_id) then
        return first, second
    end
    t.assert(bucket_is_writable(second, bucket_id))
    return second, first
end

local function send_bucket(source, bucket_id, destination_id)
    source:exec(function(id, destination)
        local ok, err = require('vshard.storage').bucket_send(
            id,
            destination
        )
        if not ok then
            error(tostring(err))
        end
    end, {bucket_id, destination_id})
end

local function drop_sent_bucket(server, bucket_id)
    server:exec(function(id)
        local storage = require('vshard.storage')
        local stat = storage.bucket_stat(id)
        if stat == nil then
            return
        end
        if stat.status == 'active' or stat.status == 'pinned' then
            return
        end
        if stat.status == 'sent' then
            box.space._bucket:update({id}, {{'=', 2, 'garbage'}})
        end
        storage.bucket_delete_garbage(id, {force = true})
        storage.bucket_force_drop(id)
    end, {bucket_id})
end

local function prepare_bucket_transfer(g, bucket_id)
    wait_storage_masters_synced(g)
    set_rebalancer(g, false)
    g.storage_call_discovery_mode = set_discovery(g.router, 'off')

    -- Keep the old route in the router cache while the bucket is moved.
    g.router:eval([[
        local utils = require('crud.common.utils')
        local router = assert(utils.get_vshard_router_instance())
        return router:route(...).id
    ]], {bucket_id})

    local source, destination = find_transfer_endpoints(g, bucket_id)
    g.storage_call_transfer = {
        bucket_id = bucket_id,
        source = source,
        destination = destination,
        source_id = replicaset_id(source),
        destination_id = replicaset_id(destination),
    }
    return g.storage_call_transfer
end

local function cleanup_bucket_transfer(g)
    local transfer = g.storage_call_transfer
    if transfer ~= nil then
        helpers.call_on_servers(g.cluster, storage_masters, function(server)
            server:exec(function()
                rawset(_G, 'storage_call_test_wait_release', true)
            end)
        end)

        t.helpers.retrying({timeout = 10, delay = 0.01}, function()
            local send_started = transfer.source:exec(function()
                return rawget(_G, 'storage_call_test_bucket_send_started')
                    == true
            end)
            local send_done = transfer.source:exec(function()
                return rawget(_G, 'storage_call_test_bucket_send_done')
                    == true
            end)
            t.assert(not send_started or send_done)
        end)

        if bucket_is_writable(transfer.destination, transfer.bucket_id) then
            drop_sent_bucket(transfer.source, transfer.bucket_id)
            send_bucket(
                transfer.destination,
                transfer.bucket_id,
                transfer.source_id
            )
            drop_sent_bucket(transfer.destination, transfer.bucket_id)
        end

        g.storage_call_transfer = nil
    end

    if g.storage_call_discovery_mode ~= nil then
        set_discovery(g.router, g.storage_call_discovery_mode)
        g.storage_call_discovery_mode = nil
        g.router:eval([[
            local utils = require('crud.common.utils')
            local router = assert(utils.get_vshard_router_instance())
            router:discovery_wakeup()
        ]])
    end
    set_rebalancer(g, true)
end

local function get_sleep_calls_count(cluster)
    local count = 0
    helpers.call_on_storages(cluster, function(server)
        count = count + server:eval([[
            return _G.storage_call_test_sleep_calls or 0
        ]])
    end)
    return count
end

local function get_target_calls_count(cluster)
    local count = 0
    helpers.call_on_storages(cluster, function(server)
        count = count + server:eval([[
            return _G.storage_call_test_target_calls or 0
        ]])
    end)
    return count
end

local function get_cleanup_rollback_calls_count(cluster)
    local count = 0
    helpers.call_on_storages(cluster, function(server)
        count = count + server:eval([[
            return _G.storage_call_test_cleanup_rollback_calls or 0
        ]])
    end)
    return count
end

local function get_transaction_value(g, id)
    local value
    helpers.call_on_servers(g.cluster, storage_masters, function(server)
        local current = server:exec(function(transaction_id)
            local tuple = box.space.storage_call_test_transactions:get({
                transaction_id,
            })
            return tuple ~= nil and tuple[2] or nil
        end, {id})
        if current ~= nil then
            value = current
        end
    end)
    return value
end

local function release_waiting_targets(g)
    helpers.call_on_servers(g.cluster, storage_masters, function(server)
        server:exec(function()
            rawset(_G, 'storage_call_test_wait_release', true)
        end)
    end)

    t.helpers.retrying({timeout = 10, delay = 0.01}, function()
        helpers.call_on_servers(g.cluster, storage_masters, function(server)
            local state = server:exec(function()
                return {
                    started = _G.storage_call_test_wait_started,
                    finished = _G.storage_call_test_wait_finished,
                }
            end)
            t.assert(not state.started or state.finished)
        end)
    end)
end

local function install_cleanup_fault(g)
    helpers.exec_on_cluster(g.cluster, function()
        if box.space._bucket == nil then
            return
        end

        rawset(_G, 'storage_call_test_cleanup_rollback_calls', 0)
        rawset(
            _G,
            'storage_call_test_original_rollback',
            box.rollback
        )

        rawset(box, 'rollback', function(...)
            _G.storage_call_test_cleanup_rollback_calls =
                _G.storage_call_test_cleanup_rollback_calls + 1
            _G.storage_call_test_original_rollback(...)
            error('simulated transaction rollback error')
        end)
    end)
end

local function remove_cleanup_fault(g)
    helpers.exec_on_cluster(g.cluster, function()
        if box.space._bucket == nil then
            return
        end

        if _G.storage_call_test_original_rollback ~= nil then
            rawset(
                box,
                'rollback',
                _G.storage_call_test_original_rollback
            )
        end
        rawset(_G, 'storage_call_test_cleanup_rollback_calls', nil)
        rawset(_G, 'storage_call_test_original_rollback', nil)
    end)
end

local function install_sharding_check_fault(g)
    helpers.exec_on_cluster(g.cluster, function()
        if box.space._bucket == nil then
            return
        end

        local sharding = require('crud.common.sharding')
        rawset(
            _G,
            'storage_call_test_original_check_sharding_hash',
            sharding.check_sharding_hash
        )
        sharding.check_sharding_hash = function()
            box.begin()
            error('simulated unexpected sharding check error')
        end
    end)
end

local function remove_sharding_check_fault(g)
    helpers.exec_on_cluster(g.cluster, function()
        if box.space._bucket == nil then
            return
        end

        local original = rawget(
            _G,
            'storage_call_test_original_check_sharding_hash'
        )
        if original ~= nil then
            require('crud.common.sharding').check_sharding_hash = original
        end
        rawset(_G, 'storage_call_test_original_check_sharding_hash', nil)
    end)
end

local function get_bucket_for_key(router, space_name, key)
    return router:eval([[
        local fiber = require('fiber')
        local routing = require('crud.storage_call.routing')
        local utils = require('crud.common.utils')
        local vshard_router = assert(utils.get_vshard_router_instance())
        local call = assert(routing.call(vshard_router, {
            func_name = 'storage_call_test_counted',
            space_name = ...,
            key = select(2, ...),
        }, 1, fiber.clock() + 1, vshard_router:bucket_count()))
        return call.bucket_id
    ]], {space_name, key})
end

local function restart_storage(g, server_name)
    local server = g.cluster:server(server_name)
    server:stop()
    server:start()

    if g.params.backend == helpers.backend.VSHARD then
        server:wait_for_rw()

        local bootstrap_key = 'uuid'
        if type(g.params.backend_cfg) == 'table'
        and g.params.backend_cfg.identification_mode == 'name_as_key' then
            bootstrap_key = 'name'
        end

        server:exec(function(cfg, identification_key)
            require('vshard.storage').cfg(
                cfg,
                box.info[identification_key]
            )
            require('crud').init_storage()
        end, {g.cfg, bootstrap_key})
    elseif g.params.backend == helpers.backend.CONFIG then
        server:wait_for_rw()
        server:exec(function()
            require('crud').init_storage()
        end)
    end

    helpers.wait_crud_is_ready_on_cluster(g, {
        backend = g.params.backend,
    })
end

group.before_all(function(g)
    helpers.start_default_cluster(g, 'srv_simple_operations')
    helpers.exec_on_cluster(g.cluster, install_test_functions)

    g.buckets = get_buckets_from_different_replicasets(g.router)
    t.assert_equals(#g.buckets, 2)
    g.same_replicaset_buckets = get_buckets_from_same_replicaset(g.router)
    t.assert_equals(#g.same_replicaset_buckets, 2)
end)

group.after_all(function(g)
    helpers.stop_cluster(g.cluster, g.params.backend)
end)

group.before_each(function(g)
    helpers.exec_on_cluster(g.cluster, reset_test_state)
end)

group.before_test(
    'test_batch_preserves_same_bucket_order_and_uses_map_callrw',
    install_map_callrw_spy
)
group.after_test(
    'test_batch_preserves_same_bucket_order_and_uses_map_callrw',
    remove_map_callrw_spy
)

group.before_test(
    'test_timeout_before_rpc_is_not_ambiguous',
    install_rpc_counter
)
group.after_test(
    'test_timeout_before_rpc_is_not_ambiguous',
    remove_rpc_counter
)

group.before_test(
    'test_batch_timeout_before_rpc_is_not_ambiguous',
    install_rpc_counter
)
group.after_test(
    'test_batch_timeout_before_rpc_is_not_ambiguous',
    remove_rpc_counter
)

group.before_test(
    'test_single_cdata_bucket_is_rejected',
    install_rpc_counter
)
group.after_test(
    'test_single_cdata_bucket_is_rejected',
    remove_rpc_counter
)

group.before_test(
    'test_single_out_of_range_bucket_is_rejected',
    install_rpc_counter
)
group.after_test(
    'test_single_out_of_range_bucket_is_rejected',
    remove_rpc_counter
)

group.before_test(
    'test_batch_reroutes_payload_after_bucket_move',
    function(g)
        prepare_bucket_transfer(g, g.buckets[1])
    end
)
group.after_test(
    'test_batch_reroutes_payload_after_bucket_move',
    cleanup_bucket_transfer
)

group.before_test(
    'test_batch_keeps_bucket_locked_after_router_timeout',
    function(g)
        prepare_bucket_transfer(g, g.buckets[1])
    end
)
group.after_test(
    'test_batch_keeps_bucket_locked_after_router_timeout',
    cleanup_bucket_transfer
)

group.before_test(
    'test_cleanup_continues_after_rollback_error',
    install_cleanup_fault
)
group.after_test(
    'test_cleanup_continues_after_rollback_error',
    remove_cleanup_fault
)

group.before_test(
    'test_target_and_rollback_errors_are_preserved',
    install_cleanup_fault
)
group.after_test(
    'test_target_and_rollback_errors_are_preserved',
    remove_cleanup_fault
)

group.before_test(
    'test_unexpected_item_error_does_not_stop_batch',
    install_sharding_check_fault
)
group.after_test(
    'test_unexpected_item_error_does_not_stop_batch',
    remove_sharding_check_fault
)

group.before_test('test_transport_error_is_not_retried', function(g)
    g.router:eval([[
        local errors = require('errors')
        local utils = require('crud.common.utils')
        local router = assert(utils.get_vshard_router_instance())
        local TestError = errors.new_class(
            'StorageCallTransportTestError'
        )

        rawset(_G, 'storage_call_test_original_callrw', {})
        rawset(_G, 'storage_call_test_callrw_count', 0)

        for _, replicaset in pairs(router:routeall()) do
            _G.storage_call_test_original_callrw[replicaset] =
                replicaset.callrw
            replicaset.callrw = function()
                _G.storage_call_test_callrw_count =
                    _G.storage_call_test_callrw_count + 1
                return nil, TestError:new(
                    'storage call transport test error'
                )
            end
        end
    ]])
end)

group.after_test('test_transport_error_is_not_retried', remove_rpc_counter)

group.before_test('test_batch_transport_error_is_global', function(g)
    g.router:eval([[
        local errors = require('errors')
        local utils = require('crud.common.utils')
        local router = assert(utils.get_vshard_router_instance())
        local TestError = errors.new_class(
            'StorageCallMapTransportTestError'
        )

        rawset(
            _G,
            'storage_call_test_original_map_callrw',
            router.map_callrw
        )
        router.map_callrw = function()
            return nil,
                TestError:new('storage call map transport test error'),
                'test-replicaset'
        end
    ]])
end)

group.after_test('test_batch_transport_error_is_global', function(g)
    g.router:eval([[
        local utils = require('crud.common.utils')
        local router = assert(utils.get_vshard_router_instance())
        router.map_callrw = _G.storage_call_test_original_map_callrw
        rawset(_G, 'storage_call_test_original_map_callrw', nil)
    ]])
end)

group.after_test(
    'test_batch_map_timeout_hides_committed_result',
    release_waiting_targets
)

group.test_single_call_with_bucket_id = function(g)
    local result, err = g.router:call('crud.storage_call', {
        'storage_call_test_returns',
        {'value'},
        {bucket_id = g.buckets[1]},
    })

    t.assert_equals(err, nil)
    t.assert_equals(result.returns[1], 'value')
    t.assert_equals(result.returns[2], box.NULL)
    t.assert_equals(result.returns[3], false)
    t.assert_type(result.returns[4], 'number')
end

group.test_single_call_with_key = function(g)
    local result, err = g.router:call('crud.storage_call', {
        'storage_call_test_returns',
        {'value'},
        {space_name = 'customers', key = {1}},
    })

    t.assert_equals(err, nil)
    t.assert_equals(result.returns[1], 'value')
end

group.test_empty_batch = function(g)
    local result, err = g.router:call('crud.storage_call_many', {{}})

    t.assert_equals(err, nil)
    t.assert_equals(result, {results = {}})
end

group.test_missing_function = function(g)
    local result, err = g.router:call('crud.storage_call', {
        'storage_call_test_missing',
        {},
        {bucket_id = g.buckets[1]},
    })

    t.assert_equals(result, nil)
    t.assert_str_contains(err.err, 'is not registered')
    t.assert_equals(err.func_name, 'storage_call_test_missing')
    t.assert_equals(err.bucket_id, g.buckets[1])
    t.assert_equals(err.may_have_side_effects, false)
    t.assert_equals(err.operation_index, nil)
    t.assert_equals(err.operation_data, nil)
end

group.test_non_persistent_function = function(g)
    local result, err = g.router:call('crud.storage_call', {
        'storage_call_test_non_persistent',
        {},
        {bucket_id = g.buckets[1]},
    })

    t.assert_equals(result, nil)
    t.assert_str_contains(err.err, 'is not persistent')
    t.assert_equals(err.func_name, 'storage_call_test_non_persistent')
    t.assert_equals(err.bucket_id, g.buckets[1])
    t.assert_equals(err.may_have_side_effects, false)
end

group.test_target_error_does_not_stop_batch = function(g)
    local calls = {
        {
            func_name = 'storage_call_test_error',
            bucket_id = g.buckets[1],
        },
        {
            func_name = 'storage_call_test_returns',
            args = {'after error'},
            bucket_id = g.buckets[1],
        },
    }

    local result, err = g.router:call('crud.storage_call_many', {calls})

    t.assert_equals(err, nil)
    t.assert_str_contains(result.results[1].error.err, 'storage call test error')
    t.assert_equals(result.results[1].error.may_have_side_effects, true)
    t.assert_equals(result.results[2].returns[1], 'after error')
end

group.test_unserializable_result_does_not_break_batch = function(g)
    local calls = {
        {
            func_name = 'storage_call_test_unserializable',
            bucket_id = g.buckets[1],
        },
        {
            func_name = 'storage_call_test_returns',
            args = {'after invalid result'},
            bucket_id = g.buckets[1],
        },
    }

    local result, err = g.router:call('crud.storage_call_many', {calls})

    t.assert_equals(err, nil)
    t.assert_str_contains(result.results[1].error.err, 'cannot be serialized')
    t.assert_equals(result.results[1].error.may_have_side_effects, true)
    t.assert_equals(result.results[2].returns[1], 'after invalid result')
end

group.test_batch_preserves_same_bucket_order_and_uses_map_callrw = function(g)
    local calls = {
        {
            func_name = 'storage_call_test_order',
            args = {'first'},
            bucket_id = g.buckets[1],
        },
        {
            func_name = 'storage_call_test_returns',
            args = {'other replicaset'},
            bucket_id = g.buckets[2],
        },
        {
            func_name = 'storage_call_test_order',
            args = {'second'},
            bucket_id = g.buckets[1],
        },
    }

    local result, err = g.router:call('crud.storage_call_many', {calls})

    t.assert_equals(err, nil)
    t.assert_equals(result.results[1].returns[1], 1)
    t.assert_equals(result.results[2].returns[1], 'other replicaset')
    t.assert_equals(result.results[3].returns[1], 2)
    local map_callrw = get_map_callrw_spy(g.router)
    t.assert_equals(map_callrw.count, 1)
    t.assert_equals(
        map_callrw.args.func_name,
        '_crud.storage_call_many_on_storage'
    )
    t.assert_equals(map_callrw.args.bucket_calls[g.buckets[1]], {
        'storage_call_test_order',
        'storage_call_test_order',
    })
    t.assert_equals(map_callrw.args.bucket_calls[g.buckets[2]], {
        'storage_call_test_returns',
    })
end

group.test_results_preserve_input_order_for_buckets_on_same_replicaset =
function(g)
    local calls = {
        {
            func_name = 'storage_call_test_returns',
            args = {'first'},
            bucket_id = g.same_replicaset_buckets[1],
        },
        {
            func_name = 'storage_call_test_returns',
            args = {'second'},
            bucket_id = g.same_replicaset_buckets[2],
        },
        {
            func_name = 'storage_call_test_returns',
            args = {'third'},
            bucket_id = g.same_replicaset_buckets[1],
        },
    }

    local result, err = g.router:call('crud.storage_call_many', {calls})

    t.assert_equals(err, nil)
    t.assert_equals(result.results[1].returns[1], 'first')
    t.assert_equals(result.results[2].returns[1], 'second')
    t.assert_equals(result.results[3].returns[1], 'third')
end

group.test_invalid_item_does_not_stop_batch = function(g)
    local calls = {
        {
            func_name = 'storage_call_test_returns',
            bucket_id = g.buckets[1],
            space_name = 'customers',
            key = {1},
        },
        {
            func_name = 'storage_call_test_returns',
            args = {'valid'},
            bucket_id = g.buckets[1],
        },
    }

    local result, err = g.router:call('crud.storage_call_many', {calls})

    t.assert_equals(err, nil)
    t.assert_str_contains(result.results[1].error.err, 'either bucket_id')
    t.assert_equals(result.results[1].error.may_have_side_effects, false)
    t.assert_equals(result.results[2].returns[1], 'valid')
end

group.test_invalid_bucket_does_not_start_target = function(g)
    local calls = {
        {
            func_name = 'storage_call_test_counted',
            args = {'must not run'},
            bucket_id = 0,
        },
        {
            func_name = 'storage_call_test_counted',
            args = {'valid'},
            bucket_id = g.buckets[1],
        },
    }

    local result, err = g.router:call('crud.storage_call_many', {calls})

    t.assert_equals(err, nil)
    t.assert_str_contains(result.results[1].error.err, 'expected unsigned')
    t.assert_equals(result.results[1].error.may_have_side_effects, false)
    t.assert_equals(result.results[2].returns[1], 'valid')
    t.assert_equals(get_target_calls_count(g.cluster), 1)
end

group.test_single_cdata_bucket_is_rejected = function(g)
    local response = g.router:eval([[
        local crud = require('crud')
        local ffi = require('ffi')
        local result, err = crud.storage_call(
            'storage_call_test_counted',
            {'must not run'},
            {bucket_id = ffi.new('uint64_t', ...)}
        )
        return {result = result, error = err}
    ]], {g.buckets[1]})

    t.assert_equals(response.result, nil)
    t.assert_str_contains(
        response.error.err,
        'expected unsigned Lua number'
    )
    t.assert_equals(response.error.may_have_side_effects, false)
    t.assert_equals(get_rpc_count(g.router), 0)
    t.assert_equals(get_target_calls_count(g.cluster), 0)
end

group.test_single_out_of_range_bucket_is_rejected = function(g)
    local result, err = g.router:call('crud.storage_call', {
        'storage_call_test_counted',
        {'must not run'},
        {bucket_id = get_bucket_count(g.router) + 1},
    })

    t.assert_equals(result, nil)
    t.assert_str_contains(err.err, 'expected unsigned Lua number')
    t.assert_equals(err.may_have_side_effects, false)
    t.assert_equals(get_rpc_count(g.router), 0)
    t.assert_equals(get_target_calls_count(g.cluster), 0)
end

group.test_batch_invalid_bucket_items_do_not_stop_valid_calls = function(g)
    local response = g.router:eval([[
        local crud = require('crud')
        local ffi = require('ffi')
        local bucket_id, bucket_count = ...
        local result, err = crud.storage_call_many({
            {
                func_name = 'storage_call_test_counted',
                args = {'cdata must not run'},
                bucket_id = ffi.new('uint64_t', bucket_id),
            },
            {
                func_name = 'storage_call_test_counted',
                args = {'out of range must not run'},
                bucket_id = bucket_count + 1,
            },
            {
                func_name = 'storage_call_test_counted',
                args = {'valid'},
                bucket_id = bucket_id,
            },
        })
        return {result = result, error = err}
    ]], {g.buckets[1], get_bucket_count(g.router)})
    local result, err = response.result, response.error

    t.assert_equals(err, nil)
    t.assert_str_contains(
        result.results[1].error.err,
        'expected unsigned Lua number'
    )
    t.assert_equals(result.results[1].error.may_have_side_effects, false)
    t.assert_str_contains(
        result.results[2].error.err,
        'expected unsigned Lua number'
    )
    t.assert_equals(result.results[2].error.may_have_side_effects, false)
    t.assert_equals(result.results[3].returns[1], 'valid')
    t.assert_equals(get_target_calls_count(g.cluster), 1)
end

group.test_batch_reroutes_payload_after_bucket_move = function(g)
    local transfer = g.storage_call_transfer
    send_bucket(
        transfer.source,
        transfer.bucket_id,
        transfer.destination_id
    )

    local destination_uuid = transfer.destination:exec(function()
        return box.info.uuid
    end)
    local result, err = g.router:call('crud.storage_call_many', {{
        {
            func_name = 'storage_call_test_location',
            args = {'moved bucket'},
            bucket_id = transfer.bucket_id,
        },
        {
            func_name = 'storage_call_test_location',
            args = {'destination bucket'},
            bucket_id = g.buckets[2],
        },
    }})

    t.assert_equals(err, nil)
    t.assert_equals(result.results[1].returns, {
        'moved bucket',
        destination_uuid,
    })
    t.assert_equals(result.results[2].returns, {
        'destination bucket',
        destination_uuid,
    })
    t.assert_equals(transfer.source:exec(function()
        return _G.storage_call_test_target_calls
    end), 0)
    t.assert_equals(transfer.destination:exec(function()
        return _G.storage_call_test_target_calls
    end), 2)
end

group.test_batch_keeps_bucket_locked_after_router_timeout = function(g)
    local transfer = g.storage_call_transfer
    local result, err = g.router:call('crud.storage_call_many', {{
        {
            func_name = 'storage_call_test_wait_for_release',
            bucket_id = transfer.bucket_id,
        },
    }, {
        timeout = 0.02,
    }})

    t.assert_equals(result, nil)
    helpers.assert_timeout_error(err.err)
    t.assert_equals(err.may_have_side_effects, true)
    t.helpers.retrying({timeout = 5, delay = 0.01}, function()
        t.assert(transfer.source:exec(function()
            return _G.storage_call_test_wait_started
        end))
    end)

    transfer.source:exec(function(bucket_id, destination_id)
        local fiber = require('fiber')
        local storage = require('vshard.storage')
        rawset(_G, 'storage_call_test_bucket_send_started', true)
        fiber.create(function()
            local ok, send_err = storage.bucket_send(
                bucket_id,
                destination_id
            )
            if not ok then
                rawset(
                    _G,
                    'storage_call_test_bucket_send_error',
                    tostring(send_err)
                )
            end
            rawset(_G, 'storage_call_test_bucket_send_done', true)
        end)
    end, {transfer.bucket_id, transfer.destination_id})

    t.helpers.retrying({timeout = 5, delay = 0.01}, function()
        t.assert(transfer.source:exec(function()
            return _G.storage_call_test_bucket_send_started
        end))
    end)
    fiber.sleep(0.1)
    t.assert_not(transfer.source:exec(function()
        return _G.storage_call_test_bucket_send_done
    end))

    transfer.source:exec(function()
        rawset(_G, 'storage_call_test_wait_release', true)
    end)
    t.helpers.retrying({timeout = 10, delay = 0.01}, function()
        local state = transfer.source:exec(function()
            return {
                target_finished = _G.storage_call_test_wait_finished,
                send_done = _G.storage_call_test_bucket_send_done,
                send_error = rawget(
                    _G,
                    'storage_call_test_bucket_send_error'
                ),
            }
        end)
        t.assert(state.target_finished)
        t.assert(state.send_done)
        t.assert_equals(state.send_error, nil)
    end)
end

group.test_open_transaction_is_rolled_back = function(g)
    local calls = {
        {
            func_name = 'storage_call_test_open_transaction',
            bucket_id = g.buckets[1],
        },
        {
            func_name = 'storage_call_test_returns',
            args = {'after transaction'},
            bucket_id = g.buckets[1],
        },
    }

    local result, err = g.router:call('crud.storage_call_many', {calls})

    t.assert_equals(err, nil)
    t.assert_str_contains(result.results[1].error.err, 'open transaction')
    t.assert_equals(result.results[1].error.may_have_side_effects, true)
    t.assert_equals(result.results[2].returns[1], 'after transaction')
end

group.test_transaction_is_rolled_back_after_target_error = function(g)
    local calls = {
        {
            func_name = 'storage_call_test_transaction_write_and_error',
            args = {1, 'must be rolled back'},
            bucket_id = g.buckets[1],
        },
        {
            func_name = 'storage_call_test_transaction_get',
            args = {1},
            bucket_id = g.buckets[1],
        },
    }

    local result, err = g.router:call('crud.storage_call_many', {calls})

    t.assert_equals(err, nil)
    t.assert_str_contains(
        result.results[1].error.err,
        'storage call transaction error'
    )
    t.assert_equals(result.results[1].error.may_have_side_effects, true)
    t.assert_equals(result.results[2].returns[1], box.NULL)
end

group.test_committed_transaction_survives_neighbor_error = function(g)
    local calls = {
        {
            func_name = 'storage_call_test_transaction_commit',
            args = {1, 'committed'},
            bucket_id = g.buckets[1],
        },
        {
            func_name = 'storage_call_test_error',
            bucket_id = g.buckets[1],
        },
        {
            func_name = 'storage_call_test_transaction_get',
            args = {1},
            bucket_id = g.buckets[1],
        },
    }

    local result, err = g.router:call('crud.storage_call_many', {calls})

    t.assert_equals(err, nil)
    t.assert_equals(result.results[1].returns[1], true)
    t.assert_str_contains(result.results[2].error.err, 'storage call test error')
    t.assert_equals(result.results[3].returns[1], 'committed')
end

group.test_unexpected_item_error_does_not_stop_batch = function(g)
    local key = {1}
    local bucket_id = get_bucket_for_key(g.router, 'customers', key)
    local calls = {
        {
            func_name = 'storage_call_test_counted',
            args = {'must not run'},
            space_name = 'customers',
            key = key,
        },
        {
            func_name = 'storage_call_test_counted',
            args = {'after unexpected error'},
            bucket_id = bucket_id,
        },
    }

    local result, err = g.router:call('crud.storage_call_many', {calls})

    t.assert_equals(err, nil)
    t.assert_str_contains(
        result.results[1].error.err,
        'simulated unexpected sharding check error'
    )
    t.assert_equals(result.results[1].error.may_have_side_effects, true)
    t.assert_equals(result.results[2].returns[1], 'after unexpected error')
    t.assert_equals(get_target_calls_count(g.cluster), 1)
end

group.test_cleanup_continues_after_rollback_error = function(g)
    local calls = {
        {
            func_name = 'storage_call_test_open_transaction',
            bucket_id = g.buckets[1],
        },
        {
            func_name = 'storage_call_test_returns',
            args = {'after rollback error'},
            bucket_id = g.buckets[1],
        },
    }

    local result, err = g.router:call('crud.storage_call_many', {calls})

    t.assert_equals(err, nil)
    local first_error = result.results[1].error
    t.assert_str_contains(first_error.err, 'open transaction')
    t.assert_equals(get_cleanup_rollback_calls_count(g.cluster), 1)
    t.assert_str_contains(
        first_error.cleanup_errors.transaction_rollback,
        'simulated transaction rollback error'
    )
    t.assert_equals(first_error.may_have_side_effects, true)
    t.assert_equals(result.results[2].returns[1], 'after rollback error')
end

group.test_target_and_rollback_errors_are_preserved = function(g)
    local calls = {
        {
            func_name = 'storage_call_test_transaction_write_and_error',
            args = {1, 'must be rolled back'},
            bucket_id = g.buckets[1],
        },
        {
            func_name = 'storage_call_test_transaction_get',
            args = {1},
            bucket_id = g.buckets[1],
        },
    }

    local result, err = g.router:call('crud.storage_call_many', {calls})

    t.assert_equals(err, nil)
    local first_error = result.results[1].error
    t.assert_str_contains(first_error.err, 'storage call transaction error')
    t.assert_str_contains(
        first_error.err,
        'cleanup errors: transaction rollback'
    )
    t.assert_str_contains(
        first_error.cleanup_errors.transaction_rollback,
        'simulated transaction rollback error'
    )
    t.assert_equals(first_error.may_have_side_effects, true)
    t.assert_equals(result.results[2].returns[1], box.NULL)
end

group.test_target_function_acl = function(g)
    local connection = net_box.connect(g.router.net_box_uri, {
        user = 'storage_call_test_user',
        password = 'secret',
    })
    t.assert(connection:wait_connected())

    local result, err = connection:call('crud.storage_call', {
        'storage_call_test_returns',
        {'allowed'},
        {bucket_id = g.buckets[1]},
    })
    t.assert_equals(err, nil)
    t.assert_equals(result.returns[1], 'allowed')
    t.assert_equals(result.returns[5], 'storage_call_test_user')

    result, err = connection:call('crud.storage_call_many', {{
        {
            func_name = 'storage_call_test_returns',
            args = {'allowed in batch'},
            bucket_id = g.buckets[1],
        },
    }})
    t.assert_equals(err, nil)
    t.assert_equals(result.results[1].returns[1], 'allowed in batch')
    t.assert_equals(
        result.results[1].returns[5],
        'storage_call_test_user'
    )

    result, err = connection:call('crud.storage_call', {
        'storage_call_test_effective_user',
        {},
        {bucket_id = g.buckets[1]},
    })
    t.assert_equals(result, nil)
    t.assert_str_contains(err.err, 'has setuid enabled')
    t.assert_equals(err.may_have_side_effects, false)

    result, err = connection:call('crud.storage_call', {
        'storage_call_test_error',
        {},
        {bucket_id = g.buckets[1]},
    })
    t.assert_equals(result, nil)
    t.assert_str_contains(err.err, 'Execute access')
    t.assert_equals(err.may_have_side_effects, false)

    result, err = connection:call('crud.storage_call', {
        'storage_call_test_access_denied_inside',
        {},
        {bucket_id = g.buckets[1]},
    })
    t.assert_equals(result, nil)
    t.assert_str_contains(err.err, 'Execute access')
    t.assert_equals(err.may_have_side_effects, true)
    t.assert_equals(get_target_calls_count(g.cluster), 1)

    connection:close()
end

group.test_internal_dispatcher_rejects_untrusted_user = function(g)
    local storage = g.cluster:server('s1-master')
    local connection = net_box.connect(storage.net_box_uri, {
        user = 'storage_call_test_user',
        password = 'secret',
    })
    t.assert(connection:wait_connected())

    local dispatcher_names = {
        '_crud.storage_call_on_storage',
        '_crud.storage_call_many_on_storage',
    }
    for _, dispatcher_name in ipairs(dispatcher_names) do
        local ok, result, err = pcall(
            connection.call,
            connection,
            dispatcher_name,
            {'admin', {}}
        )
        if ok then
            t.assert_equals(result, nil)
        else
            err = result
        end
        t.assert_str_contains(
            tostring(err),
            'Access to the internal storage_call dispatcher is denied'
        )
    end

    connection:close()
end

group.test_timeout_is_reported_as_ambiguous = function(g)
    local result, err = g.router:call('crud.storage_call', {
        'storage_call_test_sleep',
        {0.2},
        {bucket_id = g.buckets[1], timeout = 0.01},
    })

    t.assert_equals(result, nil)
    helpers.assert_timeout_error(err.err)
    t.assert_equals(err.may_have_side_effects, true)

    fiber.sleep(0.25)
    t.assert_equals(get_sleep_calls_count(g.cluster), 1)
end

group.test_timeout_before_rpc_is_not_ambiguous = function(g)
    local result, err = g.router:call('crud.storage_call', {
        'storage_call_test_counted',
        {'must not run'},
        {bucket_id = g.buckets[1], timeout = 0},
    })

    t.assert_equals(result, nil)
    t.assert_str_contains(err.err, 'before the operation was sent to storage')
    t.assert_equals(err.may_have_side_effects, false)
    t.assert_equals(get_rpc_count(g.router), 0)
    t.assert_equals(get_target_calls_count(g.cluster), 0)
end

group.test_batch_timeout_before_rpc_is_not_ambiguous = function(g)
    local calls = {
        {
            func_name = 'storage_call_test_counted',
            args = {'must not run'},
            bucket_id = g.buckets[1],
        },
        {
            func_name = 'storage_call_test_counted',
            args = {'must not run either'},
            bucket_id = g.buckets[2],
        },
    }

    local result, err = g.router:call('crud.storage_call_many', {
        calls,
        {timeout = 0},
    })

    t.assert_equals(err, nil)
    for operation_index = 1, #calls do
        local call_error = result.results[operation_index].error
        t.assert_str_contains(
            call_error.err,
            'before the operation was sent to storage'
        )
        t.assert_equals(call_error.may_have_side_effects, false)
        t.assert_equals(call_error.operation_index, operation_index)
        t.assert_equals(call_error.operation_data, calls[operation_index])
    end
    t.assert_equals(get_rpc_count(g.router), 0)
    t.assert_equals(get_target_calls_count(g.cluster), 0)
end

group.test_batch_timeout_is_reported_as_ambiguous = function(g)
    local result, err = g.router:call('crud.storage_call_many', {{
        {
            func_name = 'storage_call_test_sleep',
            args = {0.2},
            bucket_id = g.buckets[1],
        },
        {
            func_name = 'storage_call_test_sleep',
            args = {0.2},
            bucket_id = g.buckets[2],
        },
    }, {
        timeout = 0.01,
    }})

    t.assert_equals(result, nil)
    helpers.assert_timeout_error(err.err)
    t.assert_equals(err.may_have_side_effects, true)

    fiber.sleep(0.25)
    t.assert(get_sleep_calls_count(g.cluster) > 0)
end

group.test_batch_map_timeout_hides_committed_result = function(g)
    local transaction_id = 101
    local result, err = g.router:call('crud.storage_call_many', {{
        {
            func_name = 'storage_call_test_transaction_commit',
            args = {transaction_id, 'committed before timeout'},
            bucket_id = g.buckets[1],
        },
        {
            func_name = 'storage_call_test_wait_for_release',
            bucket_id = g.buckets[2],
        },
    }, {
        timeout = 0.1,
    }})

    t.assert_equals(result, nil)
    helpers.assert_timeout_error(err.err)
    t.assert_equals(err.may_have_side_effects, true)

    t.helpers.retrying({timeout = 5, delay = 0.01}, function()
        t.assert_equals(
            get_transaction_value(g, transaction_id),
            'committed before timeout'
        )
    end)

    local wait_started = false
    t.helpers.retrying({timeout = 5, delay = 0.01}, function()
        helpers.call_on_servers(g.cluster, storage_masters, function(server)
            wait_started = wait_started or server:exec(function()
                return _G.storage_call_test_wait_started
            end)
        end)
        t.assert(wait_started)
    end)

    release_waiting_targets(g)
end

group.test_transport_error_is_not_retried = function(g)
    local result, err = g.router:call('crud.storage_call', {
        'storage_call_test_returns',
        {'value'},
        {bucket_id = g.buckets[1]},
    })

    t.assert_equals(result, nil)
    t.assert_str_contains(err.err, 'storage call transport test error')
    t.assert_equals(err.may_have_side_effects, true)
    t.assert_equals(get_rpc_count(g.router), 1)
end

group.test_batch_transport_error_is_global = function(g)
    local result, err = g.router:call('crud.storage_call_many', {{
        {
            func_name = 'storage_call_test_counted',
            args = {'must not run'},
            bucket_id = g.buckets[1],
        },
        {
            func_name = 'storage_call_test_counted',
            args = {'must not run either'},
            bucket_id = g.buckets[2],
        },
    }})

    t.assert_equals(result, nil)
    t.assert_str_contains(err.err, 'storage call map transport test error')
    t.assert_equals(err.may_have_side_effects, true)
    t.assert_equals(err.replicaset_id, 'test-replicaset')
    t.assert_equals(get_target_calls_count(g.cluster), 0)
end

group.test_persistent_function_survives_storage_restart = function(g)
    restart_storage(g, 's1-master')

    -- vshard reconnects the restarted master asynchronously. Retrying is safe
    -- here because storage_call_test_returns() has no side effects.
    local result, err
    t.helpers.retrying({timeout = 60, delay = 0.1}, function()
        result, err = g.router:call('crud.storage_call_many', {{
            {
                func_name = 'storage_call_test_returns',
                args = {'first replicaset'},
                bucket_id = g.buckets[1],
            },
            {
                func_name = 'storage_call_test_returns',
                args = {'second replicaset'},
                bucket_id = g.buckets[2],
            },
        }})
        t.assert_equals(err, nil)
    end)

    t.assert_equals(result.results[1].returns[1], 'first replicaset')
    t.assert_equals(result.results[2].returns[1], 'second replicaset')
end
