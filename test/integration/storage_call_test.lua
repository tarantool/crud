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
end

local function reset_test_state()
    if box.space._bucket == nil then
        return
    end

    rawset(_G, 'storage_call_test_values', {})
    rawset(_G, 'storage_call_test_sleep_calls', 0)
    rawset(_G, 'storage_call_test_target_calls', 0)
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
    'test_batch_preserves_order_and_uses_map_callrw',
    install_rpc_counter
)
group.after_test(
    'test_batch_preserves_order_and_uses_map_callrw',
    remove_rpc_counter
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

group.test_batch_preserves_order_and_uses_map_callrw = function(g)
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
    -- Partial map_callrw performs one Ref and one Map request per replicaset.
    t.assert_equals(get_rpc_count(g.router), 4)
end

group.test_results_for_different_buckets_on_same_storage_preserve_order =
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

    local result, err = g.router:call('crud.storage_call_many', {{
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
    t.assert_equals(result.results[1].returns[1], 'first replicaset')
    t.assert_equals(result.results[2].returns[1], 'second replicaset')
end
