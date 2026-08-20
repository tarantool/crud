local t = require('luatest')

local helpers = require('test.helper')

local group = t.group('storage_call', helpers.backend_matrix({{
    engine = 'memtx',
}}, {
    skip_safe_mode = true,
}))

local function install_test_functions()
    if box.space._bucket == nil then
        return
    end

    rawset(_G, 'storage_call_test_non_persistent', function()
        return true
    end)

    if not box.info.ro then
        local test_func_bodies = {
            storage_call_test_returns = [[
                function(...)
                    return select(1, ...), nil, false, box.info.id
                end
            ]],
            storage_call_test_error = [[
                function()
                    error('storage call test error')
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

        box.schema.func.create('storage_call_test_non_persistent', {
            if_not_exists = true,
        })
        box.schema.func.create('storage_call_test_setuid', {
            body = [[
                function()
                    return box.session.effective_user()
                end
            ]],
            is_sandboxed = false,
            setuid = true,
            if_not_exists = true,
        })
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

group.before_all(function(g)
    helpers.start_default_cluster(g, 'srv_simple_operations')
    helpers.exec_on_cluster(g.cluster, install_test_functions)

    g.buckets = get_buckets_from_different_replicasets(g.router)
    t.assert_equals(#g.buckets, 2)
end)

group.after_all(function(g)
    helpers.stop_cluster(g.cluster, g.params.backend)
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
end

group.test_non_persistent_function = function(g)
    local result, err = g.router:call('crud.storage_call', {
        'storage_call_test_non_persistent',
        {},
        {bucket_id = g.buckets[1]},
    })

    t.assert_equals(result, nil)
    t.assert_str_contains(err.err, 'is not persistent')
    t.assert_equals(err.may_have_side_effects, false)
end

group.test_setuid_function = function(g)
    local result, err = g.router:call('crud.storage_call', {
        'storage_call_test_setuid',
        {},
        {bucket_id = g.buckets[1]},
    })

    t.assert_equals(result, nil)
    t.assert_str_contains(err.err, 'has setuid enabled')
    t.assert_equals(err.may_have_side_effects, false)
end

group.test_target_error = function(g)
    local result, err = g.router:call('crud.storage_call', {
        'storage_call_test_error',
        {},
        {bucket_id = g.buckets[1]},
    })

    t.assert_equals(result, nil)
    t.assert_str_contains(err.err, 'storage call test error')
    t.assert_equals(err.may_have_side_effects, true)
end

group.test_invalid_route = function(g)
    local result, err = g.router:call('crud.storage_call', {
        'storage_call_test_returns',
        {},
        {
            bucket_id = g.buckets[1],
            space_name = 'customers',
            key = {1},
        },
    })

    t.assert_equals(result, nil)
    t.assert_str_contains(err.err, 'either bucket_id')
end
