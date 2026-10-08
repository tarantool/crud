local t = require('luatest')

local helpers = require('test.helper')
local utils = require('crud.common.utils')
local router = require('crud.storage_call.router')
local sharding_utils = require('crud.common.sharding.utils')

local group = t.group('storage_call_batch')

group.before_all(function()
    helpers.box_cfg()
end)

group.before_each(function(g)
    g.get_router = utils.get_vshard_router_instance
    g.vshard_router = {
        bucket_count = function() return 3000 end,
    }
    utils.get_vshard_router_instance = function() return g.vshard_router end
end)

group.after_each(function(g)
    utils.get_vshard_router_instance = g.get_router
end)

local function make_calls(count)
    local calls = {}
    for operation_index = 1, count do
        calls[operation_index] = {
            func_name = 'test_' .. operation_index,
            bucket_id = operation_index,
        }
    end
    return calls
end

local function call_many(g, map_results, calls)
    g.vshard_router.map_callrw = function() return map_results end
    return router.call_many(calls, {})
end

group.test_groups_buckets_and_restores_input_order = function(g)
    local calls = make_calls(3)
    calls[3].bucket_id = calls[1].bucket_id
    g.vshard_router.map_callrw = function(_, _, _, opts)
        t.assert_equals(#opts.bucket_ids[1], 2)
        t.assert_equals(opts.bucket_ids[1][1].operation_index, 1)
        t.assert_equals(opts.bucket_ids[1][2].operation_index, 3)
        t.assert_equals(#opts.bucket_ids[2], 1)
        return {replicaset = {{
            {operation_index = 3, returns = {'third'}},
            {operation_index = 1, returns = {'first'}},
            {operation_index = 2, returns = {'second'}},
        }}}
    end

    local result, err = router.call_many(calls, {})
    t.assert_equals(err, nil)
    t.assert_equals(result.results[1].returns, {'first'})
    t.assert_equals(result.results[2].returns, {'second'})
    t.assert_equals(result.results[3].returns, {'third'})
end

local invalid_response_cases = {
    response_is_not_a_table = {
        replicaset = true,
    },
    response_has_no_first_return_value = {
        replicaset = {},
    },
    result_is_not_a_table = {
        replicaset = {{'not a table'}},
    },
    operation_index_is_unknown = {
        replicaset = {{{operation_index = 2, returns = {'value'}}}},
    },
    operation_index_is_duplicated = {
        replicaset = {{
            {operation_index = 1, returns = {'first'}},
            {operation_index = 1, returns = {'second'}},
        }},
    },
}

for name, map_results in pairs(invalid_response_cases) do
    group['test_rejects_' .. name] = function(g)
        local result, err = call_many(g, map_results, make_calls(1))

        t.assert_equals(result, nil)
        t.assert_str_contains(err.err, 'Storage returned an invalid response')
        t.assert_equals(err.replicaset_id, 'replicaset')
        t.assert_equals(err.may_have_side_effects, true)
    end
end

group.test_rejects_result_for_an_already_completed_operation = function(g)
    local calls = make_calls(2)
    calls[1].func_name = 1
    local result, err = call_many(g, {
        replicaset = {{{operation_index = 1, returns = {'unexpected'}}}},
    }, calls)

    t.assert_equals(result, nil)
    t.assert_str_contains(err.err, 'Storage returned an invalid response')
    t.assert_equals(err.replicaset_id, 'replicaset')
end

group.test_missing_result_is_a_top_level_error = function(g)
    local result, err = call_many(g, {
        replicaset = {{{operation_index = 1, returns = {'first'}}}},
    }, make_calls(2))

    t.assert_equals(result, nil)
    t.assert_str_contains(err.err, 'Storage returned an invalid response')
    t.assert_equals(err.may_have_side_effects, true)
end

group.test_adds_operation_and_replicaset_to_storage_error = function(g)
    local storage_error = {
        err = 'target failed',
        may_have_side_effects = true,
    }
    local calls = make_calls(1)
    local result, err = call_many(g, {
        replicaset = {{{operation_index = 1, error = storage_error}}},
    }, calls)

    t.assert_equals(err, nil)
    t.assert_equals(result.results[1].error, storage_error)
    t.assert_equals(result.results[1].error.operation_data, calls[1])
    t.assert_equals(result.results[1].error.replicaset_id, 'replicaset')
end

group.test_marks_sharding_mismatch_as_not_started = function(g)
    local result, err = call_many(g, {
        replicaset = {{
            {
                operation_index = 1,
                error = sharding_utils.ShardingHashMismatchError:new(
                    'sharding hash mismatch'),
            },
            {operation_index = 2, returns = {'done'}},
        }},
    }, make_calls(2))

    t.assert_equals(err, nil)
    t.assert_equals(result.results[1].error.err, 'sharding hash mismatch')
    t.assert_equals(result.results[1].error.may_have_side_effects, false)
end

group.test_timeout_before_map_preserves_routing_errors = function(g)
    local calls = make_calls(2)
    calls[1].func_name = 1
    g.vshard_router.map_callrw = function()
        error('Map must not run after the deadline')
    end
    local result, err = router.call_many(calls, {timeout = 0})

    t.assert_equals(err, nil)
    t.assert_str_contains(result.results[1].error.err, 'func_name')
    t.assert_equals(result.results[1].error.operation_data, calls[1])
    t.assert_equals(result.results[1].error.may_have_side_effects, false)
    t.assert_str_contains(
        result.results[2].error.err,
        'before the operation was sent to storage'
    )
    t.assert_equals(result.results[2].error.operation_index, nil)
    t.assert_equals(result.results[2].error.operation_data, calls[2])
    t.assert_equals(result.results[2].error.may_have_side_effects, false)
end
