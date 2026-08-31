local t = require('luatest')

local batch = require('crud.storage_call.batch')
local sharding_metadata = require(
    'crud.common.sharding.sharding_metadata'
)

local group = t.group('storage_call_batch')

local function make_calls(count)
    local original_calls = {}
    local expected_calls = {}

    for operation_index = 1, count do
        original_calls[operation_index] = {
            func_name = 'test_' .. operation_index,
            space_name = 'customers',
            key = {operation_index},
        }
        expected_calls[operation_index] = {
            func_name = 'test_' .. operation_index,
            bucket_id = operation_index,
        }
    end

    return original_calls, expected_calls
end

local function collect(map_results, count, results, router)
    local original_calls, expected_calls = make_calls(count)
    return batch.collect(
        router or {},
        map_results,
        original_calls,
        results or {},
        expected_calls
    )
end

group.before_each(function(g)
    g.original_reload_sharding_cache =
        sharding_metadata.reload_sharding_cache
end)

group.after_each(function(g)
    sharding_metadata.reload_sharding_cache =
        g.original_reload_sharding_cache
end)

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
    group['test_rejects_' .. name] = function()
        local result, err = collect(map_results, 1)

        t.assert_equals(result, nil)
        t.assert_str_contains(err.err, 'Storage returned an invalid response')
        t.assert_equals(err.replicaset_id, 'replicaset')
        t.assert_equals(err.may_have_side_effects, true)
    end
end

group.test_rejects_result_for_an_already_completed_operation = function()
    local result, err = collect({
        replicaset = {{{operation_index = 1, returns = {'unexpected'}}}},
    }, 1, {
        [1] = {error = {err = 'routing error'}},
    })

    t.assert_equals(result, nil)
    t.assert_str_contains(err.err, 'Storage returned an invalid response')
    t.assert_equals(err.replicaset_id, 'replicaset')
end

group.test_marks_missing_results_as_ambiguous = function()
    local result, err = collect({
        replicaset = {{{operation_index = 1, returns = {'first'}}}},
    }, 2)

    t.assert_equals(err, nil)
    t.assert_equals(result.results[1].returns, {'first'})
    t.assert_str_contains(
        result.results[2].error.err,
        'No result was returned for the operation'
    )
    t.assert_equals(result.results[2].error.func_name, 'test_2')
    t.assert_equals(result.results[2].error.bucket_id, 2)
    t.assert_equals(result.results[2].error.operation_index, 2)
    t.assert_equals(result.results[2].error.may_have_side_effects, true)
end

group.test_adds_operation_and_replicaset_to_storage_error = function()
    local storage_error = {
        err = 'target failed',
        may_have_side_effects = true,
    }
    local result, err = collect({
        replicaset = {{{operation_index = 1, error = storage_error}}},
    }, 1)

    t.assert_equals(err, nil)
    t.assert_equals(result.results[1].error, storage_error)
    t.assert_equals(
        result.results[1].error.operation_data,
        {
            func_name = 'test_1',
            space_name = 'customers',
            key = {1},
        }
    )
    t.assert_equals(result.results[1].error.replicaset_id, 'replicaset')
end

group.test_reloads_sharding_cache_after_hash_mismatch = function()
    local router = {}
    local reload_calls = {}
    sharding_metadata.reload_sharding_cache = function(actual_router,
                                                       space_name)
        table.insert(reload_calls, {actual_router, space_name})
    end

    local result, err = collect({
        replicaset = {{{
            operation_index = 1,
            error = {
                err = 'sharding hash mismatch',
                sharding_hash_mismatch = true,
            },
        }}},
    }, 1, nil, router)

    t.assert_equals(err, nil)
    t.assert_equals(result.results[1].error.err, 'sharding hash mismatch')
    t.assert_equals(reload_calls, {{router, 'customers'}})
end

group.test_mark_not_sent_preserves_existing_routing_errors = function()
    local original_calls, expected_calls = make_calls(2)
    expected_calls[1] = nil
    local routing_error = {error = {err = 'routing error'}}
    local result = batch.mark_not_sent(
        {[1] = routing_error},
        expected_calls,
        original_calls
    )

    t.assert_equals(result.results[1], routing_error)
    t.assert_str_contains(
        result.results[2].error.err,
        'before the operation was sent to storage'
    )
    t.assert_equals(result.results[2].error.operation_index, 2)
    t.assert_equals(result.results[2].error.operation_data, original_calls[2])
    t.assert_equals(result.results[2].error.may_have_side_effects, false)
end
