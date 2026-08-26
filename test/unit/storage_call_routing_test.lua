local t = require('luatest')

local routing = require('crud.storage_call.routing')

local group = t.group('storage_call_routing')

group.test_array_length_accepts_empty_and_dense_arrays = function()
    local length, err = routing.array_length({})
    t.assert_equals(err, nil)
    t.assert_equals(length, 0)

    length, err = routing.array_length({{}, {}, {}})
    t.assert_equals(err, nil)
    t.assert_equals(length, 3)
end

group.test_array_length_rejects_non_array_keys = function()
    local length, err = routing.array_length({
        {},
        unexpected = {},
    })

    t.assert_equals(length, nil)
    t.assert_str_contains(err.err, 'calls must be an array')
end

group.test_array_length_rejects_gaps = function()
    local length, err = routing.array_length({
        [1] = {},
        [3] = {},
    })

    t.assert_equals(length, nil)
    t.assert_str_contains(err.err, 'calls must not contain gaps')
end

local invalid_call_cases = {
    item_is_not_a_table = {
        call_data = 'not a table',
        error = 'calls[7] must be a table',
    },
    func_name_is_missing = {
        call_data = {bucket_id = 1},
        error = 'calls[7].func_name must be a string',
    },
    func_name_is_not_a_string = {
        call_data = {func_name = 123, bucket_id = 1},
        error = 'calls[7].func_name must be a string',
    },
    args_is_not_a_table = {
        call_data = {func_name = 'test', args = 'value', bucket_id = 1},
        error = 'calls[7].args must be a table',
    },
    both_routes_are_specified = {
        call_data = {
            func_name = 'test',
            bucket_id = 1,
            space_name = 'customers',
            key = {1},
        },
        error = 'must specify either bucket_id or space_name and key',
    },
    route_is_missing = {
        call_data = {func_name = 'test'},
        error = 'must specify bucket_id or both space_name and key',
    },
    route_has_only_space_name = {
        call_data = {func_name = 'test', space_name = 'customers'},
        error = 'must specify bucket_id or both space_name and key',
    },
    route_has_only_key = {
        call_data = {func_name = 'test', key = {1}},
        error = 'must specify bucket_id or both space_name and key',
    },
    space_name_is_not_a_string = {
        call_data = {func_name = 'test', space_name = {}, key = {1}},
        error = 'calls[7].space_name must be a string',
    },
    bucket_id_is_invalid = {
        call_data = {func_name = 'test', bucket_id = 0},
        error = 'expected unsigned',
    },
}

for name, case in pairs(invalid_call_cases) do
    group['test_rejects_' .. name] = function()
        local routed_call, err = routing.call({}, case.call_data, 7, 1)

        t.assert_equals(routed_call, nil)
        t.assert_str_contains(err.err, case.error)
        t.assert_equals(err.may_have_side_effects, false)
        t.assert_equals(err.operation_index, 7)
        t.assert_equals(err.operation_data, case.call_data)
    end
end

group.test_direct_bucket_route_defaults_args = function()
    local routed_call, err = routing.call({}, {
        func_name = 'test',
        bucket_id = 1,
    }, 7, 1)

    t.assert_equals(err, nil)
    t.assert_equals(routed_call, {
        operation_index = 7,
        func_name = 'test',
        args = {},
        bucket_id = 1,
        skip_sharding_hash_check = true,
    })
end
