local t = require('luatest')
local ffi = require('ffi')
local fiber = require('fiber')

local routing = require('crud.storage_call.routing')
local sharding_metadata = require(
    'crud.common.sharding.sharding_metadata'
)
local utils = require('crud.common.utils')

local group = t.group('storage_call_routing')
local bucket_count = 3000

group.before_each(function(g)
    g.get_space = utils.get_space
    g.fetch_sharding_key =
        sharding_metadata.fetch_sharding_key_on_router
    g.fetch_sharding_func =
        sharding_metadata.fetch_sharding_func_on_router
end)

group.after_each(function(g)
    utils.get_space = g.get_space
    sharding_metadata.fetch_sharding_key_on_router =
        g.fetch_sharding_key
    sharding_metadata.fetch_sharding_func_on_router =
        g.fetch_sharding_func
end)

local function route(call_data)
    return routing.call(
        {},
        call_data,
        7,
        fiber.clock() + 1,
        bucket_count
    )
end

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
    bucket_id_is_cdata = {
        call_data = {
            func_name = 'test',
            bucket_id = ffi.new('uint64_t', 1),
        },
        error = 'expected unsigned Lua number',
    },
    bucket_id_is_out_of_range = {
        call_data = {
            func_name = 'test',
            bucket_id = bucket_count + 1,
        },
        error = 'range [1, 3000]',
    },
}

for name, case in pairs(invalid_call_cases) do
    group['test_rejects_' .. name] = function()
        local routed_call, err = route(case.call_data)

        t.assert_equals(routed_call, nil)
        t.assert_str_contains(err.err, case.error)
        t.assert_equals(err.may_have_side_effects, false)
        t.assert_equals(err.operation_index, 7)
        t.assert_equals(err.operation_data, case.call_data)
    end
end

group.test_direct_bucket_route_defaults_args = function()
    local routed_call, err = route({
        func_name = 'test',
        bucket_id = 1,
    })

    t.assert_equals(err, nil)
    t.assert_equals(routed_call, {
        operation_index = 7,
        func_name = 'test',
        args = {},
        bucket_id = 1,
        skip_sharding_hash_check = true,
    })
end

group.test_single_returns_only_route_data = function()
    local route_data, err = routing.single(
        {},
        {bucket_id = 1},
        fiber.clock() + 1,
        bucket_count
    )

    t.assert_equals(err, nil)
    t.assert_equals(route_data, {
        bucket_id = 1,
        skip_sharding_hash_check = true,
    })
end

group.test_single_route_error_uses_opts_context = function()
    local route_data, err = routing.single(
        {},
        {},
        fiber.clock() + 1,
        bucket_count
    )

    t.assert_equals(route_data, nil)
    t.assert_str_contains(
        err.err,
        'opts must specify bucket_id or both space_name and key'
    )
    t.assert_equals(err.may_have_side_effects, false)
end

group.test_key_route_passes_remaining_timeout_to_each_stage = function()
    local timeouts = {}
    utils.get_space = function(_, _, opts)
        table.insert(timeouts, opts.timeout)
        fiber.sleep(0.01)
        return {index = {[0] = {parts = {}}}}
    end
    sharding_metadata.fetch_sharding_key_on_router = function(_, _, timeout)
        table.insert(timeouts, timeout)
        fiber.sleep(0.01)
        return {value = nil, hash = 1}
    end
    sharding_metadata.fetch_sharding_func_on_router = function(_, _, timeout)
        table.insert(timeouts, timeout)
        return {value = nil, hash = 2}
    end

    local router = {
        bucket_id_strcrc32 = function()
            return 1
        end,
    }
    local routed_call, err = routing.call(router, {
        func_name = 'test',
        space_name = 'customers',
        key = {1},
    }, 1, fiber.clock() + 1, bucket_count)

    t.assert_equals(err, nil)
    t.assert_equals(routed_call.bucket_id, 1)
    t.assert_equals(#timeouts, 3)
    t.assert(timeouts[1] > timeouts[2])
    t.assert(timeouts[2] > timeouts[3])
end

group.test_key_route_stops_when_common_deadline_expires = function()
    local metadata_fetches = 0
    utils.get_space = function()
        fiber.sleep(0.02)
        return {index = {[0] = {parts = {}}}}
    end
    sharding_metadata.fetch_sharding_key_on_router = function()
        metadata_fetches = metadata_fetches + 1
        return {value = nil, hash = 1}
    end

    local routed_call, err = routing.call({}, {
        func_name = 'test',
        space_name = 'customers',
        key = {1},
    }, 1, fiber.clock() + 0.005, bucket_count)

    t.assert_equals(routed_call, nil)
    t.assert_str_contains(
        err.err,
        'before the operation was sent to storage'
    )
    t.assert_equals(err.may_have_side_effects, false)
    t.assert_equals(metadata_fetches, 0)
end
