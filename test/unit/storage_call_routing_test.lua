local t = require('luatest')
local ffi = require('ffi')
local fiber = require('fiber')

local schema = require('crud.common.schema')
local const = require('crud.common.const')
local routing = require('crud.storage_call.routing')
local sharding_metadata = require(
    'crud.common.sharding.sharding_metadata'
)
local utils = require('crud.common.utils')

local group = t.group('storage_call_routing')
local bucket_count = 3000

group.before_each(function(g)
    g.get_space = utils.get_space
    g.reload_schema = schema.reload_schema
    g.fetch_sharding_key =
        sharding_metadata.fetch_sharding_key_on_router
    g.fetch_sharding_func =
        sharding_metadata.fetch_sharding_func_on_router
end)

group.after_each(function(g)
    utils.get_space = g.get_space
    schema.reload_schema = g.reload_schema
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

group.test_cdata_bucket_id_is_normalized_for_vshard = function()
    local routed_call, err = route({
        func_name = 'test',
        bucket_id = ffi.new('uint64_t', 1),
    })
    t.assert_equals(err, nil)
    t.assert_equals(routed_call.bucket_id, 1)
    t.assert_equals(type(routed_call.bucket_id), 'number')
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

group.test_missing_space_requests_schema_reload = function()
    utils.get_space = function() return nil end

    local result, err, need_reload = routing.single({}, {
        space_name = 'new_space', key = {1},
    }, fiber.clock() + 1, bucket_count)

    t.assert_equals(result, nil)
    t.assert_str_contains(err.err, 'does not exist')
    t.assert_equals(need_reload, const.NEED_SCHEMA_RELOAD)
end

group.test_schema_reload_cannot_send_after_deadline = function()
    utils.get_space = function() return nil end
    schema.reload_schema = function()
        fiber.sleep(0.02)
        return true
    end

    local result, err = schema.wrap_func_reload({}, routing.single,
        {space_name = 'new_space', key = {1}},
        fiber.clock() + 0.005, bucket_count, {})

    t.assert_equals(result, nil)
    t.assert_str_contains(err.err, 'before the operation was sent to storage')
    t.assert_equals(err.may_have_side_effects, false)
end

group.test_sharding_function_exception_is_a_routing_error = function()
    utils.get_space = function()
        return {index = {[0] = {parts = {}}}}
    end
    sharding_metadata.fetch_sharding_key_on_router = function()
        return {hash = 1}
    end
    sharding_metadata.fetch_sharding_func_on_router = function()
        return {value = function() error('invalid custom key') end}
    end

    local call_data = {
        func_name = 'test', space_name = 'customers', key = {1},
    }
    local result, err = route(call_data)
    t.assert_equals(result, nil)
    t.assert_str_contains(err.err, 'invalid custom key')
    t.assert_equals(err.operation_index, nil)
    t.assert_equals(err.operation_data, call_data)
    t.assert_equals(err.may_have_side_effects, false)

    result, err = routing.single({}, call_data, fiber.clock() + 1, bucket_count)
    t.assert_equals(result, nil)
    t.assert_str_contains(err.err, 'invalid custom key')
    t.assert_equals(err.may_have_side_effects, false)
end
