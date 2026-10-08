local fiber = require('fiber')

local const = require('crud.common.const')
local sharding = require('crud.common.sharding')
local sharding_key = require('crud.common.sharding.sharding_key')
local sharding_metadata = require(
    'crud.common.sharding.sharding_metadata'
)
local utils = require('crud.common.utils')
local storage_call_errors = require('crud.storage_call.errors')

local routing = {}

local function remaining_timeout(deadline)
    return math.max(deadline - fiber.clock(), 0)
end

local function by_key(vshard_router, call_data, deadline, bucket_count,
                      route_context)
    local timeout = remaining_timeout(deadline)
    if timeout == 0 then
        return nil, storage_call_errors.timeout_before_send(call_data)
    end

    local space, err = utils.get_space(call_data.space_name, vshard_router, {
        timeout = timeout,
        read_only = false,
    })
    if space == nil and err == nil and
    not route_context.schema_reload_requested then
        -- No target has run yet. Let the common wrapper reload net.box schema
        -- once before routing the request again.
        route_context.schema_reload_requested = true
        return nil, storage_call_errors.class:new(
            'Space %q does not exist', call_data.space_name
        ), const.NEED_SCHEMA_RELOAD
    end
    if err ~= nil then
        return nil, err
    end
    if space == nil then
        return nil, storage_call_errors.class:new(
            'Space %q does not exist',
            call_data.space_name
        )
    end

    local key = call_data.key
    if box.tuple.is(key) then
        key = key:totable()
    end

    timeout = remaining_timeout(deadline)
    if timeout == 0 then
        return nil, storage_call_errors.timeout_before_send(call_data)
    end

    local sharding_key_data
    sharding_key_data, err = sharding_metadata.fetch_sharding_key_on_router(
        vshard_router,
        call_data.space_name,
        timeout
    )
    if err ~= nil then
        return nil, err
    end

    local extracted_sharding_key
    extracted_sharding_key, err = sharding_key.extract_from_pk(
        vshard_router,
        call_data.space_name,
        sharding_key_data.value,
        space.index[0].parts,
        key
    )
    if err ~= nil then
        return nil, err
    end

    timeout = remaining_timeout(deadline)
    if timeout == 0 then
        return nil, storage_call_errors.timeout_before_send(call_data)
    end

    local bucket_id_data
    bucket_id_data, err = sharding.key_get_bucket_id(
        vshard_router,
        call_data.space_name,
        extracted_sharding_key,
        nil,
        timeout
    )
    if err ~= nil then
        return nil, err
    end

    local bucket_id = bucket_id_data.bucket_id
    err = sharding.validate_bucket_id(bucket_id, 'sharding function result')
    if err ~= nil then
        return nil, err
    end
    bucket_id = tonumber(bucket_id)
    if bucket_id > bucket_count then
        return nil, storage_call_errors.class:new(
            'Invalid bucket_id in sharding function result: '
            .. 'expected value in range [1, %d]', bucket_count
        )
    end

    return {
        bucket_id = bucket_id,
        space_name = call_data.space_name,
        sharding_key_hash = sharding_key_data.hash,
        sharding_func_hash = bucket_id_data.sharding_func_hash,
        skip_sharding_hash_check = false,
    }
end

--- Resolves previously validated route options to a bucket.
function routing.route(vshard_router, route_options, deadline, bucket_count,
                       route_context)
    if route_options.bucket_id ~= nil then
        return {
            bucket_id = tonumber(route_options.bucket_id),
            skip_sharding_hash_check = true,
        }
    end

    return by_key(
        vshard_router,
        route_options,
        deadline,
        bucket_count,
        route_context or {}
    )
end

--- Routes one validated item of a batch call.
function routing.call(vshard_router, call_data, operation_index, deadline,
                      bucket_count, route_context)
    local ok, route_data
    local err
    local need_reload
    ok, route_data, err, need_reload = pcall(
        routing.route,
        vshard_router,
        call_data,
        deadline,
        bucket_count,
        route_context
    )
    if not ok then
        err = route_data
    end
    if not ok or err ~= nil then
        return nil, storage_call_errors.new(
            storage_call_errors.message(err),
            {
                func_name = call_data.func_name,
                bucket_id = call_data.bucket_id,
                operation_data = call_data,
            },
            false
        ), need_reload
    end

    return {
        operation_index = operation_index,
        func_name = call_data.func_name,
        args = call_data.args or {},
        bucket_id = route_data.bucket_id,
        space_name = route_data.space_name,
        sharding_key_hash = route_data.sharding_key_hash,
        sharding_func_hash = route_data.sharding_func_hash,
        skip_sharding_hash_check = route_data.skip_sharding_hash_check,
    }
end

--- Routes a previously validated single call.
function routing.single(vshard_router, opts, deadline, bucket_count,
                        route_context)
    local ok, route_data, err, need_reload = pcall(
        routing.route,
        vshard_router,
        opts,
        deadline,
        bucket_count,
        route_context
    )
    if not ok then
        err = route_data
    end
    if not ok or err ~= nil then
        local single_err = storage_call_errors.class:new(
            '%s',
            storage_call_errors.message(err)
        )
        single_err.may_have_side_effects = false
        return nil, single_err, need_reload
    end

    return route_data
end

return routing
