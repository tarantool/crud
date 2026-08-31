local fiber = require('fiber')

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

local function validate_bucket_id(bucket_id, bucket_count, where)
    if type(bucket_id) ~= 'number'
    or bucket_id < 1
    or bucket_id % 1 ~= 0
    or bucket_id > bucket_count then
        return storage_call_errors.class:new(
            ('Invalid bucket_id in %s: expected unsigned Lua number in '
                .. 'range [1, %d], got %s'),
            where,
            bucket_count,
            type(bucket_id)
        )
    end
end

--- Validates that a value is an array without gaps and returns its length.
function routing.array_length(value)
    local count = 0
    local max_index = 0

    for key in pairs(value) do
        if type(key) ~= 'number' or key < 1 or key % 1 ~= 0 then
            return nil, storage_call_errors.class:new(
                'calls must be an array'
            )
        end
        count = count + 1
        max_index = math.max(max_index, key)
    end

    if count ~= max_index then
        return nil, storage_call_errors.class:new(
            'calls must not contain gaps'
        )
    end

    return count
end

local function validate_call(call_data, operation_index)
    if type(call_data) ~= 'table' then
        return nil, storage_call_errors.new(
            ('calls[%d] must be a table'):format(operation_index),
            {
                operation_index = operation_index,
                operation_data = call_data,
            },
            false
        )
    end

    local error_call_data = {
        func_name = call_data.func_name,
        bucket_id = call_data.bucket_id,
        operation_index = operation_index,
        operation_data = call_data,
    }

    if type(call_data.func_name) ~= 'string' then
        return nil, storage_call_errors.new(
            ('calls[%d].func_name must be a string'):format(operation_index),
            error_call_data,
            false
        )
    end

    if call_data.args ~= nil and type(call_data.args) ~= 'table' then
        return nil, storage_call_errors.new(
            ('calls[%d].args must be a table'):format(operation_index),
            error_call_data,
            false
        )
    end

    return error_call_data
end

local function validate_route(route_data, where)
    local has_bucket_id = route_data.bucket_id ~= nil
    local has_space_name = route_data.space_name ~= nil
    local has_key = route_data.key ~= nil

    if has_bucket_id and (has_space_name or has_key) then
        return storage_call_errors.class:new(
            '%s must specify either bucket_id or space_name and key',
            where
        )
    end

    if not has_bucket_id and not (has_space_name and has_key) then
        return storage_call_errors.class:new(
            '%s must specify bucket_id or both space_name and key',
            where
        )
    end

    if has_space_name and type(route_data.space_name) ~= 'string' then
        return storage_call_errors.class:new(
            '%s.space_name must be a string',
            where
        )
    end
end

local function by_key(vshard_router, call_data, deadline, bucket_count)
    local timeout = remaining_timeout(deadline)
    if timeout == 0 then
        return nil, storage_call_errors.timeout_before_send(call_data)
    end

    local space, err = utils.get_space(call_data.space_name, vshard_router, {
        timeout = timeout,
        read_only = false,
    })
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

    local sharding_func_data
    sharding_func_data, err = sharding_metadata.fetch_sharding_func_on_router(
        vshard_router,
        call_data.space_name,
        timeout
    )
    if err ~= nil then
        return nil, err
    end

    local bucket_id
    if sharding_func_data.value ~= nil then
        bucket_id = sharding_func_data.value(extracted_sharding_key)
    else
        bucket_id = vshard_router:bucket_id_strcrc32(
            extracted_sharding_key
        )
    end

    err = validate_bucket_id(
        bucket_id,
        bucket_count,
        'sharding function result'
    )
    if err ~= nil then
        return nil, err
    end

    return {
        bucket_id = bucket_id,
        space_name = call_data.space_name,
        sharding_key_hash = sharding_key_data.hash,
        sharding_func_hash = sharding_func_data.hash,
        skip_sharding_hash_check = false,
    }
end

--- Validates route options and resolves them to a bucket.
function routing.route(vshard_router, route_options, deadline, bucket_count,
                       where)
    local err = validate_route(route_options, where)
    if err ~= nil then
        return nil, err
    end

    if route_options.bucket_id ~= nil then
        err = validate_bucket_id(
            route_options.bucket_id,
            bucket_count,
            where
        )
        if err ~= nil then
            return nil, err
        end

        return {
            bucket_id = route_options.bucket_id,
            skip_sharding_hash_check = true,
        }
    end

    return by_key(
        vshard_router,
        route_options,
        deadline,
        bucket_count
    )
end

--- Validates and routes one item of a batch call.
function routing.call(vshard_router, call_data, operation_index, deadline,
                      bucket_count)
    local error_call_data, err = validate_call(call_data, operation_index)
    if err ~= nil then
        return nil, err
    end

    local context = ('calls[%d]'):format(operation_index)
    local route_data
    route_data, err = routing.route(
        vshard_router,
        call_data,
        deadline,
        bucket_count,
        context
    )
    if err ~= nil then
        return nil, storage_call_errors.new(
            storage_call_errors.message(err),
            error_call_data,
            false
        )
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

--- Routes a single call without validating target function fields.
function routing.single(vshard_router, opts, deadline, bucket_count)
    local route_data, err = routing.route(
        vshard_router,
        opts,
        deadline,
        bucket_count,
        'opts'
    )
    if err ~= nil then
        local single_err = storage_call_errors.class:new(
            '%s',
            storage_call_errors.message(err)
        )
        single_err.may_have_side_effects = false
        return nil, single_err
    end

    return route_data
end

return routing
