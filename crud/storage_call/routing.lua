local sharding = require('crud.common.sharding')
local sharding_key = require('crud.common.sharding.sharding_key')
local sharding_metadata = require(
    'crud.common.sharding.sharding_metadata'
)
local utils = require('crud.common.utils')
local storage_call_errors = require('crud.storage_call.errors')

local routing = {}

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

    local has_bucket_id = call_data.bucket_id ~= nil
    local has_space_name = call_data.space_name ~= nil
    local has_key = call_data.key ~= nil

    if has_bucket_id and (has_space_name or has_key) then
        return nil, storage_call_errors.new(
            ('calls[%d] must specify either bucket_id or space_name and key')
                :format(operation_index),
            error_call_data,
            false
        )
    end

    if not has_bucket_id and not (has_space_name and has_key) then
        return nil, storage_call_errors.new(
            ('calls[%d] must specify bucket_id or both space_name and key')
                :format(operation_index),
            error_call_data,
            false
        )
    end

    if has_space_name and type(call_data.space_name) ~= 'string' then
        return nil, storage_call_errors.new(
            ('calls[%d].space_name must be a string'):format(operation_index),
            error_call_data,
            false
        )
    end

    return error_call_data
end

local function by_key(vshard_router, call_data, timeout)
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

    err = sharding.validate_bucket_id(bucket_id, 'sharding function result')
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

function routing.call(vshard_router, call_data, operation_index, timeout)
    local error_call_data, err = validate_call(call_data, operation_index)
    if err ~= nil then
        return nil, err
    end

    local route_data
    if call_data.bucket_id ~= nil then
        local context = ('calls[%d]'):format(operation_index)
        err = sharding.validate_bucket_id(call_data.bucket_id, context)
        if err ~= nil then
            return nil, storage_call_errors.new(
                storage_call_errors.message(err),
                error_call_data,
                false
            )
        end

        route_data = {
            bucket_id = call_data.bucket_id,
            skip_sharding_hash_check = true,
        }
    else
        route_data, err = by_key(vshard_router, call_data, timeout)
        if err ~= nil then
            return nil, storage_call_errors.new(
                storage_call_errors.message(err),
                error_call_data,
                false
            )
        end
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

function routing.single(vshard_router, opts, timeout)
    local routed_call, err = routing.call(vshard_router, {
        func_name = '',
        args = {},
        bucket_id = opts.bucket_id,
        space_name = opts.space_name,
        key = opts.key,
    }, 1, timeout)
    if err ~= nil then
        return nil, storage_call_errors.class:new(
            '%s',
            storage_call_errors.message(err)
        )
    end

    return routed_call
end

return routing
