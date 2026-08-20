local sharding = require('crud.common.sharding')
local sharding_key = require('crud.common.sharding.sharding_key')
local sharding_metadata = require(
    'crud.common.sharding.sharding_metadata'
)
local utils = require('crud.common.utils')
local storage_call_errors = require('crud.storage_call.errors')

local routing = {}

local function by_key(vshard_router, space_name, key, timeout)
    local space, err = utils.get_space(space_name, vshard_router, {
        timeout = timeout,
        read_only = false,
    })
    if err ~= nil then
        return nil, err
    end
    if space == nil then
        return nil, storage_call_errors.class:new(
            'Space %q does not exist',
            space_name
        )
    end

    if box.tuple.is(key) then
        key = key:totable()
    end

    local sharding_key_data
    sharding_key_data, err = sharding_metadata.fetch_sharding_key_on_router(
        vshard_router,
        space_name,
        timeout
    )
    if err ~= nil then
        return nil, err
    end

    local extracted_sharding_key
    extracted_sharding_key, err = sharding_key.extract_from_pk(
        vshard_router,
        space_name,
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
        space_name,
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
        space_name = space_name,
        sharding_key_hash = sharding_key_data.hash,
        sharding_func_hash = sharding_func_data.hash,
        skip_sharding_hash_check = false,
    }
end

function routing.single(vshard_router, opts, timeout)
    local has_bucket_id = opts.bucket_id ~= nil
    local has_space_name = opts.space_name ~= nil
    local has_key = opts.key ~= nil

    if has_bucket_id and (has_space_name or has_key) then
        return nil, storage_call_errors.class:new(
            'Specify either bucket_id or space_name and key'
        )
    end
    if not has_bucket_id and not (has_space_name and has_key) then
        return nil, storage_call_errors.class:new(
            'Specify bucket_id or both space_name and key'
        )
    end

    if has_bucket_id then
        local err = sharding.validate_bucket_id(
            opts.bucket_id,
            'opts.bucket_id'
        )
        if err ~= nil then
            return nil, err
        end

        return {
            bucket_id = opts.bucket_id,
            skip_sharding_hash_check = true,
        }
    end

    return by_key(
        vshard_router,
        opts.space_name,
        opts.key,
        timeout
    )
end

return routing
