local vshard = require('vshard')
local errors = require('errors')

local BucketIDError = errors.new_class("BucketIDError", {capture_stack = false})
local GetReplicasetsError = errors.new_class('GetReplicasetsError', {capture_stack = false})

local utils = require('crud.common.utils')
local dev_checks = require('crud.common.dev_checks')
local sharding_metadata_module = require('crud.common.sharding.sharding_metadata')

local sharding = {}

function sharding.get_replicasets_by_bucket_id(bucket_id)
    local replicaset, err = vshard.router.route(bucket_id)
    if replicaset == nil then
        return nil, GetReplicasetsError:new("Failed to get replicaset for bucket_id %s: %s", bucket_id, err.err)
    end

    return {
        [replicaset.uuid] = replicaset,
    }
end

function sharding.key_get_bucket_id(space_name, key, specified_bucket_id)
    dev_checks('string', '?', '?number|cdata')

    if specified_bucket_id ~= nil then
        return specified_bucket_id
    end

    local sharding_func, err = sharding_metadata_module.fetch_sharding_func_on_router(space_name)
    if err ~= nil then
        return nil, err
    end

    if sharding_func ~= nil then
        return sharding_func(key)
    end

    return vshard.router.bucket_id_strcrc32(key)
end

function sharding.tuple_get_bucket_id(tuple, space, specified_bucket_id)
    if specified_bucket_id ~= nil then
        return specified_bucket_id
    end

    local sharding_index_parts = space.index[0].parts
    local sharding_key_as_index_obj, err = sharding_metadata_module.fetch_sharding_key_on_router(space.name)
    if err ~= nil then
        return nil, err
    end
    if sharding_key_as_index_obj ~= nil then
        sharding_index_parts = sharding_key_as_index_obj.parts
    end
    local key = utils.extract_key(tuple, sharding_index_parts)

    return sharding.key_get_bucket_id(space.name, key)
end

function sharding.tuple_set_and_return_bucket_id(tuple, space, specified_bucket_id)
    local bucket_id_fieldno, err = utils.get_bucket_id_fieldno(space)
    if err ~= nil then
        return nil, BucketIDError:new("Failed to get bucket ID fieldno: %s", err)
    end

    if specified_bucket_id ~= nil then
        if tuple[bucket_id_fieldno] == nil then
            tuple[bucket_id_fieldno] = specified_bucket_id
        else
            if tuple[bucket_id_fieldno] ~= specified_bucket_id then
                return nil, BucketIDError:new(
                    "Tuple and opts.bucket_id contain different bucket_id values: %s and %s",
                    tuple[bucket_id_fieldno], specified_bucket_id
                )
            end
        end
    end

    local bucket_id = tuple[bucket_id_fieldno]
    if bucket_id == nil then
        bucket_id, err = sharding.tuple_get_bucket_id(tuple, space)
        if err ~= nil then
            return nil, err
        end
        tuple[bucket_id_fieldno] = bucket_id
    end

    return bucket_id
end

return sharding
