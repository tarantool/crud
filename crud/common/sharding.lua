local vshard = require('vshard')
local errors = require('errors')

local BucketIDError = errors.new_class("BucketIDError", {capture_stack = false})

local utils = require('crud.common.utils')

local sharding = {}

function sharding.key_get_bucket_id(key, specified_bucket_id)
    if specified_bucket_id ~= nil then
        return specified_bucket_id
    end

    return vshard.router.bucket_id_strcrc32(key)
end

function sharding.tuple_get_bucket_id(tuple, space, specified_bucket_id)
    if specified_bucket_id ~= nil then
        return specified_bucket_id
    end

    local key = utils.extract_key(tuple, space.index[0].parts)
    return sharding.key_get_bucket_id(key)
end

function sharding.tuple_set_and_return_bucket_id(tuple, space, specified_bucket_id)
    local bucket_id_fieldno, err = utils.get_bucket_id_fieldno(space)
    if err ~= nil then
        return nil, BucketIDError:new("Failed to get bucket ID fielno:", err)
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

    if tuple[bucket_id_fieldno] == nil then
        tuple[bucket_id_fieldno] = sharding.tuple_get_bucket_id(tuple, space)
    end

    local bucket_id = tuple[bucket_id_fieldno]
    return bucket_id
end

return sharding
