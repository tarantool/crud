local vshard = require('vshard')
local errors = require('errors')

local BucketIDError = errors.new_class("BucketIDError", {capture_stack = false})

local utils = require('crud.common.utils')

local sharding = {}

local sharding_key_cache

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
        return nil, BucketIDError:new("Failed to get bucket ID fielno: %s", err)
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

-- Get sharding key (actually field names) for all spaces in schema
-- and cache it's value to speedup access.
function sharding.get_ddl_sharding_key(space_name)
    if box.space._ddl_sharding_key == nil then
        return nil
    end

    if sharding_key_cache == nil then
        sharding_key_cache = box.space._ddl_sharding_key:select{}
    end

    if space_name ~= nil then
        return sharding_key_cache[space_name]
    else
        return sharding_key_cache
    end
end

return sharding
