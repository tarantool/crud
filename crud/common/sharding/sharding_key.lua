local errors = require('errors')
local log = require('log')

local dev_checks = require('crud.common.dev_checks')
local router_cache = require('crud.common.sharding.router_metadata_cache')
local utils = require('crud.common.utils')

local ShardingKeyError = errors.new_class("ShardingKeyError", {capture_stack = false})
local WrongShardingConfigurationError = errors.new_class('WrongShardingConfigurationError',  {capture_stack = false})

local sharding_key_module = {}

-- Build a structure similar to index, but it is not a real index object,
-- it contains only parts key with fieldno's.
local function as_index_object(space_name, space_format, sharding_key_def)
    dev_checks('string', 'table', 'table')

    local fieldnos = {}
    local fieldno_map = utils.get_format_fieldno_map(space_format)
    for _, field_name in ipairs(sharding_key_def) do
        local fieldno = fieldno_map[field_name]
        if fieldno == nil then
            return nil, WrongShardingConfigurationError:new(
                "No such field (%s) in a space format (%s)", field_name, space_name)
        end
        table.insert(fieldnos, {fieldno = fieldno})
    end

    return {parts = fieldnos}
end

-- Make sure sharding key definition is a part of primary key.
local function is_part_of_pk(cache, space_name, primary_index_parts, sharding_key_as_index_obj)
    dev_checks('table', 'string', 'table', 'table')

    if cache.is_part_of_pk[space_name] ~= nil then
        return cache.is_part_of_pk[space_name]
    end

    local is_part_of_pk = true
    local pk_fieldno_map = utils.get_index_fieldno_map(primary_index_parts)
    for _, part in ipairs(sharding_key_as_index_obj.parts) do
        if pk_fieldno_map[part.fieldno] == nil then
            is_part_of_pk = false
            break
        end
    end
    cache.is_part_of_pk[space_name] = is_part_of_pk

    return is_part_of_pk
end

-- Build an array with sharding key values. Function extracts those values from
-- primary key that are part of sharding key (passed as index object).
local function extract_from_index(primary_key, primary_index_parts, sharding_key_as_index_obj)
    dev_checks('table', 'table', 'table')

    -- TODO: extract_from_index() calculates primary_index_parts on each
    -- request. It is better to cache it's value.
    -- https://github.com/tarantool/crud/issues/243
    local primary_index_fieldno_map = utils.get_index_fieldno_map(primary_index_parts)

    local sharding_key = {}
    for _, part in ipairs(sharding_key_as_index_obj.parts) do
        -- part_number cannot be nil because earlier we checked that tuple
        -- field names defined in sharding key definition are part of primary
        -- key.
        local part_number = primary_index_fieldno_map[part.fieldno]
        assert(part_number ~= nil)
        local field_value = primary_key[part_number]
        table.insert(sharding_key, field_value)
    end

    return sharding_key
end

-- Extract sharding key from pk.
-- Returns a table with sharding key or pair of nil and error.
function sharding_key_module.extract_from_pk(vshard_router, space_name, sharding_key_as_index_obj,
                                             primary_index_parts, primary_key)
    dev_checks('table', 'string', '?table', 'table', '?')

    if sharding_key_as_index_obj == nil then
        return primary_key
    end

    local cache = router_cache.get_instance(vshard_router)
    local res = is_part_of_pk(cache, space_name, primary_index_parts, sharding_key_as_index_obj)
    if res == false then
        return nil, ShardingKeyError:new(
            "Sharding key for space %q is missed in primary index, specify bucket_id",
            space_name
        )
    end
    if type(primary_key) ~= 'table' then
        primary_key = {primary_key}
    end

    return extract_from_index(primary_key, primary_index_parts, sharding_key_as_index_obj)
end

function sharding_key_module.construct_as_index_obj_cache(vshard_router, metadata_map, specified_space_name)
    dev_checks('table', 'table', 'string')

    local result_err

    local cache = router_cache.get_instance(vshard_router)
    cache[router_cache.SHARDING_KEY_MAP_NAME] = {}
    local key_cache = cache[router_cache.SHARDING_KEY_MAP_NAME]

    cache[router_cache.META_HASH_MAP_NAME][router_cache.SHARDING_KEY_MAP_NAME] = {}
    local key_hash_cache = cache[router_cache.META_HASH_MAP_NAME][router_cache.SHARDING_KEY_MAP_NAME]

    for space_name, metadata in pairs(metadata_map) do
        if metadata.sharding_key_def ~= nil then
            local sharding_key_as_index_obj, err = as_index_object(space_name,
                                                                   metadata.space_format,
                                                                   metadata.sharding_key_def)
            if err ~= nil then
                if specified_space_name == space_name then
                    result_err = err
                    log.error(err)
                else
                    log.warn(err)
                end
            end

            key_cache[space_name] = sharding_key_as_index_obj
            key_hash_cache[space_name] = metadata.sharding_key_hash
        end
    end

    return result_err
end

sharding_key_module.internal = {
    as_index_object = as_index_object,
    extract_from_index = extract_from_index,
    is_part_of_pk = is_part_of_pk,
}

return sharding_key_module
