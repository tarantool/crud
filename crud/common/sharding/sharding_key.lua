local fiber = require('fiber')
local errors = require('errors')

local call = require('crud.common.call')
local const = require('crud.common.const')
local dev_checks = require('crud.common.dev_checks')
local cache = require('crud.common.sharding.sharding_key_cache')
local utils = require('crud.common.utils')

local ShardingKeyError = errors.new_class("ShardingKeyError", {capture_stack = false})
local FetchShardingKeyError = errors.new_class('FetchShardingKeyError', {capture_stack = false})
local WrongShardingConfigurationError = errors.new_class('WrongShardingConfigurationError',  {capture_stack = false})

local FETCH_FUNC_NAME = '_crud.fetch_on_storage'

local sharding_key_module = {}

-- Function decorator that is used to prevent _fetch_on_router() from being
-- called concurrently by different fibers.
local function locked(f)
    dev_checks('function')

    return function(timeout, ...)
        local timeout_deadline = fiber.clock() + timeout
        local ok = cache.fetch_lock:put(true, timeout)
        -- channel:put() returns false in two cases: when timeout is exceeded
        -- or channel has been closed. However error message describes only
        -- first reason, I'm not sure we need to disclose to users such details
        -- like problems with synchronization objects.
        if not ok then
            return FetchShardingKeyError:new(
                "Timeout for fetching sharding key is exceeded")
        end
        local timeout = timeout_deadline - fiber.clock()
        local status, err = pcall(f, timeout, ...)
        cache.fetch_lock:get()
        if not status or err ~= nil then
            return err
        end
    end
end

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

-- Return a map with metadata or nil when space box.space._ddl_sharding_key is
-- not available on storage.
function sharding_key_module.fetch_on_storage()
    local sharding_key_space = box.space._ddl_sharding_key
    if sharding_key_space == nil then
        return nil
    end

    local SPACE_NAME_FIELDNO = 1
    local SPACE_SHARDING_KEY_FIELDNO = 2
    local metadata_map = {}
    for _, tuple in sharding_key_space:pairs() do
        local space_name = tuple[SPACE_NAME_FIELDNO]
        local sharding_key_def = tuple[SPACE_SHARDING_KEY_FIELDNO]
        local space_format = box.space[space_name]:format()
        metadata_map[space_name] = {
            sharding_key_def = sharding_key_def,
            space_format = space_format,
        }
    end

    return metadata_map
end

-- Under high load we may get a case when more than one fiber will fetch
-- metadata from storages. It is not good from performance point of view.
-- locked() wraps a _fetch_on_router() to limit a number of fibers that fetches
-- a sharding metadata by a single one, other fibers will wait while
-- cache.fetch_lock become unlocked during timeout passed to
-- _fetch_on_router().
local _fetch_on_router = locked(function(timeout)
    dev_checks('number')

    if cache.sharding_key_as_index_obj_map ~= nil then
        return
    end

    local metadata_map, err = call.any(FETCH_FUNC_NAME, {}, {
        timeout = timeout
    })
    if err ~= nil then
        return err
    end
    if metadata_map == nil then
        cache.sharding_key_as_index_obj_map = {}
        return
    end

    cache.sharding_key_as_index_obj_map = {}
    for space_name, metadata in pairs(metadata_map) do
        local sharding_key_as_index_obj, err = as_index_object(space_name,
                                                    metadata.space_format,
                                                    metadata.sharding_key_def)
        if err ~= nil then
            return err
        end
        cache.sharding_key_as_index_obj_map[space_name] = sharding_key_as_index_obj
    end
end)

-- Get sharding index for a certain space.
--
-- Return:
--  - sharding key as index object, when sharding key definition found on
--  storage.
--  - nil, when sharding key definition was not found on storage. Pay attention
--  that nil without error is a successfull return value.
--  - nil and error, when something goes wrong on fetching attempt.
--
function sharding_key_module.fetch_on_router(space_name, timeout)
    dev_checks('string', '?number')

    if cache.sharding_key_as_index_obj_map ~= nil then
        return cache.sharding_key_as_index_obj_map[space_name]
    end

    local timeout = timeout or const.FETCH_SHARDING_KEY_TIMEOUT
    local err = _fetch_on_router(timeout)
    if err ~= nil then
        if cache.sharding_key_as_index_obj_map ~= nil then
            return cache.sharding_key_as_index_obj_map[space_name]
        end
        return nil, err
    end

    if cache.sharding_key_as_index_obj_map ~= nil then
        return cache.sharding_key_as_index_obj_map[space_name]
    end

    return nil, FetchShardingKeyError:new(
        "Fetching sharding key for space '%s' is failed", space_name)
end

function sharding_key_module.update_cache(space_name)
    cache.drop_caches()
    return sharding_key_module.fetch_on_router(space_name)
end

-- Make sure sharding key definition is a part of primary key.
local function is_part_of_pk(space_name, primary_index_parts, sharding_key_as_index_obj)
    dev_checks('string', 'table', 'table')

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
function sharding_key_module.extract_from_pk(space_name, primary_index_parts, primary_key, timeout)
    dev_checks('string', 'table', '?', '?number')

    local sharding_key_as_index_obj, err = sharding_key_module.fetch_on_router(space_name, timeout)
    if err ~= nil then
        return nil, err
    end
    if sharding_key_as_index_obj == nil then
        return primary_key
    end

    local res = is_part_of_pk(space_name, primary_index_parts, sharding_key_as_index_obj)
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

function sharding_key_module.init()
   _G._crud.fetch_on_storage = sharding_key_module.fetch_on_storage
end

sharding_key_module.internal = {
    as_index_object = as_index_object,
    extract_from_index = extract_from_index,
    is_part_of_pk = is_part_of_pk,
}

return sharding_key_module
