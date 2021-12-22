local fiber = require('fiber')
local errors = require('errors')

local call = require('crud.common.call')
local const = require('crud.common.const')
local dev_checks = require('crud.common.dev_checks')
local cache = require('crud.common.sharding.sharding_metadata_cache')
local sharding_key = require('crud.common.sharding.sharding_key')

local FetchShardingMetadataError = errors.new_class('FetchShardingMetadataError', {capture_stack = false})

local FETCH_FUNC_NAME = '_crud.fetch_on_storage'

local sharding_metadata_module = {}

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
            return FetchShardingMetadataError:new(
                "Timeout for fetching sharding metadata is exceeded")
        end
        local timeout = timeout_deadline - fiber.clock()
        local status, err = pcall(f, timeout, ...)
        cache.fetch_lock:get()
        if not status or err ~= nil then
            return err
        end
    end
end

-- Return a map with metadata or nil when space box.space._ddl_sharding_key is
-- not available on storage.
function sharding_metadata_module.fetch_on_storage()
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
local _fetch_on_router = locked(function(timeout, space_name, metadata_map_name)
    dev_checks('number', 'string', 'string')

    if cache[metadata_map_name] ~= nil then
        return
    end

    local metadata_map, err = call.any(FETCH_FUNC_NAME, {}, {
        timeout = timeout
    })
    if err ~= nil then
        return err
    end
    if metadata_map == nil then
        cache[cache.SHARDING_KEY_MAP_NAME] = {}
        return
    end

    local err = sharding_key.construct_as_index_obj_cache(metadata_map, space_name)
    if err ~= nil then
        return err
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
local function fetch_on_router(space_name, metadata_map_name, timeout)
    if cache[metadata_map_name] ~= nil then
        return cache[metadata_map_name][space_name]
    end

    local timeout = timeout or const.FETCH_SHARDING_METADATA_TIMEOUT
    local err = _fetch_on_router(timeout, space_name, metadata_map_name)
    if err ~= nil then
        return nil, err
    end

    if cache[metadata_map_name] ~= nil then
        return cache[metadata_map_name][space_name]
    end

    return nil, FetchShardingMetadataError:new(
        "Fetching sharding key for space '%s' is failed", space_name)
end

function sharding_metadata_module.fetch_sharding_key_on_router(space_name, timeout)
    dev_checks('string', '?number')

    return fetch_on_router(space_name, cache.SHARDING_KEY_MAP_NAME, timeout)
end

function sharding_metadata_module.update_sharding_key_cache(space_name)
    cache.drop_caches()
    return sharding_metadata_module.fetch_sharding_key_on_router(space_name)
end

function sharding_metadata_module.init()
   _G._crud.fetch_on_storage = sharding_metadata_module.fetch_on_storage
end

return sharding_metadata_module
