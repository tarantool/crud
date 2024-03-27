local fiber = require('fiber')
local errors = require('errors')
local log = require('log')

local call = require('crud.common.call')
local const = require('crud.common.const')
local utils = require('crud.common.utils')
local dev_checks = require('crud.common.dev_checks')
local router_cache = require('crud.common.sharding.router_metadata_cache')
local storage_cache = require('crud.common.sharding.storage_metadata_cache')
local sharding_func = require('crud.common.sharding.sharding_func')
local sharding_key = require('crud.common.sharding.sharding_key')
local sharding_utils = require('crud.common.sharding.utils')

local FetchShardingMetadataError = errors.new_class('FetchShardingMetadataError', {capture_stack = false})

local FETCH_FUNC_NAME = 'fetch_on_storage'
local CRUD_FETCH_FUNC_NAME = utils.get_storage_call(FETCH_FUNC_NAME)

local sharding_metadata_module = {}

-- Function decorator that is used to prevent _fetch_on_router() from being
-- called concurrently by different fibers.
local function locked(f)
    dev_checks('function')

    return function(vshard_router, space_name, metadata_map_name, timeout)
        local timeout_deadline = fiber.clock() + timeout

        local cache = router_cache.get_instance(vshard_router)

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
        local status, err = pcall(f, vshard_router, space_name, metadata_map_name, timeout)
        cache.fetch_lock:get()
        if not status or err ~= nil then
            return err
        end
    end
end

-- Return a map with metadata or nil when spaces box.space._ddl_sharding_key and
-- box.space._ddl_sharding_func are not available on storage.
function sharding_metadata_module.fetch_on_storage()
    local sharding_key_space = box.space._ddl_sharding_key
    local sharding_func_space = box.space._ddl_sharding_func

    if sharding_key_space == nil and sharding_func_space == nil then
        return nil
    end

    local metadata_map = {}

    if sharding_key_space ~= nil then
        for _, tuple in sharding_key_space:pairs() do
            local space_name = tuple[sharding_utils.SPACE_NAME_FIELDNO]
            local sharding_key_def = tuple[sharding_utils.SPACE_SHARDING_KEY_FIELDNO]
            local space = box.space[space_name]

            if space ~= nil then
                local space_format = space:format()
                metadata_map[space_name] = {
                    sharding_key_def = sharding_key_def,
                    sharding_key_hash = storage_cache.get_sharding_key_hash(space_name),
                    space_format = space_format,
                }
            else
                log.warn('Found sharding info for %q, but space not exists. ' ..
                         'Ensure that you did a proper cleanup after DDL space drop.',
                         space_name)
            end
        end
    end

    if sharding_func_space ~= nil then
        for _, tuple in sharding_func_space:pairs() do
            local space_name = tuple[sharding_utils.SPACE_NAME_FIELDNO]
            local sharding_func_def = sharding_utils.extract_sharding_func_def(tuple)
            metadata_map[space_name] = metadata_map[space_name] or {}
            metadata_map[space_name].sharding_func_def = sharding_func_def
            metadata_map[space_name].sharding_func_hash = storage_cache.get_sharding_func_hash(space_name)
        end
    end

    return metadata_map
end

-- Under high load we may get a case when more than one fiber will fetch
-- metadata from storages. It is not good from performance point of view.
-- locked() wraps a _fetch_on_router() to limit a number of fibers that fetches
-- a sharding metadata by a single one, other fibers will wait while
-- cache.fetch_lock become unlocked during timeout passed to
-- _fetch_on_router().
-- metadata_map_name == nil means forced reload.
local _fetch_on_router = locked(function(vshard_router, space_name, metadata_map_name, timeout)
    dev_checks('table', 'string', '?string', 'number')

    local cache = router_cache.get_instance(vshard_router)

    if (metadata_map_name ~= nil) and (cache[metadata_map_name]) ~= nil then
        return
    end

    local metadata_map, err = call.any(vshard_router, CRUD_FETCH_FUNC_NAME, {}, {
        timeout = timeout
    })
    if err ~= nil then
        return err
    end
    if metadata_map == nil then
        cache[router_cache.SHARDING_KEY_MAP_NAME] = {}
        cache[router_cache.SHARDING_FUNC_MAP_NAME] = {}
        cache[router_cache.META_HASH_MAP_NAME] = {
            [router_cache.SHARDING_KEY_MAP_NAME] = {},
            [router_cache.SHARDING_FUNC_MAP_NAME] = {},
        }
        return
    end

    local err = sharding_key.construct_as_index_obj_cache(vshard_router, metadata_map, space_name)
    if err ~= nil then
        return err
    end

    local err = sharding_func.construct_as_callable_obj_cache(vshard_router, metadata_map, space_name)
    if err ~= nil then
        return err
    end
end)

local function fetch_on_router(vshard_router, space_name, metadata_map_name, timeout)
    local cache = router_cache.get_instance(vshard_router)

    if cache[metadata_map_name] ~= nil then
        return {
            value = cache[metadata_map_name][space_name],
            hash = cache[router_cache.META_HASH_MAP_NAME][metadata_map_name][space_name]
        }
    end

    local timeout = timeout or const.FETCH_SHARDING_METADATA_TIMEOUT
    local err = _fetch_on_router(vshard_router, space_name, metadata_map_name, timeout)
    if err ~= nil then
        return nil, err
    end

    if cache[metadata_map_name] ~= nil then
        return {
            value = cache[metadata_map_name][space_name],
            hash = cache[router_cache.META_HASH_MAP_NAME][metadata_map_name][space_name],
        }
    end

    return nil, FetchShardingMetadataError:new(
        "Fetching sharding key for space '%s' is failed", space_name)
end

-- Get sharding index for a certain space.
--
-- Return:
--  - sharding key as index object, when sharding key definition found on
--  storage.
--  - nil, when sharding key definition was not found on storage. Pay attention
--  that nil without error is a successfull return value.
--  - nil and error, when something goes wrong on fetching attempt.
--
function sharding_metadata_module.fetch_sharding_key_on_router(vshard_router, space_name, timeout)
    dev_checks('table', 'string', '?number')

    return fetch_on_router(vshard_router, space_name, router_cache.SHARDING_KEY_MAP_NAME, timeout)
end

-- Get sharding func for a certain space.
--
-- Return:
--  - sharding func as callable object, when sharding func definition found on
--  storage.
--  - nil, when sharding func definition was not found on storage. Pay attention
--  that nil without error is a successfull return value.
--  - nil and error, when something goes wrong on fetching attempt.
--
function sharding_metadata_module.fetch_sharding_func_on_router(vshard_router, space_name, timeout)
    dev_checks('table', 'string', '?number')

    return fetch_on_router(vshard_router, space_name, router_cache.SHARDING_FUNC_MAP_NAME, timeout)
end

function sharding_metadata_module.update_sharding_key_cache(vshard_router, space_name)
    router_cache.drop_instance(vshard_router)

    return sharding_metadata_module.fetch_sharding_key_on_router(vshard_router, space_name)
end

function sharding_metadata_module.update_sharding_func_cache(vshard_router, space_name)
    router_cache.drop_instance(vshard_router)

    return sharding_metadata_module.fetch_sharding_func_on_router(vshard_router, space_name)
end

function sharding_metadata_module.reload_sharding_cache(vshard_router, space_name)
    router_cache.drop_instance(vshard_router)

    local err = _fetch_on_router(vshard_router, space_name, nil, const.FETCH_SHARDING_METADATA_TIMEOUT)
    if err ~= nil then
        log.warn('Failed to reload sharding cache: %s', err)
    end
end

sharding_metadata_module.storage_api = {[FETCH_FUNC_NAME] = sharding_metadata_module.fetch_on_storage}

return sharding_metadata_module
