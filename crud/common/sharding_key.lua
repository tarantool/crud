local fiber = require('fiber')
local errors = require('errors')
local vshard = require('vshard')

local const = require('crud.common.const')
local dev_checks = require('crud.common.dev_checks')
local cache = require('crud.common.sharding_key_cache')
local utils = require('crud.common.utils')

local FetchShardingKeyError = errors.new_class('FetchShardingKeyError', {capture_stack = false})
local WrongShardingConfigurationError = errors.new_class('WrongShardingConfigurationError',  {capture_stack = false})

local FETCH_FUNC_NAME = '_crud.fetch_on_storage'

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

-- Return a map or nil when metadata is not available.
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

local sharding_key_fetch_in_progress = false
local sharding_key_fetch_cond = fiber.cond()

-- Under high load we may get a case when many fibers will fetch metadata from
-- storages. It is not good from performance point of view. reentrant_fetch()
-- wraps a fetch_on_storage() to limit a number of fibers that fetches a
-- metadata by a single one, other fibers will wait for a
-- FETCH_SHARDING_KEY_TIMEOUT and then make one more attempt to fetch metadata
-- if a cache is still empty.
local function reentrant_fetch(replicaset)
    dev_checks('table')

    if cache.sharding_key_as_index_obj_map ~= nil then
        return cache.sharding_key_as_index_obj_map
    end

    if sharding_key_fetch_in_progress == true then
        local timeout = false
        if sharding_key_fetch_cond:wait(const.FETCH_SHARDING_KEY_TIMEOUT) then
            timeout = true
        end
	if cache.sharding_key_as_index_obj_map ~= nil then
	    return cache.sharding_key_as_index_obj_map
	end
	if timeout then
            return nil, FetchShardingKeyError:new(
                            'Waiting for sharding key to be fetched is timed out')
	end
    end

    sharding_key_fetch_in_progress = true
    local metadata_map, err = replicaset:call(FETCH_FUNC_NAME, {}, {
        timeout = const.FETCH_SHARDING_KEY_TIMEOUT
    })
    if err ~= nil then
        sharding_key_fetch_in_progress = false
        sharding_key_fetch_cond:broadcast()
        return nil, err
    end

    sharding_key_fetch_in_progress = false
    sharding_key_fetch_cond:broadcast()

    if metadata_map == nil then
        return {}
    end
    local sharding_key_as_index_obj_map = {}
    for space_name, metadata in pairs(metadata_map) do
        local sharding_key_as_index_obj, err = as_index_object(space_name,
                                                    metadata.space_format,
                                                    metadata.sharding_key_def)
        if err ~= nil then
            return nil, err
        end
        sharding_key_as_index_obj_map[space_name] = sharding_key_as_index_obj
    end

    return sharding_key_as_index_obj_map
end

-- Get sharding index for a certain space.
-- Return a sharding key as index object or nil with error.
function sharding_key_module.fetch_on_router(space_name)
    dev_checks('string')

    if cache.sharding_key_as_index_obj_map == nil then
        local replicasets = vshard.router.routeall()
        local replicaset = select(2, next(replicasets))
        local err
        cache.sharding_key_as_index_obj_map, err = reentrant_fetch(replicaset)
        if err ~= nil then
            return nil, err
        end
    end

    return cache.sharding_key_as_index_obj_map[space_name]
end

function sharding_key_module.init()
   _G._crud.fetch_on_storage = sharding_key_module.fetch_on_storage
end

sharding_key_module.internal = {
    as_index_object = as_index_object,
}

return sharding_key_module
