local stash = require('crud.common.stash')
local utils = require('crud.common.sharding.utils')

local storage_metadata_cache = {}

local FUNC = 1
local KEY = 2

local cache_data = {
    [FUNC] = nil,
    [KEY] = nil,
}

local ddl_space = {
    [FUNC] = '_ddl_sharding_func',
    [KEY] = '_ddl_sharding_key',
}

local trigger_stash = stash.get(stash.name.ddl_triggers)

local function update_sharding_func_hash(old, new)
    if new ~= nil then
        local space_name = new[utils.SPACE_NAME_FIELDNO]
        local sharding_func_def = utils.extract_sharding_func_def(new)
        cache_data[FUNC][space_name] = utils.compute_hash(sharding_func_def)
    else
        local space_name = old[utils.SPACE_NAME_FIELDNO]
        cache_data[FUNC][space_name] = nil
    end
end

local function update_sharding_key_hash(old, new)
    if new ~= nil then
        local space_name = new[utils.SPACE_NAME_FIELDNO]
        local sharding_key_def = new[utils.SPACE_SHARDING_KEY_FIELDNO]
        cache_data[KEY][space_name] = utils.compute_hash(sharding_key_def)
    else
        local space_name = old[utils.SPACE_NAME_FIELDNO]
        cache_data[KEY][space_name] = nil
    end
end

local update_hash = {
    [FUNC] = update_sharding_func_hash,
    [KEY] = update_sharding_key_hash,
}

local function init_cache(section)
    cache_data[section] = {}

    local space = box.space[ddl_space[section]]

    local update_hash_func = update_hash[section]

    -- Remove old trigger if there was some code reload.
    -- It is possible that ddl space was dropped and created again,
    -- so removing non-existing trigger will cause fail;
    -- thus we use pcall.
    pcall(space.on_replace, space, nil, trigger_stash[section])

    trigger_stash[section] = space:on_replace(
        function(old, new)
            return update_hash_func(old, new)
        end
    )

    for _, tuple in space:pairs() do
        local space_name = tuple[utils.SPACE_NAME_FIELDNO]
        -- If the cache record for a space is not nil, it means
        -- that it was already set to up-to-date value with trigger.
        -- It is more like an overcautiousness since the cycle
        -- isn't expected to yield, but let it be here.
        if cache_data[section][space_name] == nil then
            update_hash_func(nil, tuple)
        end
    end
end

local function get_sharding_hash(space_name, section)
    if box.space[ddl_space[section]] == nil then
        return nil
    end

    -- If one would drop and rebuild ddl spaces fom scratch manually,
    -- caching is likely to break.
    if cache_data[section] == nil then
        init_cache(section)
    end

    return cache_data[section][space_name]
end

function storage_metadata_cache.get_sharding_func_hash(space_name)
    return get_sharding_hash(space_name, FUNC)
end

function storage_metadata_cache.get_sharding_key_hash(space_name)
    return get_sharding_hash(space_name, KEY)
end

function storage_metadata_cache.drop_caches()
    cache_data = {}
end

return storage_metadata_cache
