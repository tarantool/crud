local fiber = require('fiber')

local sharding_metadata_cache = {}

sharding_metadata_cache.SHARDING_KEY_MAP_NAME = "sharding_key_as_index_obj_map"
sharding_metadata_cache.SHARDING_FUNC_MAP_NAME = "sharding_func_map"
sharding_metadata_cache[sharding_metadata_cache.SHARDING_KEY_MAP_NAME] = nil
sharding_metadata_cache[sharding_metadata_cache.SHARDING_FUNC_MAP_NAME] = nil
sharding_metadata_cache.fetch_lock = fiber.channel(1)
sharding_metadata_cache.is_part_of_pk = {}

function sharding_metadata_cache.drop_caches()
    sharding_metadata_cache[sharding_metadata_cache.SHARDING_KEY_MAP_NAME] = nil
    sharding_metadata_cache[sharding_metadata_cache.SHARDING_FUNC_MAP_NAME] = nil
    if sharding_metadata_cache.fetch_lock ~= nil then
        sharding_metadata_cache.fetch_lock:close()
    end
    sharding_metadata_cache.fetch_lock = fiber.channel(1)
    sharding_metadata_cache.is_part_of_pk = {}
end

return sharding_metadata_cache
