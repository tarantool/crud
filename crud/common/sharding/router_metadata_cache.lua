local fiber = require('fiber')

local router_metadata_cache = {}

router_metadata_cache.SHARDING_KEY_MAP_NAME = "sharding_key_as_index_obj_map"
router_metadata_cache.SHARDING_FUNC_MAP_NAME = "sharding_func_map"
router_metadata_cache.META_HASH_MAP_NAME = "sharding_meta_hash_map"
router_metadata_cache[router_metadata_cache.SHARDING_KEY_MAP_NAME] = nil
router_metadata_cache[router_metadata_cache.SHARDING_FUNC_MAP_NAME] = nil
router_metadata_cache[router_metadata_cache.META_HASH_MAP_NAME] = {}
router_metadata_cache.fetch_lock = fiber.channel(1)
router_metadata_cache.is_part_of_pk = {}

function router_metadata_cache.drop_caches()
    router_metadata_cache[router_metadata_cache.SHARDING_KEY_MAP_NAME] = nil
    router_metadata_cache[router_metadata_cache.SHARDING_FUNC_MAP_NAME] = nil
    router_metadata_cache[router_metadata_cache.META_HASH_MAP_NAME] = {}
    if router_metadata_cache.fetch_lock ~= nil then
        router_metadata_cache.fetch_lock:close()
    end
    router_metadata_cache.fetch_lock = fiber.channel(1)
    router_metadata_cache.is_part_of_pk = {}
end

return router_metadata_cache