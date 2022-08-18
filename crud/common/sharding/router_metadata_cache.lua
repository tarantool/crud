local fiber = require('fiber')

local router_metadata_cache = {}

router_metadata_cache.SHARDING_KEY_MAP_NAME = "sharding_key_as_index_obj_map"
router_metadata_cache.SHARDING_FUNC_MAP_NAME = "sharding_func_map"
router_metadata_cache.META_HASH_MAP_NAME = "sharding_meta_hash_map"

local internal_storage = {}

function router_metadata_cache.get_instance(vshard_router)
    local name = vshard_router.name

    if internal_storage[name] ~= nil then
        return internal_storage[name]
    end

    internal_storage[name] = {
        [router_metadata_cache.SHARDING_KEY_MAP_NAME] = nil,
        [router_metadata_cache.SHARDING_FUNC_MAP_NAME] = nil,
        [router_metadata_cache.META_HASH_MAP_NAME] = {},
        fetch_lock = fiber.channel(1),
        is_part_of_pk = {}
    }

    return internal_storage[name]
end

function router_metadata_cache.drop_instance(vshard_router)
    local name = vshard_router.name

    if internal_storage[name] == nil then
        return
    end

    if internal_storage[name].fetch_lock ~= nil then
        internal_storage[name].fetch_lock:close()
    end

    internal_storage[name] = nil
end

function router_metadata_cache.drop_caches()
    for name, _ in pairs(internal_storage) do
        router_metadata_cache.drop_instance(name)
    end

    internal_storage = {}
end

return router_metadata_cache