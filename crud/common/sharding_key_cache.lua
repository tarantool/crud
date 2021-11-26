local fiber = require('fiber')

local sharding_key_cache = {}

sharding_key_cache.sharding_key_as_index_obj_map = nil
sharding_key_cache.fetch_lock = fiber.channel(1)
sharding_key_cache.is_part_of_pk = {}

function sharding_key_cache.drop_caches()
    sharding_key_cache.sharding_key_as_index_obj_map = nil
    if sharding_key_cache.fetch_lock ~= nil then
        sharding_key_cache.fetch_lock:close()
    end
    sharding_key_cache.fetch_lock = fiber.channel(1)
    sharding_key_cache.is_part_of_pk = {}
end

return sharding_key_cache
