local fiber = require('fiber')

local sharding_key_cache = {}

sharding_key_cache.sharding_key_as_index_obj_map = nil
sharding_key_cache.fetch_lock = fiber.channel(1)

return sharding_key_cache
