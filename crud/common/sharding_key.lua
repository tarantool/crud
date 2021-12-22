local sharding_metadata_module = require('crud.common.sharding.sharding_metadata')

local sharding_key_cache = {}

-- This method is exported here because
-- we already have customers using old API
-- for updating sharding key cache in their
-- projects like `require('crud.common.sharding_key').update_cache()`
function sharding_key_cache.update_cache(space_name)
    return sharding_metadata_module.update_sharding_key_cache(space_name)
end

return sharding_key_cache
