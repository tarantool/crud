local log = require('log')

local sharding_metadata_module = require('crud.common.sharding.sharding_metadata')
local utils = require('crud.common.utils')

local sharding_key_cache = {}

-- This method is exported here because
-- we already have customers using old API
-- for updating sharding key cache in their
-- projects like `require('crud.common.sharding_key').update_cache()`
function sharding_key_cache.update_cache(space_name, vshard_router)
    log.warn("require('crud.common.sharding_key').update_cache()" ..
             "is deprecated and will be removed in future releases")

    local vshard_router, err = utils.get_vshard_router_instance(vshard_router)
    if err ~= nil then
        return nil, err
    end

    return sharding_metadata_module.update_sharding_key_cache(vshard_router, space_name)
end

return sharding_key_cache
