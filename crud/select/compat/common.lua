local vshard = require('vshard')
local errors = require('errors')

local GetReplicasetsError = errors.new_class('GetReplicasetsError')

local SELECT_FUNC_NAME = '_crud.select_on_storage'
local DEFAULT_BATCH_SIZE = 100

local function get_replicasets_by_sharding_key(bucket_id)
    local replicaset, err = vshard.router.route(bucket_id)
    if replicaset == nil then
        return nil, GetReplicasetsError:new("Failed to get replicaset for bucket_id %s: %s", bucket_id, err.err)
    end

    return {
        [replicaset.uuid] = replicaset,
    }
end

return {
    get_replicasets_by_sharding_key = get_replicasets_by_sharding_key,
    SELECT_FUNC_NAME = SELECT_FUNC_NAME,
    DEFAULT_BATCH_SIZE = DEFAULT_BATCH_SIZE,
}
