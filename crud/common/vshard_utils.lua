local vshard = require('vshard')

local vshard_utils = {}

function vshard_utils.get_self_vshard_replicaset()
    local box_info = box.info()

    local ok, storage_info = pcall(vshard.storage.info)
    assert(ok, 'vshard.storage.cfg() must be called first')

    if vshard_utils.get_vshard_identification_mode() == 'name_as_key' then
        local replicaset_name = box_info.replicaset.name

        return replicaset_name, storage_info.replicasets[replicaset_name]
    else
        local replicaset_uuid
        if box_info.replicaset ~= nil then
            replicaset_uuid = box_info.replicaset.uuid
        else
            replicaset_uuid = box_info.cluster.uuid
        end

        return replicaset_uuid, storage_info.replicasets[replicaset_uuid]
    end
end

function vshard_utils.get_self_vshard_replica_id()
    local box_info = box.info()

    if vshard_utils.get_vshard_identification_mode() == 'name_as_key' then
        return box_info.name
    else
        return box_info.uuid
    end
end

function vshard_utils.get_replicaset_id(vshard_router, replicaset)
    -- https://github.com/tarantool/vshard/issues/460.
    local known_replicasets = vshard_router:routeall()

    for known_replicaset_id, known_replicaset in pairs(known_replicasets) do
        if known_replicaset == replicaset then
            return known_replicaset_id
        end
    end

    return nil
end

function vshard_utils.get_vshard_identification_mode()
    -- https://github.com/tarantool/vshard/issues/460.
    assert(vshard.storage.internal.current_cfg ~= nil, 'available only on vshard storage')
    return vshard.storage.internal.current_cfg.identification_mode
end

return vshard_utils
