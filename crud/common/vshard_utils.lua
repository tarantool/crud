local luri = require('uri')

local vshard = require('vshard')

local vshard_utils = {}

-- get_replicasets returns vshard replicasets from vshard.storage.internal
-- copy pasted from https://github.com/tarantool/vshard/blob/9ad0e2726a5137398f50fe88ac105f53e446c3e2/vshard/storage/init.lua#L3962-L3984
-- todo: remove after https://github.com/tarantool/vshard/issues/565 closed
local function get_replicasets()
    local ireplicasets = {}
    local M = vshard.storage.internal
    local is_named = M.this_replica.id == M.this_replica.name
    for id, replicaset in pairs(M.replicasets) do
        local master = replicaset.master
        local master_info
        if replicaset.is_master_auto then
            master_info = 'auto'
        elseif not master then
            master_info = 'missing'
        else
            local uri = master:safe_uri()
            local conn = master.conn
            master_info = {
                uri = uri, uuid = conn and conn.peer_uuid,
                name = is_named and master.name or nil,
                state = conn and conn.state, error = conn and conn.error,
            }
        end
        ireplicasets[id] = {
            uuid = replicaset.uuid,
            name = is_named and replicaset.name or nil,
            master = master_info,
        }
    end
    return ireplicasets
end

function vshard_utils.get_self_vshard_replicaset()
    local box_info = vshard_utils.__get_box_info()

    local ok, storage_info = vshard_utils.__get_storage_info()
    assert(ok, 'vshard.storage.cfg() must be called first')

    local is_needs_upgrade_2_11 = vshard_utils.is_schema_needs_upgrade_from_2_11()

    if vshard_utils.get_vshard_identification_mode() == 'name_as_key' and not is_needs_upgrade_2_11 then
        local replicaset_name = box_info.replicaset.name
        return replicaset_name, storage_info.replicasets[replicaset_name]
    else
        local replicaset_uuid
        if box_info.replicaset ~= nil then
            replicaset_uuid = box_info.replicaset.uuid
        else
            replicaset_uuid = box_info.cluster.uuid
        end

        for _, rep in pairs(storage_info.replicasets) do
            if rep.uuid == replicaset_uuid then
                return replicaset_uuid, rep
            end
        end
        error(('failed to find replicaset by uuid %s'):format(replicaset_uuid))
    end
end

-- for unit tests
function vshard_utils.__get_storage_info()
    -- cartridge disable vshard.storage on the very first apply_config
    -- here we check this and do not call vshard.storage.info
    -- todo: remove after https://github.com/tarantool/vshard/issues/565 closed
    if vshard.storage.internal.is_enabled == false then
        return true, {
            replicasets = get_replicasets(),
        }
    end
    return pcall(vshard.storage.info)
end

-- for unit tests
function vshard_utils.__get_box_info()
    return box.info()
end

function vshard_utils.is_schema_needs_upgrade_from_2_11()
    local version_tup = box.space._schema:get({'version'})
    local version_str = ("%s.%s"):format(version_tup[2], version_tup[3])
    if version_str == "2.11" and box.internal.schema_needs_upgrade() then
        return true
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

function vshard_utils.get_this_replica_user()
    local replicaset_key, replicaset = vshard_utils.get_self_vshard_replicaset()

    if replicaset == nil or replicaset.master == nil then
        error(string.format(
            'Failed to find a vshard configuration ' ..
            'for storage replicaset with key %q.',
            replicaset_key))
    end

    local uri
    if replicaset.master == 'auto' then
        -- https://github.com/tarantool/vshard/issues/467.
        uri = vshard.storage.internal.this_replica.uri
    else
        uri = replicaset.master.uri
    end

    return luri.parse(uri).login
end

function vshard_utils.get_replicaset_master(replicaset, opts)
    opts = opts or {}
    local cached = opts.cached or false

    if (not cached) and replicaset.locate_master ~= nil then
        replicaset:locate_master()
    end

    return replicaset.master
end

return vshard_utils
