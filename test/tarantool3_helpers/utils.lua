local function has_role(object, role)
    if object == nil then
        return false
    end

    for _, v in ipairs(object.roles or {}) do
        if v == role then
            return true
        end
    end

    return false
end

local function is_group_has_sharding_role(group, role)
    return has_role(group.sharding, role)
end

local function is_replicaset_has_sharding_role(group, replicaset, role)
    if is_group_has_sharding_role(group, role) then
        return true
    end

    return has_role(replicaset.sharding, role)
end

local function is_replicaset_a_sharding_router(group, replicaset)
    return is_replicaset_has_sharding_role(group, replicaset, 'router')
end

local function is_replicaset_a_sharding_storage(group, replicaset)
    return is_replicaset_has_sharding_role(group, replicaset, 'storage')
end

local function is_group_a_sharding_router(group)
    return is_group_has_sharding_role(group, 'router')
end

local function is_group_a_sharding_storage(group)
    return is_group_has_sharding_role(group, 'storage')
end

return {
    is_group_a_sharding_router = is_group_a_sharding_router,
    is_group_a_sharding_storage = is_group_a_sharding_storage,
    is_replicaset_a_sharding_router = is_replicaset_a_sharding_router,
    is_replicaset_a_sharding_storage = is_replicaset_a_sharding_storage,
}
