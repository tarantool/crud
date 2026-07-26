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

local function is_group_a_crud_router(group)
    return has_role(group, 'roles.crud-router')
end

local function is_group_a_crud_storage(group)
    return has_role(group, 'roles.crud-storage')
end

local function is_replicaset_has_role(group, replicaset, role)
    return has_role(group, role) or has_role(replicaset, role)
end

local function is_replicaset_a_crud_router(group, replicaset)
    return is_replicaset_has_role(group, replicaset, 'roles.crud-router')
end

local function is_replicaset_a_crud_storage(group, replicaset)
    return is_replicaset_has_role(group, replicaset, 'roles.crud-storage')
end

local utils = {}
function utils.dump_pretty(o, max_depth, __current_depth, __indent)
    max_depth = max_depth or 8
    __current_depth = __current_depth or 0
    __indent = __indent or 0

    if __current_depth > max_depth then
        return '<MAX DEPTH>'
    end

    if type(o) == 'table' then
        local s = '{\n'
        __indent = __indent + 1
        for k,v in pairs(o) do
            if type(k) == 'table' then
                k = '"<' .. tostring(k) .. '>"'
            elseif type(k) ~= 'number' then
                k = '"'..k..'"'
            end
            s = s .. string.rep(' ', 4*__indent) .. '['..k..'] = ' ..
                    utils.dump_pretty(v, max_depth, __current_depth + 1, __indent) .. ',\n'
        end
        __indent = __indent - 1
        return s .. string.rep(' ', 4*__indent) .. '}'
    else
        return tostring(o)
    end
end

local fio = require('fio')
local xlog = require('xlog')
local function dump_cluster_xlogs(servers)
    print('###################### SNAP AND XLOG DUMP ######################')
    for _, server in ipairs(servers) do
        local xlogpath = fio.pathjoin(server.chdir, 'var', 'lib', server.alias)
        if not fio.path.is_dir(xlogpath) then
            error(xlogpath .. ' is not a directory')
        end

        print('###################### ' .. server.alias .. ' ######################')
        for _, fname in ipairs(fio.listdir(xlogpath)) do
            print('########## ' .. fname)
            print(utils.dump_pretty(xlog.pairs(fio.pathjoin(xlogpath, fname)):totable()))
        end
    end
    print('###################### END OF DUMP ######################')
end

return {
    is_group_a_sharding_router = is_group_a_sharding_router,
    is_group_a_sharding_storage = is_group_a_sharding_storage,
    is_replicaset_a_sharding_router = is_replicaset_a_sharding_router,
    is_replicaset_a_sharding_storage = is_replicaset_a_sharding_storage,

    is_group_a_crud_router = is_group_a_crud_router,
    is_group_a_crud_storage = is_group_a_crud_storage,
    is_replicaset_a_crud_router = is_replicaset_a_crud_router,
    is_replicaset_a_crud_storage = is_replicaset_a_crud_storage,

    dump_pretty = utils.dump_pretty,
    dump_cluster_xlogs = dump_cluster_xlogs,
}
