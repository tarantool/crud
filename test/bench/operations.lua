local call = require('crud.common.call')

local M = {}

function M.replace_raw(space_name, tuple)
    box.space[space_name]:replace(tuple)
end

function M.replace_crud(space, tuple, opts)
    call.storage_api.call_on_storage('guest', '_crud.replace_on_storage', space, tuple, opts)
end

function M.get_raw(space_name, key)
    box.space[space_name]:get(key)
end

function M.get_crud(space, key, field_names, opts)
    call.storage_api.call_on_storage('guest', '_crud.get_on_storage', space, key, field_names, opts)
end

function M.delete_raw(space_name, key)
    box.space[space_name]:delete(key)
end

function M.delete_crud(space, key, field_names, opts)
    call.storage_api.call_on_storage('guest', '_crud.delete_on_storage', space, key, field_names, opts)
end

function M.insert_raw(space_name, tuple)
    box.space[space_name]:insert(tuple)
end

function M.insert_crud(space, tuple, opts)
    call.storage_api.call_on_storage('guest', '_crud.insert_on_storage', space, tuple, opts)
end

function M.update_raw(space_name, key, operations)
    box.space[space_name]:update(key, operations)
end

function M.update_crud(space, key, operations, field_names, opts)
    call.storage_api.call_on_storage('guest', '_crud.update_on_storage', space, key, operations, field_names, opts)
end

function M.upsert_raw(space_name, tuple, operations)
    box.space[space_name]:upsert(tuple, operations)
end

function M.upsert_crud(space, tuple, operations, opts)
    call.storage_api.call_on_storage('guest', '_crud.upsert_on_storage', space, tuple, operations, opts)
end

function M.select_raw(space_name, index_name, key, opts)
    box.space[space_name].index[index_name]:select(key, opts)
end

function M.select_crud(space_name, index_id, conditions, opts)
    call.storage_api.call_on_storage('guest', '_crud.select_on_storage', space_name, index_id, conditions, opts)
end

return M
