local checks = require('checks')
local errors = require('errors')

local FlattenError = errors.new_class("FlattenError", {capture_stack = false})
local UnflattenError = errors.new_class("UnflattenError", {capture_stack = false})

local utils = {}

function utils.table_count(table)
    checks("table")

    local cnt = 0
    for _, _ in pairs(table) do
        cnt = cnt + 1
    end

    return cnt
end

function utils.format_replicaset_error(replicaset_uuid, msg, ...)
    checks("string", "string")

    return string.format(
        "Failed for %s: %s",
        replicaset_uuid,
        string.format(msg, ...)
    )
end

function utils.get_space(space_name, replicasets)
    local replicaset = select(2, next(replicasets))
    local space = replicaset.master.conn.space[space_name]
    return space
end

local system_fields = { bucket_id = true }

function utils.flatten(object, space_format)
    if object == nil then return nil end

    local tuple = {}

    for fieldno, field_format in ipairs(space_format) do
        local value = object[field_format.name]

        if not system_fields[field_format.name] then
            if not field_format.is_nullable and value == nil then
                return nil, FlattenError:new("Field %q isn't nullable", field_format.name)
            end
        end

        tuple[fieldno] = value
    end

    return tuple
end

function utils.unflatten(tuple, space_format)
    if tuple == nil then return nil end

    local object = {}

    for fieldno, field_format in ipairs(space_format) do
        local value = tuple[fieldno]

        if not field_format.is_nullable and value == nil then
            return nil, UnflattenError:new("Field %s isn't nullable", fieldno)
        end

        object[field_format.name] = value
    end

    return object
end

function utils.extract_key(tuple, key_parts)
    local key = {}
    for i, part in ipairs(key_parts) do
        key[i] = tuple[part.fieldno]
    end
    return key
end

function utils.merge_primary_key_parts(key_parts, pk_parts)
    local merged_parts = {}
    local key_fieldnos = {}

    for _, part in ipairs(key_parts) do
        table.insert(merged_parts, part)
        key_fieldnos[part.fieldno] = true
    end

    for _, pk_part in ipairs(pk_parts) do
        if not key_fieldnos[pk_part.fieldno] then
            table.insert(merged_parts, pk_part)
        end
    end

    return merged_parts
end

return utils
