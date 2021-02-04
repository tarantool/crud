local errors = require('errors')
local ffi = require('ffi')
local vshard = require('vshard')

local schema = require('crud.common.schema')
local dev_checks = require('crud.common.dev_checks')

local FlattenError = errors.new_class("FlattenError", {capture_stack = false})
local UnflattenError = errors.new_class("UnflattenError", {capture_stack = false})
local ParseOperationsError = errors.new_class('ParseOperationsError',  {capture_stack = false})
local ShardingError = errors.new_class('ShardingError',  {capture_stack = false})
local GetSpaceFormatError = errors.new_class('GetSpaceFormatError',  {capture_stack = false})

local utils = {}

function utils.table_count(table)
    dev_checks("table")

    local cnt = 0
    for _, _ in pairs(table) do
        cnt = cnt + 1
    end

    return cnt
end

function utils.format_replicaset_error(replicaset_uuid, msg, ...)
    dev_checks("string", "string")

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

function utils.get_space_format(space_name, replicasets)
    local space = utils.get_space(space_name, replicasets)
    if space == nil then
        return nil, GetSpaceFormatError:new("Space %q doesn't exist", space_name)
    end

    local space_format = space:format()

    return space_format
end

local system_fields = { bucket_id = true }

function utils.flatten(object, space_format, bucket_id)
    if object == nil then return nil end

    local tuple = {}

    local fieldnames = {}

    for fieldno, field_format in ipairs(space_format) do
        local fieldname = field_format.name
        local value = object[fieldname]

        if not system_fields[fieldname] then
            if not field_format.is_nullable and value == nil then
                return nil, FlattenError:new("Field %q isn't nullable", fieldname)
            end
        end

        if bucket_id ~= nil and fieldname == 'bucket_id' then
            value = bucket_id
        end

        tuple[fieldno] = value
        fieldnames[fieldname] = true
    end

    for fieldname in pairs(object) do
        if not fieldnames[fieldname] then
            return nil, FlattenError:new("Unknown field %q is specified", fieldname)
        end
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

local enabled_tarantool_features = {}

local function determine_enabled_features()
    local major_minor_patch = _G._TARANTOOL:split('-', 1)[1]
    local major_minor_patch_parts = major_minor_patch:split('.', 2)

    local major = tonumber(major_minor_patch_parts[1])
    local minor = tonumber(major_minor_patch_parts[2])
    local patch = tonumber(major_minor_patch_parts[3])

    -- since Tarantool 2.3
    enabled_tarantool_features.fieldpaths = major >= 2 and (minor > 3 or minor == 3 and patch >= 1)

    -- since Tarantool 2.4
    enabled_tarantool_features.uuids = major >= 2 and (minor > 4 or minor == 4 and patch >= 1)
end

function utils.tarantool_supports_fieldpaths()
    if enabled_tarantool_features.fieldpaths == nil then
        determine_enabled_features()
    end

    return enabled_tarantool_features.fieldpaths
end

function utils.tarantool_supports_uuids()
    if enabled_tarantool_features.uuids == nil then
        determine_enabled_features()
    end

    return enabled_tarantool_features.uuids
end

function utils.convert_operations(user_operations, space_format)
    if utils.tarantool_supports_fieldpaths() then
        return user_operations
    end

    local converted_operations = {}

    for _, operation in ipairs(user_operations) do
        if type(operation[2]) == 'string' then
            local field_id
            for fieldno, field_format in ipairs(space_format) do
                if field_format.name == operation[2] then
                    field_id = fieldno
                    break
                end
            end

            if field_id == nil then
                return nil, ParseOperationsError:new(
                        "Space format doesn't contain field named %q", operation[2])
            end

            table.insert(converted_operations, {
                operation[1], field_id, operation[3]
            })
        else
            table.insert(converted_operations, operation)
        end
    end

    return converted_operations
end

function utils.unflatten_rows(rows, metadata)
    if metadata == nil then
        return nil, UnflattenError:new('Metadata is not provided')
    end

    local result = table.new(#rows, 0)
    local err
    for i, row in ipairs(rows) do
        result[i], err = utils.unflatten(row, metadata)
        if err ~= nil then
            return nil, err
        end
    end
    return result
end

local inverted_tarantool_iters = {
    [box.index.EQ] = box.index.REQ,
    [box.index.GT] = box.index.LT,
    [box.index.GE] = box.index.LE,
    [box.index.LT] = box.index.GT,
    [box.index.LE] = box.index.GE,
    [box.index.REQ] = box.index.EQ,
}

function utils.invert_tarantool_iter(iter)
    local inverted_iter = inverted_tarantool_iters[iter]
    assert(inverted_iter ~= nil, "Unsupported Tarantool iterator: " .. tostring(iter))
    return inverted_iter
end

function utils.reverse_inplace(t)
    for i = 1,#t - 1 do
        t[i], t[#t - i + 1] = t[#t - i + 1], t[i]
    end
    return t
end

function utils.get_bucket_id_fieldno(space, shard_index_name)
    shard_index_name = shard_index_name or 'bucket_id'
    local bucket_id_index = space.index[shard_index_name]
    if bucket_id_index == nil then
        return nil, ShardingError:new('%q index is not found', shard_index_name)
    end

    return bucket_id_index.parts[1].fieldno
end

local uuid_t = ffi.typeof('struct tt_uuid')
function utils.is_uuid(value)
    return ffi.istype(uuid_t, value)
end

function utils.format_result(rows, space)
    return {
        metadata = table.copy(space:format()),
        rows = rows,
    }
end

local function flatten_obj(space_name, obj)
    local space_format, err = utils.get_space_format(space_name, vshard.router.routeall())
    if err ~= nil then
        return nil, FlattenError:new("Failed to get space format: %s", err), true
    end

    local tuple, err = utils.flatten(obj, space_format)
    if err ~= nil then
        return nil, FlattenError:new("Object is specified in bad format: %s", err), true
    end

    return tuple
end

function utils.flatten_obj_reload(space_name, obj)
    return schema.wrap_func_reload(flatten_obj, space_name, obj)
end

return utils
