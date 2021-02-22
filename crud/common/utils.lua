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
local FilterFieldsError = errors.new_class('FilterFieldsError',  {capture_stack = false})

local utils = {}

local space_format_cache = setmetatable({}, {__mode = 'k'})

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

local function append(lines, s, ...)
    table.insert(lines, string.format(s, ...))
end

local flatten_functions_cache = setmetatable({}, {__mode = 'k'})

function utils.flatten(object, space_format, bucket_id)
    local flatten_func = flatten_functions_cache[space_format]
    if flatten_func ~= nil then
        local data, err = flatten_func(object, bucket_id)
        if err ~= nil then
            return nil, FlattenError:new(err)
        end
        return data
    end

    local lines = {}
    append(lines, 'local object, bucket_id = ...')

    append(lines, 'for k in pairs(object) do')
    append(lines, '    if fieldmap[k] == nil then')
    append(lines, '        return nil, format(\'Unknown field %%q is specified\', k)')
    append(lines, '    end')
    append(lines, 'end')

    local len = #space_format
    append(lines, 'local result = {%s}', string.rep('NULL,', len))

    local fieldmap = {}

    for i, field in ipairs(space_format) do
        fieldmap[field.name] = true
        if field.name ~= 'bucket_id' then
            append(lines, 'if object[%q] ~= nil then', field.name)
            append(lines, '    result[%d] = object[%q]', i, field.name)
            if field.is_nullable ~= true then
                append(lines, 'else')
                append(lines, '    return nil, \'Field %q isn\\\'t nullable\'', field.name)
            end
            append(lines, 'end')
        else
            append(lines, 'if bucket_id ~= nil then')
            append(lines, '    result[%d] = bucket_id', i, field.name)
            append(lines, 'else')
            append(lines, '    result[%d] = object[%q]', i, field.name)
            append(lines, 'end')
        end
    end
    append(lines, 'return result')

    local code = table.concat(lines, '\n')
    local env = {
        pairs = pairs,
        format = string.format,
        fieldmap = fieldmap,
        NULL = box.NULL,
    }
    flatten_func = assert(load(code, '@flatten', 't', env))

    flatten_functions_cache[space_format] = flatten_func
    local data, err = flatten_func(object, bucket_id)
    if err ~= nil then
        return nil, FlattenError:new(err)
    end
    return data
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

local function id_not_in_table(operations, id)
    for _, operation in ipairs(operations) do
        if operation[2] == id then
            return false
        end
    end

    return true
end

local function add_nullable_fields_rec(operations, space_format, tuple, id)
    if id < 2 or tuple[id - 1] ~= box.NULL then
        return operations
    end

    if space_format[id - 1].is_nullable and id_not_in_table(operations, id - 1) then
        table.insert(operations, {'=', id - 1, box.NULL})
        return add_nullable_fields_rec(operations, space_format, tuple, id - 1)
    end

    return operations
end

-- Tarantool < 2 has no fields `box.error.NO_SUCH_FIELD_NO` and `box.error.NO_SUCH_FIELD_NAME`.
function utils.is_field_not_found(err_code)
    local patch_parts = _G._TARANTOOL:split('-', 1)[1]:split('.', 2)
    local major = tonumber(patch_parts[1])

    if major >= 2 then
        if err_code == box.error.NO_SUCH_FIELD_NO or err_code == box.error.NO_SUCH_FIELD_NAME then
            return true
        end
    else
        if err_code == box.error.NO_SUCH_FIELD then
            return true
        end
    end

    return false
end

function utils.add_intermediate_nullable_fields(operations, space_format, tuple)
    if tuple == nil then
        return operations
    end

    local formatted_operations, err = utils.convert_operations(operations, space_format)
    if err ~= nil then
        return operations
    end

    for i = 1, #formatted_operations do
        formatted_operations = add_nullable_fields_rec(
            formatted_operations,
            space_format,
            tuple,
            formatted_operations[i][2]
        )
    end

    table.sort(formatted_operations, function(v1, v2) return v1[2] < v2[2] end)
    return formatted_operations
end

function utils.convert_operations(user_operations, space_format)
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

local function get_field_format(space_format, field_name)
    dev_checks('table', 'string')

    local metadata = space_format_cache[space_format]
    if metadata ~= nil then
        return metadata[field_name]
    end

    space_format_cache[space_format] = {}
    for _, field in ipairs(space_format) do
        space_format_cache[space_format][field.name] = field
    end

    return space_format_cache[space_format][field_name]
end

local function filter_format_fields(space_format, field_names)
    dev_checks('table', 'table')

    local filtered_space_format = {}

    for i, field_name in ipairs(field_names) do
        filtered_space_format[i] = get_field_format(space_format, field_name)
        if filtered_space_format[i] == nil then
            return nil, FilterFieldsError:new(
                    'Space format doesn\'t contain field named %q', field_name
            )
        end
    end

    return filtered_space_format
end

function utils.format_result(rows, space, field_names)
    local result = {}
    local err
    local space_format = space:format()
    result.rows = rows

    if field_names == nil then
        result.metadata = table.copy(space_format)
        return result
    end

    result.metadata, err = filter_format_fields(space_format, field_names)

    if err ~= nil then
        return nil, err
    end

    return result
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
