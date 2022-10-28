local errors = require('errors')
local ffi = require('ffi')
local vshard = require('vshard')
local fun = require('fun')
local bit = require('bit')
local log = require('log')

local is_cartridge, cartridge = pcall(require, 'cartridge')

local const = require('crud.common.const')
local schema = require('crud.common.schema')
local dev_checks = require('crud.common.dev_checks')

local FlattenError = errors.new_class("FlattenError", {capture_stack = false})
local UnflattenError = errors.new_class("UnflattenError", {capture_stack = false})
local ParseOperationsError = errors.new_class('ParseOperationsError', {capture_stack = false})
local ShardingError = errors.new_class('ShardingError', {capture_stack = false})
local GetSpaceFormatError = errors.new_class('GetSpaceFormatError', {capture_stack = false})
local FilterFieldsError = errors.new_class('FilterFieldsError', {capture_stack = false})
local NotInitializedError = errors.new_class('NotInitialized')
local StorageInfoError = errors.new_class('StorageInfoError')
local VshardRouterError = errors.new_class('VshardRouterError', {capture_stack = false})
local fiber_clock = require('fiber').clock

local utils = {}

local space_format_cache = setmetatable({}, {__mode = 'k'})

-- copy from LuaJIT lj_char.c
local lj_char_bits = {
    0,
    1,  1,  1,  1,  1,  1,  1,  1,  1,  3,  3,  3,  3,  3,  1,  1,
    1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,  1,
    2,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,  4,
    152,152,152,152,152,152,152,152,152,152,  4,  4,  4,  4,  4,  4,
    4,176,176,176,176,176,176,160,160,160,160,160,160,160,160,160,
    160,160,160,160,160,160,160,160,160,160,160,  4,  4,  4,  4,132,
    4,208,208,208,208,208,208,192,192,192,192,192,192,192,192,192,
    192,192,192,192,192,192,192,192,192,192,192,  4,  4,  4,  4,  1,
    128,128,128,128,128,128,128,128,128,128,128,128,128,128,128,128,
    128,128,128,128,128,128,128,128,128,128,128,128,128,128,128,128,
    128,128,128,128,128,128,128,128,128,128,128,128,128,128,128,128,
    128,128,128,128,128,128,128,128,128,128,128,128,128,128,128,128,
    128,128,128,128,128,128,128,128,128,128,128,128,128,128,128,128,
    128,128,128,128,128,128,128,128,128,128,128,128,128,128,128,128,
    128,128,128,128,128,128,128,128,128,128,128,128,128,128,128,128,
    128,128,128,128,128,128,128,128,128,128,128,128,128,128,128,128
}

local LJ_CHAR_IDENT = 0x80
local LJ_CHAR_DIGIT = 0x08

local LUA_KEYWORDS = {
    ['and'] = true,
    ['end'] = true,
    ['in'] = true,
    ['repeat'] = true,
    ['break'] = true,
    ['false'] = true,
    ['local'] = true,
    ['return'] = true,
    ['do'] = true,
    ['for'] = true,
    ['nil'] = true,
    ['then'] = true,
    ['else'] = true,
    ['function'] = true,
    ['not'] = true,
    ['true'] = true,
    ['elseif'] = true,
    ['if'] = true,
    ['or'] = true,
    ['until'] = true,
    ['while'] = true,
}

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

function utils.flatten(object, space_format, bucket_id, skip_nullability_check)
    local flatten_func = flatten_functions_cache[space_format]
    if flatten_func ~= nil then
        local data, err = flatten_func(object, bucket_id, skip_nullability_check)
        if err ~= nil then
            return nil, FlattenError:new(err)
        end
        return data
    end

    local lines = {}
    append(lines, 'local object, bucket_id, skip_nullability_check = ...')

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
                append(lines, 'elseif skip_nullability_check ~= true then')
                append(lines, '    return nil, \'Field %q isn\\\'t nullable' ..
                              ' (set skip_nullability_check_on_flatten option to true to skip check)\'',
                              field.name)
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
    local data, err = flatten_func(object, bucket_id, skip_nullability_check)
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

function utils.enrich_field_names_with_cmp_key(field_names, key_parts, space_format)
    if field_names == nil then
        return nil
    end

    local enriched_field_names = {}
    local key_field_names = {}

    for _, field_name in ipairs(field_names) do
        table.insert(enriched_field_names, field_name)
        key_field_names[field_name] = true
    end

    for _, part in ipairs(key_parts) do
        local field_name = space_format[part.fieldno].name
        if not key_field_names[field_name] then
            table.insert(enriched_field_names, field_name)
            key_field_names[field_name] = true
        end
    end

    return enriched_field_names
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

    -- since Tarantool 2.6.3 / 2.7.2 / 2.8.1
    enabled_tarantool_features.jsonpath_indexes = major >= 3 or (major >= 2 and ((minor >= 6 and patch >= 3)
        or (minor >= 7 and patch >= 2) or (minor >= 8 and patch >= 1) or minor >= 9))

    -- The merger module was implemented in 2.2.1, see [1].
    -- However it had the critical problem [2], which leads to
    -- segfault at attempt to use the module from a fiber serving
    -- iproto request. So we don't use it in versions before the
    -- fix.
    --
    -- [1]: https://github.com/tarantool/tarantool/issues/3276
    -- [2]: https://github.com/tarantool/tarantool/issues/4954
    enabled_tarantool_features.builtin_merger =
        (major == 2 and minor == 3 and patch >= 3) or
        (major == 2 and minor == 4 and patch >= 2) or
        (major == 2 and minor == 5 and patch >= 1) or
        (major >= 2 and minor >= 6) or
        (major >= 3)

    -- The external merger module leans on a set of relatively
    -- new APIs in tarantool. So it works only on tarantool
    -- versions, which offer those APIs.
    --
    -- See README of the module:
    -- https://github.com/tarantool/tuple-merger
    enabled_tarantool_features.external_merger =
        (major == 1 and minor == 10 and patch >= 8) or
        (major == 2 and minor == 4 and patch >= 3) or
        (major == 2 and minor == 5 and patch >= 2) or
        (major == 2 and minor == 6 and patch >= 1) or
        (major == 2 and minor >= 7) or
        (major >= 3)
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

function utils.tarantool_supports_jsonpath_indexes()
    if enabled_tarantool_features.jsonpath_indexes == nil then
        determine_enabled_features()
    end

    return enabled_tarantool_features.jsonpath_indexes
end

function utils.tarantool_has_builtin_merger()
    if enabled_tarantool_features.builtin_merger == nil then
        determine_enabled_features()
    end

    return enabled_tarantool_features.builtin_merger
end

function utils.tarantool_supports_external_merger()
    if enabled_tarantool_features.external_merger == nil then
        determine_enabled_features()
    end

    return enabled_tarantool_features.external_merger
end

local function add_nullable_fields_recursive(operations, operations_map, space_format, tuple, id)
    if id < 2 or tuple[id - 1] ~= box.NULL then
        return operations
    end

    if space_format[id - 1].is_nullable and not operations_map[id - 1] then
        table.insert(operations, {'=', id - 1, box.NULL})
        return add_nullable_fields_recursive(operations, operations_map, space_format, tuple, id - 1)
    end

    return operations
end

-- Tarantool < 2.1 has no fields `box.error.NO_SUCH_FIELD_NO` and `box.error.NO_SUCH_FIELD_NAME`.
if _TARANTOOL >= "2.1" then
    function utils.is_field_not_found(err_code)
        return err_code == box.error.NO_SUCH_FIELD_NO or err_code == box.error.NO_SUCH_FIELD_NAME
    end
else
    function utils.is_field_not_found(err_code)
        return err_code == box.error.NO_SUCH_FIELD
    end
end

local function get_operations_map(operations)
    local map = {}
    for _, operation in ipairs(operations) do
        map[operation[2]] = true
    end

    return map
end

function utils.add_intermediate_nullable_fields(operations, space_format, tuple)
    if tuple == nil then
        return operations
    end

    -- If tarantool doesn't supports the fieldpaths, we already
    -- have converted operations (see this function call in update.lua)
    if utils.tarantool_supports_fieldpaths() then
        local formatted_operations, err = utils.convert_operations(operations, space_format)
        if err ~= nil then
            return operations
        end

        operations = formatted_operations
    end

    -- We need this map to check if there is a field update
    -- operation with constant complexity
    local operations_map = get_operations_map(operations)
    for _, operation in ipairs(operations) do
        operations = add_nullable_fields_recursive(
            operations, operations_map,
            space_format, tuple, operation[2]
        )
    end

    table.sort(operations, function(v1, v2) return v1[2] < v2[2] end)
    return operations
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
    for i = 1,math.floor(#t / 2) do
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

-- Build a map with field number as a keys and part number
-- as a values using index parts as a source.
function utils.get_index_fieldno_map(index_parts)
    dev_checks('table')

    local fieldno_map = {}
    for i, part in ipairs(index_parts) do
        local fieldno = part.fieldno
        fieldno_map[fieldno] = i
    end

    return fieldno_map
end

-- Build a map with field names as a keys and fieldno's
-- as a values using space format as a source.
function utils.get_format_fieldno_map(space_format)
    dev_checks('table')

    local fieldno_map = {}
    for fieldno, field_format in ipairs(space_format) do
        fieldno_map[field_format.name] = fieldno
    end

    return fieldno_map
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

function utils.get_fields_format(space_format, field_names)
    dev_checks('table', '?table')

    if field_names == nil then
        return table.copy(space_format)
    end

    local filtered_space_format, err = filter_format_fields(space_format, field_names)

    if err ~= nil then
        return nil, err
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

local function truncate_tuple_metadata(tuple_metadata, field_names)
    dev_checks('?table', 'table')

    if tuple_metadata == nil then
        return nil
    end

    local truncated_metadata = {}

    if #tuple_metadata < #field_names then
        return nil, FilterFieldsError:new(
                'Field names don\'t match to tuple metadata'
        )
    end

    for i, name in ipairs(field_names) do
        if tuple_metadata[i].name ~= name then
            return nil, FilterFieldsError:new(
                    'Field names don\'t match to tuple metadata'
            )
        end

        table.insert(truncated_metadata, tuple_metadata[i])
    end

    return truncated_metadata
end

function utils.cut_objects(objs, field_names)
    dev_checks('table', 'table')

    for i, obj in ipairs(objs) do
        objs[i] = schema.filter_obj_fields(obj, field_names)
    end

    return objs
end

function utils.cut_rows(rows, metadata, field_names)
    dev_checks('table', '?table', 'table')

    local truncated_metadata, err = truncate_tuple_metadata(metadata, field_names)

    if err ~= nil then
        return nil, err
    end

    for i, row in ipairs(rows) do
        rows[i] = schema.truncate_row_trailing_fields(row, field_names)
    end

    return {
        metadata = truncated_metadata,
        rows = rows,
    }
end

local function flatten_obj(vshard_router, space_name, obj, skip_nullability_check)
    local space_format, err = utils.get_space_format(space_name, vshard_router:routeall())
    if err ~= nil then
        return nil, FlattenError:new("Failed to get space format: %s", err), const.NEED_SCHEMA_RELOAD
    end

    local tuple, err = utils.flatten(obj, space_format, nil, skip_nullability_check)
    if err ~= nil then
        return nil, FlattenError:new("Object is specified in bad format: %s", err), const.NEED_SCHEMA_RELOAD
    end

    return tuple
end

function utils.flatten_obj_reload(vshard_router, space_name, obj, skip_nullability_check)
    return schema.wrap_func_reload(vshard_router, flatten_obj, space_name, obj, skip_nullability_check)
end

-- Merge two options map.
--
-- `opts_a` and/or `opts_b` can be `nil`.
--
-- If `opts_a.foo` and `opts_b.foo` exists, prefer `opts_b.foo`.
function utils.merge_options(opts_a, opts_b)
    return fun.chain(opts_a or {}, opts_b or {}):tomap()
end

local function lj_char_isident(n)
    return bit.band(lj_char_bits[n + 2], LJ_CHAR_IDENT) == LJ_CHAR_IDENT
end

local function lj_char_isdigit(n)
    return bit.band(lj_char_bits[n + 2], LJ_CHAR_DIGIT) == LJ_CHAR_DIGIT
end

function utils.check_name_isident(name)
    dev_checks('string')

    -- sharding function name cannot
    -- be equal to lua keyword
    if LUA_KEYWORDS[name] then
        return false
    end

    -- sharding function name cannot
    -- begin with a digit
    local char_number = string.byte(name:sub(1,1))
    if lj_char_isdigit(char_number) then
        return false
    end

    -- sharding func name must be sequence
    -- of letters, digits, or underscore symbols
    for i = 1, #name do
        local char_number = string.byte(name:sub(i,i))
        if not lj_char_isident(char_number) then
            return false
        end
    end

    return true
end

function utils.update_storage_call_error_description(err, func_name, replicaset_uuid)
    if err == nil then
        return nil
    end

    if err.type == 'ClientError' and type(err.message) == 'string' then
        if err.message == string.format("Procedure '%s' is not defined", func_name) then
            if func_name:startswith('_crud.') then
                err = NotInitializedError:new("Function %s is not registered: " ..
                    "crud isn't initialized on replicaset %q or crud module versions mismatch " ..
                    "between router and storage",
                    func_name, replicaset_uuid or "Unknown")
            else
                err = NotInitializedError:new("Function %s is not registered", func_name)
            end
        end
    end
    return err
end

--- Insert each value from values to list
--
-- @function list_extend
--
-- @param table list
--  List to be extended
--
-- @param table values
--  Values to be inserted to list
--
-- @return[1] list
--  List with old values and inserted values
function utils.list_extend(list, values)
    dev_checks('table', 'table')

    for _, value in ipairs(values) do
        table.insert(list, value)
    end

    return list
end

function utils.list_slice(list, start_index, end_index)
    dev_checks('table', 'number', '?number')

    if end_index == nil then
        end_index = table.maxn(list)
    end

    local slice = {}
    for i = start_index, end_index do
        table.insert(slice, list[i])
    end

    return slice
end

--- Polls replicas for storage state
--
-- @function storage_info
--
-- @tparam ?number opts.timeout
--  Function call timeout
--
-- @tparam ?string|table opts.vshard_router
--  Cartridge vshard group name or vshard router instance.
--
-- @return a table of storage states by replica uuid.
function utils.storage_info(opts)
    opts = opts or {}

    local vshard_router, err = utils.get_vshard_router_instance(opts.vshard_router)
    if err ~= nil then
        return nil, StorageInfoError:new(err)
    end

    local replicasets, err = vshard_router:routeall()
    if replicasets == nil then
        return nil, StorageInfoError:new("Failed to get router replicasets: %s", err.err)
    end

    local futures_by_replicas = {}
    local replica_state_by_uuid = {}
    local async_opts = {is_async = true}
    local timeout = opts.timeout or const.DEFAULT_VSHARD_CALL_TIMEOUT

    for _, replicaset in pairs(replicasets) do
        for replica_uuid, replica in pairs(replicaset.replicas) do
            replica_state_by_uuid[replica_uuid] = {
                status = "error",
                is_master = replicaset.master == replica
            }
            local ok, res = pcall(replica.conn.call, replica.conn, "_crud.storage_info_on_storage",
                                  {}, async_opts)
            if ok then
                futures_by_replicas[replica_uuid] = res
            else
                local err_msg = string.format("Error getting storage info for %s", replica_uuid)
                if res ~= nil then
                    log.error("%s: %s", err_msg, res)
                    replica_state_by_uuid[replica_uuid].message = tostring(res)
                else
                    log.error(err_msg)
                    replica_state_by_uuid[replica_uuid].message = err_msg
                end
            end
        end
    end

    local deadline = fiber_clock() + timeout
    for replica_uuid, future in pairs(futures_by_replicas) do
        local wait_timeout = deadline - fiber_clock()
        if wait_timeout < 0 then
            wait_timeout = 0
        end

        local result, err = future:wait_result(wait_timeout)
        if result == nil then
            future:discard()
            local err_msg = string.format("Error getting storage info for %s", replica_uuid)
            if err ~= nil then
                if err.type == 'ClientError' and err.code == box.error.NO_SUCH_PROC then
                    replica_state_by_uuid[replica_uuid].status = "uninitialized"
                else
                    log.error("%s: %s", err_msg, err)
                    replica_state_by_uuid[replica_uuid].message = tostring(err)
                end
            else
                log.error(err_msg)
                replica_state_by_uuid[replica_uuid].message = err_msg
            end
        else
            replica_state_by_uuid[replica_uuid].status = result[1].status or "uninitialized"
        end
    end

    return replica_state_by_uuid
end

--- Storage status information.
--
-- @function storage_info_on_storage
--
-- @return a table with storage status.
function utils.storage_info_on_storage()
    return {status = "running"}
end

local expected_vshard_api = {
    'routeall', 'route', 'bucket_id_strcrc32',
    'callrw', 'callro', 'callbro', 'callre',
    'callbre', 'map_callrw'
}

--- Verifies that a table has expected vshard
--  router handles.
local function verify_vshard_router(router)
    dev_checks("table")

    for _, func_name in ipairs(expected_vshard_api) do
        if type(router[func_name]) ~= 'function' then
            return false
        end
    end

    return true
end

--- Get a vshard router instance from a parameter.
--
--  If a string passed, extract router instance from
--  Cartridge vshard groups. If table passed, verifies
--  that a table is a vshard router instance.
--
-- @function get_vshard_router_instance
--
-- @param[opt] router name of a vshard group or a vshard router
--  instance
--
-- @return[1] table vshard router instance
-- @treturn[2] nil
-- @treturn[2] table Error description
function utils.get_vshard_router_instance(router)
    dev_checks('?string|table')

    local router_instance

    if type(router) == 'string' then
        if not is_cartridge then
            return nil, VshardRouterError:new("Vshard groups are supported only in Tarantool Cartridge")
        end

        local router_service = cartridge.service_get('vshard-router')
        assert(router_service ~= nil)

        router_instance = router_service.get(router)
        if router_instance == nil then
            return nil, VshardRouterError:new("Vshard group %s is not found", router)
        end
    elseif type(router) == 'table' then
        if not verify_vshard_router(router) then
            return nil, VshardRouterError:new("Invalid opts.vshard_router table value, " ..
                                              "a vshard router instance has been expected")
        end

        router_instance = router
    else
        assert(type(router) == 'nil')
        router_instance = vshard.router.static

        if router_instance == nil then
            return nil, VshardRouterError:new("Default vshard group is not found and custom " ..
                                              "is not specified with opts.vshard_router")
        end
    end

    return router_instance
end

return utils
