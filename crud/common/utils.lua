local bit = require('bit')
local errors = require('errors')
local fiber = require('fiber')
local ffi = require('ffi')
local fun = require('fun')
local vshard = require('vshard')
local log = require('log')
local tarantool = require('tarantool')

local is_cartridge, cartridge = pcall(require, 'cartridge')
local is_cartridge_hotreload, cartridge_hotreload = pcall(require, 'cartridge.hotreload')

local const = require('crud.common.const')
local schema = require('crud.common.schema')
local dev_checks = require('crud.common.dev_checks')

local FlattenError = errors.new_class("FlattenError", {capture_stack = false})
local UnflattenError = errors.new_class("UnflattenError", {capture_stack = false})
local ParseOperationsError = errors.new_class('ParseOperationsError', {capture_stack = false})
local ShardingError = errors.new_class('ShardingError', {capture_stack = false})
local GetSpaceError = errors.new_class('GetSpaceError')
local GetSpaceFormatError = errors.new_class('GetSpaceFormatError', {capture_stack = false})
local FilterFieldsError = errors.new_class('FilterFieldsError', {capture_stack = false})
local NotInitializedError = errors.new_class('NotInitialized')
local VshardRouterError = errors.new_class('VshardRouterError', {capture_stack = false})
local UtilsInternalError = errors.new_class('UtilsInternalError', {capture_stack = false})

local utils = {}

utils.STORAGE_NAMESPACE = '_crud'

--- Returns a full call string for a storage function name.
--
--  @param string name a base name of the storage function.
--
--  @return a full string for the call.
function utils.get_storage_call(name)
    dev_checks('string')

    return ('%s.%s'):format(utils.STORAGE_NAMESPACE, name)
end

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

function utils.format_replicaset_error(replicaset_id, msg, ...)
    dev_checks("string", "string")

    return string.format(
        "Failed for %s: %s",
        replicaset_id,
        string.format(msg, ...)
    )
end

local function get_replicaset_by_replica_id(replicasets, id)
    for replicaset_id, replicaset in pairs(replicasets) do
        for replica_id, _ in pairs(replicaset.replicas) do
            if replica_id == id then
                return replicaset_id, replicaset
            end
        end
    end

    return nil, nil
end

function utils.get_spaces(vshard_router, timeout, replica_id)
    local replicasets, replicaset, replicaset_id, master

    timeout = timeout or const.DEFAULT_VSHARD_CALL_TIMEOUT
    local deadline = fiber.clock() + timeout
    local iter_sleep = math.min(timeout / 100, 0.1)
    while (
        -- Break if the deadline condition is exceeded.
        -- Handling for deadline errors are below in the code.
        fiber.clock() < deadline
    ) do
        -- Try to get master with timeout.
        replicasets = vshard_router:routeall()
        if replica_id ~= nil then
            -- Get the same replica on which the last DML operation was performed.
            -- This approach is temporary and is related to [1], [2].
            -- [1] https://github.com/tarantool/crud/issues/236
            -- [2] https://github.com/tarantool/crud/issues/361
            replicaset_id, replicaset = get_replicaset_by_replica_id(replicasets, replica_id)
            break
        else
            replicaset_id, replicaset = next(replicasets)
        end

        if replicaset ~= nil then
            -- Get cached, reload (if required) will be processed in other place.
            master = utils.get_replicaset_master(replicaset, {cached = true})
            if master ~= nil and master.conn.error == nil then
                break
            end
        end

        fiber.sleep(iter_sleep)
    end

    if replicaset == nil then
        return nil, GetSpaceError:new(
            'The router returned empty replicasets: ' ..
            'perhaps other instances are unavailable or you have configured only the router')
    end

    master = utils.get_replicaset_master(replicaset, {cached = true})

    if master == nil then
        local error_msg = string.format(
            'The master was not found in replicaset %s, ' ..
            'check status of the master and repeat the operation later',
             replicaset_id)
        return nil, GetSpaceError:new(error_msg)
    end

    if master.conn.error ~= nil then
        local error_msg = string.format(
            'The connection to the master of replicaset %s is not valid: %s',
             replicaset_id, master.conn.error)
        return nil, GetSpaceError:new(error_msg)
    end

    return master.conn.space, nil, master.conn.schema_version
end

function utils.get_space(space_name, vshard_router, timeout, replica_id)
    local spaces, err, schema_version = utils.get_spaces(vshard_router, timeout, replica_id)

    if spaces == nil then
        return nil, err
    end

    return spaces[space_name], err, schema_version
end

function utils.get_space_format(space_name, vshard_router)
    local space, err = utils.get_space(space_name, vshard_router)
    if err ~= nil then
        return nil, GetSpaceFormatError:new("An error occurred during the operation: %s", err)
    end
    if space == nil then
        return nil, GetSpaceFormatError:new("Space %q doesn't exist", space_name)
    end

    local space_format = space:format()

    return space_format
end

function utils.fetch_latest_metadata_when_single_storage(space, space_name, netbox_schema_version,
                                                         vshard_router, opts, storage_info)
    -- Checking the relevance of the schema version is necessary
    -- to prevent the irrelevant metadata of the DML operation.
    -- This approach is temporary and is related to [1], [2].
    -- [1] https://github.com/tarantool/crud/issues/236
    -- [2] https://github.com/tarantool/crud/issues/361
    local latest_space, err

    assert(storage_info.replica_schema_version ~= nil,
           'check the replica_schema_version value from storage ' ..
           'for correct use of the fetch_latest_metadata opt')

    local replica_id
    if storage_info.replica_id == nil then -- Backward compatibility.
        assert(storage_info.replica_uuid ~= nil,
               'check the replica_uuid value from storage ' ..
               'for correct use of the fetch_latest_metadata opt')
        replica_id = storage_info.replica_uuid
    else
        replica_id = storage_info.replica_id
    end

    assert(netbox_schema_version ~= nil,
           'check the netbox_schema_version value from net_box conn on router ' ..
           'for correct use of the fetch_latest_metadata opt')

    if storage_info.replica_schema_version ~= netbox_schema_version then
        local ok, reload_schema_err = schema.reload_schema(vshard_router)
        if ok then
            latest_space, err = utils.get_space(space_name, vshard_router,
                                                opts.timeout, replica_id)
            if err ~= nil then
                local warn_msg = "Failed to fetch space for latest schema actualization, metadata may be outdated: %s"
                log.warn(warn_msg, err)
            end
            if latest_space == nil then
                log.warn("Failed to find space for latest schema actualization, metadata may be outdated")
            end
        else
            log.warn("Failed to reload schema, metadata may be outdated: %s", reload_schema_err)
        end
    end
    if err == nil and latest_space ~= nil then
        space = latest_space
    end

    return space
end

function utils.fetch_latest_metadata_when_map_storages(space, space_name, vshard_router, opts,
                                                       storages_info, netbox_schema_version)
    -- Checking the relevance of the schema version is necessary
    -- to prevent the irrelevant metadata of the DML operation.
    -- This approach is temporary and is related to [1], [2].
    -- [1] https://github.com/tarantool/crud/issues/236
    -- [2] https://github.com/tarantool/crud/issues/361
    local latest_space, err
    for _, storage_info in pairs(storages_info) do
        assert(storage_info.replica_schema_version ~= nil,
            'check the replica_schema_version value from storage ' ..
            'for correct use of the fetch_latest_metadata opt')
        assert(netbox_schema_version ~= nil,
               'check the netbox_schema_version value from net_box conn on router ' ..
               'for correct use of the fetch_latest_metadata opt')
        if storage_info.replica_schema_version ~= netbox_schema_version then
            local ok, reload_schema_err = schema.reload_schema(vshard_router)
            if ok then
                latest_space, err = utils.get_space(space_name, vshard_router, opts.timeout)
                if err ~= nil then
                    local warn_msg = "Failed to fetch space for latest schema actualization, " ..
                                     "metadata may be outdated: %s"
                    log.warn(warn_msg, err)
                end
                if latest_space == nil then
                    log.warn("Failed to find space for latest schema actualization, metadata may be outdated")
                end
            else
                log.warn("Failed to reload schema, metadata may be outdated: %s", reload_schema_err)
            end
            if err == nil and latest_space ~= nil then
                space = latest_space
            end
            break
        end
    end

    return space
end

function utils.fetch_latest_metadata_for_select(space_name, vshard_router, opts,
                                                storages_info, iter)
    -- Checking the relevance of the schema version is necessary
    -- to prevent the irrelevant metadata of the DML operation.
    -- This approach is temporary and is related to [1], [2].
    -- [1] https://github.com/tarantool/crud/issues/236
    -- [2] https://github.com/tarantool/crud/issues/361
    for _, storage_info in pairs(storages_info) do
        assert(storage_info.replica_schema_version ~= nil,
               'check the replica_schema_version value from storage ' ..
               'for correct use of the fetch_latest_metadata opt')
        assert(iter.netbox_schema_version ~= nil,
               'check the netbox_schema_version value from net_box conn on router ' ..
               'for correct use of the fetch_latest_metadata opt')
        if storage_info.replica_schema_version ~= iter.netbox_schema_version then
            local ok, reload_schema_err = schema.reload_schema(vshard_router)
            if ok then
                local err
                iter.space, err = utils.get_space(space_name, vshard_router, opts.timeout)
                if err ~= nil then
                    local warn_msg = "Failed to fetch space for latest schema actualization, " ..
                                     "metadata may be outdated: %s"
                    log.warn(warn_msg, err)
                end
            else
                log.warn("Failed to reload schema, metadata may be outdated: %s", reload_schema_err)
            end
            break
        end
    end

    return iter
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
    flatten_func = assert(load(code, nil, 't', env))

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


local function get_version_suffix(suffix_candidate)
    if type(suffix_candidate) ~= 'string' then
        return nil
    end

    if suffix_candidate:find('^entrypoint$')
    or suffix_candidate:find('^alpha%d$')
    or suffix_candidate:find('^beta%d$')
    or suffix_candidate:find('^rc%d$') then
        return suffix_candidate
    end

    return nil
end

local function get_commits_since_from_version_part(commits_since_candidate)
    if commits_since_candidate == nil then
        return 0
    end

    local ok, val = pcall(tonumber, commits_since_candidate)
    if ok then
        return val
    else
        -- It may be unknown suffix instead.
        -- Since suffix already unknown, there is no way to properly compare versions.
        return 0
    end
end

local function get_commits_since(suffix, commits_since_candidate_1, commits_since_candidate_2)
    -- x.x.x.-candidate_1-candidate_2

    if suffix ~= nil then
        -- X.Y.Z-suffix-N
        return get_commits_since_from_version_part(commits_since_candidate_2)
    else
        -- X.Y.Z-N
        -- Possibly X.Y.Z-suffix-N with unknown suffix
        return get_commits_since_from_version_part(commits_since_candidate_1)
    end
end

utils.get_version_suffix = get_version_suffix


local suffix_with_digit_weight = {
    alpha = -3000,
    beta  = -2000,
    rc    = -1000,
}

local function get_version_suffix_weight(suffix)
    if suffix == nil then
        return 0
    end

    if suffix:find('^entrypoint$') then
        return -math.huge
    end

    for header, weight in pairs(suffix_with_digit_weight) do
        local pos, _, digits = suffix:find('^' .. header .. '(%d)$')
        if pos ~= nil then
            return weight + tonumber(digits)
        end
    end

    UtilsInternalError:assert(false,
        'Unexpected suffix %q, parse with "utils.get_version_suffix" first', suffix)
end

utils.get_version_suffix_weight = get_version_suffix_weight


local function is_version_ge(major, minor,
                             patch, suffix, commits_since,
                             major_to_compare, minor_to_compare,
                             patch_to_compare, suffix_to_compare, commits_since_to_compare)
    major = major or 0
    minor = minor or 0
    patch = patch or 0
    local suffix_weight = get_version_suffix_weight(suffix)
    commits_since = commits_since or 0

    major_to_compare = major_to_compare or 0
    minor_to_compare = minor_to_compare or 0
    patch_to_compare = patch_to_compare or 0
    local suffix_weight_to_compare = get_version_suffix_weight(suffix_to_compare)
    commits_since_to_compare = commits_since_to_compare or 0

    if major > major_to_compare then return true end
    if major < major_to_compare then return false end

    if minor > minor_to_compare then return true end
    if minor < minor_to_compare then return false end

    if patch > patch_to_compare then return true end
    if patch < patch_to_compare then return false end

    if suffix_weight > suffix_weight_to_compare then return true end
    if suffix_weight < suffix_weight_to_compare then return false end

    if commits_since > commits_since_to_compare then return true end
    if commits_since < commits_since_to_compare then return false end

    return true
end

utils.is_version_ge = is_version_ge


local function is_version_in_range(major, minor,
                                   patch, suffix, commits_since,
                                   major_left_side, minor_left_side,
                                   patch_left_side, suffix_left_side, commits_since_left_side,
                                   major_right_side, minor_right_side,
                                   patch_right_side, suffix_right_side, commits_since_right_side)
    return is_version_ge(major, minor,
                         patch, suffix, commits_since,
                         major_left_side, minor_left_side,
                         patch_left_side, suffix_left_side, commits_since_left_side)
       and is_version_ge(major_right_side, minor_right_side,
                         patch_right_side, suffix_right_side, commits_since_right_side,
                         major, minor,
                         patch, suffix, commits_since)
end

utils.is_version_in_range = is_version_in_range


local function get_tarantool_version()
    local version_parts = rawget(_G, '_TARANTOOL'):split('-', 3)

    local major_minor_patch_parts = version_parts[1]:split('.', 2)
    local major = tonumber(major_minor_patch_parts[1])
    local minor = tonumber(major_minor_patch_parts[2])
    local patch = tonumber(major_minor_patch_parts[3])

    local suffix = get_version_suffix(version_parts[2])

    local commits_since = get_commits_since(suffix, version_parts[2], version_parts[3])

    return major, minor, patch, suffix, commits_since
end

utils.get_tarantool_version = get_tarantool_version


local function tarantool_version_at_least(wanted_major, wanted_minor,
                                          wanted_patch, wanted_suffix, wanted_commits_since)
    local major, minor, patch, suffix, commits_since = get_tarantool_version()

    return is_version_ge(major, minor, patch, suffix, commits_since,
                         wanted_major, wanted_minor, wanted_patch, wanted_suffix, wanted_commits_since)
end

utils.tarantool_version_at_least = tarantool_version_at_least

function utils.is_enterprise_package()
    return tarantool.package == 'Tarantool Enterprise'
end


local enabled_tarantool_features = {}

local function determine_enabled_features()
    local major, minor, patch, suffix, commits_since = get_tarantool_version()

    -- since Tarantool 2.3.1
    enabled_tarantool_features.fieldpaths = is_version_ge(major, minor, patch, suffix, commits_since,
                                                          2, 3, 1, nil, nil)

    -- Full support (Lua type, space format type and indexes) for decimal type
    -- is since Tarantool 2.3.1 [1]
    --
    -- [1] https://github.com/tarantool/tarantool/commit/485439e33196e26d120e622175f88b4edc7a5aa1
    enabled_tarantool_features.decimals = is_version_ge(major, minor, patch, suffix, commits_since,
                                                        2, 3, 1, nil, nil)

    -- Full support (Lua type, space format type and indexes) for uuid type
    -- is since Tarantool 2.4.1 [1]
    --
    -- [1] https://github.com/tarantool/tarantool/commit/b238def8065d20070dcdc50b54c2536f1de4c7c7
    enabled_tarantool_features.uuids = is_version_ge(major, minor, patch, suffix, commits_since,
                                                     2, 4, 1, nil, nil)

    -- Full support (Lua type, space format type and indexes) for datetime type
    -- is since Tarantool 2.10.0-beta2 [1]
    --
    -- [1] https://github.com/tarantool/tarantool/commit/3bd870261c462416c29226414fe0a2d79aba0c74
    enabled_tarantool_features.datetimes = is_version_ge(major, minor, patch, suffix, commits_since,
                                                         2, 10, 0, 'beta2', nil)

    -- Full support (Lua type, space format type and indexes) for datetime type
    -- is since Tarantool 2.10.0-rc1 [1]
    --
    -- [1] https://github.com/tarantool/tarantool/commit/38f0c904af4882756c6dc802f1895117d3deae6a
    enabled_tarantool_features.intervals = is_version_ge(major, minor, patch, suffix, commits_since,
                                                         2, 10, 0, 'rc1', nil)

    -- since Tarantool 2.6.3 / 2.7.2 / 2.8.1
    enabled_tarantool_features.jsonpath_indexes = is_version_ge(major, minor, patch, suffix, commits_since,
                                                                2, 8, 1, nil, nil)
                                               or is_version_in_range(major, minor, patch, suffix, commits_since,
                                                                      2, 7, 2, nil, nil,
                                                                      2, 7, math.huge, nil, nil)
                                               or is_version_in_range(major, minor, patch, suffix, commits_since,
                                                                      2, 6, 3, nil, nil,
                                                                      2, 6, math.huge, nil, nil)

    -- The merger module was implemented in 2.2.1, see [1].
    -- However it had the critical problem [2], which leads to
    -- segfault at attempt to use the module from a fiber serving
    -- iproto request. So we don't use it in versions before the
    -- fix.
    --
    -- [1]: https://github.com/tarantool/tarantool/issues/3276
    -- [2]: https://github.com/tarantool/tarantool/issues/4954
    enabled_tarantool_features.builtin_merger = is_version_ge(major, minor, patch, suffix, commits_since,
                                                              2, 6, 0, nil, nil)
                                             or is_version_in_range(major, minor, patch, suffix, commits_since,
                                                                    2, 5, 1, nil, nil,
                                                                    2, 5, math.huge, nil, nil)
                                             or is_version_in_range(major, minor, patch, suffix, commits_since,
                                                                    2, 4, 2, nil, nil,
                                                                    2, 4, math.huge, nil, nil)
                                             or is_version_in_range(major, minor, patch, suffix, commits_since,
                                                                    2, 3, 3, nil, nil,
                                                                    2, 3, math.huge, nil, nil)

    -- The external merger module leans on a set of relatively
    -- new APIs in tarantool. So it works only on tarantool
    -- versions, which offer those APIs.
    --
    -- See README of the module:
    -- https://github.com/tarantool/tuple-merger
    enabled_tarantool_features.external_merger = is_version_ge(major, minor, patch, suffix, commits_since,
                                                               2, 7, 0, nil, nil)
                                              or is_version_in_range(major, minor, patch, suffix, commits_since,
                                                                     2, 6, 1, nil, nil,
                                                                     2, 6, math.huge, nil, nil)
                                              or is_version_in_range(major, minor, patch, suffix, commits_since,
                                                                     2, 5, 2, nil, nil,
                                                                     2, 5, math.huge, nil, nil)
                                              or is_version_in_range(major, minor, patch, suffix, commits_since,
                                                                     2, 4, 3, nil, nil,
                                                                     2, 4, math.huge, nil, nil)
                                              or is_version_in_range(major, minor, patch, suffix, commits_since,
                                                                     1, 10, 8, nil, nil,
                                                                     1, 10, math.huge, nil, nil)

    enabled_tarantool_features.netbox_skip_header_option = is_version_ge(major, minor, patch, suffix, commits_since,
                                                                         2, 2, 0, nil, nil)

    -- https://github.com/tarantool/tarantool/commit/11f2d999a92e45ee41b8c8d0014d8a09290fef7b
    enabled_tarantool_features.box_watch = is_version_ge(major, minor, patch, suffix, commits_since,
                                                         2, 10, 0, 'beta2', nil)

    enabled_tarantool_features.tarantool_3 = is_version_ge(major, minor, patch, suffix, commits_since,
                                                           3, 0, 0, nil, nil)

    enabled_tarantool_features.config_get_inside_roles = (
        -- https://github.com/tarantool/tarantool/commit/ebb170cb8cf2b9c4634bcf0178665909f578c335
        not utils.is_enterprise_package()
        and is_version_ge(major, minor, patch, suffix, commits_since,
                          3, 1, 0, 'entrypoint', 77)
    ) or (
        -- https://github.com/tarantool/tarantool/commit/e0e1358cb60d6749c34daf508e05586e0959bf89
        not utils.is_enterprise_package()
        and is_version_in_range(major, minor, patch, suffix, commits_since,
                                3, 0, 1, nil, 10,
                                3, 0, math.huge, nil, nil)
    ) or (
        -- https://github.com/tarantool/tarantool-ee/commit/368cc4007727af30ae3ca3a3cdfc7065f34e02aa
        utils.is_enterprise_package()
        and is_version_ge(major, minor, patch, suffix, commits_since,
                          3, 1, 0, 'entrypoint', 44)
    ) or (
        -- https://github.com/tarantool/tarantool-ee/commit/1dea81bed4cbe4856a0fc77dcc548849a2dabf45
        utils.is_enterprise_package()
        and is_version_in_range(major, minor, patch, suffix, commits_since,
                                3, 0, 1, nil, 10,
                                3, 0, math.huge, nil, nil)
    )

    enabled_tarantool_features.role_privileges_not_revoked = (
        -- https://github.com/tarantool/tarantool/commit/b982b46442e62e05ab6340343233aa766ad5e52c
        not utils.is_enterprise_package()
        and is_version_ge(major, minor, patch, suffix, commits_since,
                          3, 1, 0, 'entrypoint', 179)
    ) or (
        -- https://github.com/tarantool/tarantool/commit/ee2faf7c328abc54631233342cb9b88e4ce8cae4
        not utils.is_enterprise_package()
        and is_version_in_range(major, minor, patch, suffix, commits_since,
                                3, 0, 1, nil, 57,
                                3, 0, math.huge, nil, nil)
    ) or (
        -- https://github.com/tarantool/tarantool-ee/commit/5388e9d0f40d86226dc15bb27d85e63b0198e789
        utils.is_enterprise_package()
        and is_version_ge(major, minor, patch, suffix, commits_since,
                          3, 1, 0, 'entrypoint', 82)
    ) or (
        -- https://github.com/tarantool/tarantool-ee/commit/83d378d01bf2761da8ec684b6afe5683d38faeae
        utils.is_enterprise_package()
        and is_version_in_range(major, minor, patch, suffix, commits_since,
                                3, 0, 1, nil, 35,
                                3, 0, math.huge, nil, nil)
    )
end

determine_enabled_features()

for feature_name, feature_enabled in pairs(enabled_tarantool_features) do
    local util_name
    if feature_name == 'tarantool_3' then
        util_name = ('is_%s'):format(feature_name)
    elseif feature_name == 'builtin_merger' then
        util_name = ('tarantool_has_%s'):format(feature_name)
    elseif feature_name == 'role_privileges_not_revoked' then
        util_name = ('tarantool_%s'):format(feature_name)
    else
        util_name = ('tarantool_supports_%s'):format(feature_name)
    end

    local util_func = function() return feature_enabled end

    utils[util_name] = util_func
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
if tarantool_version_at_least(2, 1, 0, nil) then
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
    local space_format, err = utils.get_space_format(space_name, vshard_router)
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

function utils.update_storage_call_error_description(err, func_name, replicaset_id)
    if err == nil then
        return nil
    end

    if (err.type == 'ClientError' or err.type == 'AccessDeniedError')
        and type(err.message) == 'string' then
        local not_defined_str = string.format("Procedure '%s' is not defined", func_name)
        local access_denied_str = string.format("Execute access to function '%s' is denied", func_name)
        if err.message == not_defined_str or err.message:startswith(access_denied_str) then
            if func_name:startswith('_crud.') then
                err = NotInitializedError:new("Function %s is not registered: " ..
                    "crud isn't initialized on replicaset %q or crud module versions mismatch " ..
                    "between router and storage",
                    func_name, replicaset_id or "Unknown")
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

--- Check if Tarantool Cartridge hotreload supported
--  and get its implementaion.
--
-- @function is_cartridge_hotreload_supported
--
-- @return[1] true or false
-- @return[1] module table, if supported
function utils.is_cartridge_hotreload_supported()
    if not is_cartridge_hotreload then
        return false
    end

    return true, cartridge_hotreload
end

if utils.tarantool_supports_intervals() then
    -- https://github.com/tarantool/tarantool/blob/0510ffa07afd84a70c9c6f1a4c28aacd73a393d6/src/lua/datetime.lua#L175-179
    local interval_t = ffi.typeof('struct interval')

    utils.is_interval = function(o)
        return ffi.istype(interval_t, o)
    end
else
    utils.is_interval = function()
        return false
    end
end

for k, v in pairs(require('crud.common.vshard_utils')) do
    utils[k] = v
end

return utils
