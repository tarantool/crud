local checks = require('checks')
local errors = require('errors')

local const = require('crud.common.const')
local dev_checks = require('crud.common.dev_checks')
local call = require('crud.common.call')
local utils = require('crud.common.utils')
local schema = require('crud.common.schema')
local has_keydef, Keydef = pcall(require, 'crud.compare.keydef')
local select_comparators = require('crud.compare.comparators')

local BorderError = errors.new_class('BorderError', {capture_stack = false})

local borders = {}

local STAT_FUNC_NAME = 'get_border_on_storage'
local CRUD_STAT_FUNC_NAME = utils.get_storage_call(STAT_FUNC_NAME)


local function get_border_on_storage(border_name, space_name, index_id, field_names, fetch_latest_metadata)
    dev_checks('string', 'string', 'number', '?table', '?boolean')

    assert(border_name == 'min' or border_name == 'max')

    local space = box.space[space_name]
    if space == nil then
        return nil, BorderError:new("Space %q doesn't exist", space_name)
    end

    local index = space.index[index_id]
    if index == nil then
        return nil, BorderError:new("Index %q of space doesn't exist", index_id, space_name)
    end

    local function get_index_border(index)
        return index[border_name](index)
    end

    return schema.wrap_func_result(space, get_index_border, {index}, {
        add_space_schema_hash = true,
        field_names = field_names,
        fetch_latest_metadata = fetch_latest_metadata,
    })
end

borders.storage_api = {[STAT_FUNC_NAME] = get_border_on_storage}

local is_closer

if has_keydef then
    is_closer = function (compare_sign, keydef, tuple, res_tuple)
        if res_tuple == nil then
            return true
        end

        local cmp = keydef:compare(tuple, res_tuple)

        return cmp * compare_sign > 0
    end
else
    is_closer = function (_, comparator, tuple, res_tuple)
        if res_tuple == nil then
            return true
        end
        return comparator(tuple, res_tuple)
    end
end

local function call_get_border_on_router(vshard_router, border_name, space_name, index_name, opts)
    checks('table', 'string', 'string', '?string|number', {
        timeout = '?number',
        fields = '?table',
        mode = '?string',
        vshard_router = '?string|table',
        fetch_latest_metadata = '?boolean',
    })

    local space, err, netbox_schema_version = utils.get_space(space_name, vshard_router, opts.timeout)
    if err ~= nil then
        return nil, BorderError:new("An error occurred during the operation: %s", err), const.NEED_SCHEMA_RELOAD
    end
    if space == nil then
        return nil, BorderError:new("Space %q doesn't exist", space_name), const.NEED_SCHEMA_RELOAD
    end

    local index
    if index_name == nil then
        index = space.index[0]
    else
        index = space.index[index_name]
    end

    if index == nil then
        return nil,
               BorderError:new("Index %q of space %q doesn't exist", index_name, space_name),
               const.NEED_SCHEMA_RELOAD
    end

    local primary_index = space.index[0]

    local cmp_key_parts = utils.merge_primary_key_parts(index.parts, primary_index.parts)
    local field_names = utils.enrich_field_names_with_cmp_key(opts.fields, cmp_key_parts, space:format())

    local replicasets, err = vshard_router:routeall()
    if err ~= nil then
        return nil, BorderError:new("Failed to get router replicasets: %s", err)
    end
    local call_opts = {
        mode = opts.mode or 'read',
        replicasets = replicasets,
        timeout = opts.timeout,
    }
    local results, err, storages_info = call.map(vshard_router,
        CRUD_STAT_FUNC_NAME,
        {border_name, space_name, index.id, field_names, opts.fetch_latest_metadata},
        call_opts
    )

    if err ~= nil then
        return nil, BorderError:new("Failed to get %s: %s", border_name, err)
    end

    local compare_sign = border_name == 'max' and 1 or -1
    local comparator
    if has_keydef then
        comparator = Keydef.new(space, field_names, index.id)
    else
        local tarantool_iter
        if compare_sign > 0 then
            tarantool_iter = box.index.GT
        else
            tarantool_iter = box.index.LT
        end
        local key_parts = utils.merge_primary_key_parts(index.parts, primary_index.parts)
        local cmp_operator = select_comparators.get_cmp_operator(tarantool_iter)
        comparator = select_comparators.gen_tuples_comparator(cmp_operator, key_parts, field_names, space:format())
    end

    local res_tuple = nil
    for _, storage_result in pairs(results) do
        local storage_result = storage_result[1]
        if storage_result.err ~= nil then
            local err_wrapped = BorderError:new("Failed to get %s: %s", border_name, storage_result.err)

            local need_reload = schema.result_needs_reload(space, storage_result)
            if need_reload then
                return nil, err_wrapped, const.NEED_SCHEMA_RELOAD
            end

            return nil, err_wrapped
        end

        local tuple = storage_result.res
        if tuple ~= nil and is_closer(compare_sign, comparator, tuple, res_tuple) then
            res_tuple = tuple
        end
    end

    if opts.fetch_latest_metadata == true then
        -- This option is temporary and is related to [1], [2].
        -- [1] https://github.com/tarantool/crud/issues/236
        -- [2] https://github.com/tarantool/crud/issues/361
        space = utils.fetch_latest_metadata_when_map_storages(space, space_name, vshard_router, opts,
                                                              storages_info, netbox_schema_version)
    end

    local result = utils.format_result({res_tuple}, space, field_names)

    if opts.fields ~= nil then
        result = utils.cut_rows(result.rows, result.metadata, opts.fields)
    end

    return result
end

local function get_border(border_name, space_name, index_name, opts)
    opts = opts or {}
    local vshard_router, err = utils.get_vshard_router_instance(opts.vshard_router)
    if err ~= nil then
        return nil, BorderError:new(err)
    end

    return schema.wrap_func_reload(vshard_router, call_get_border_on_router,
        border_name, space_name, index_name, opts
    )
end

--- Find the minimum value in the specified index
--
-- @function min
--
-- @param string space_name
--  A space name
--
-- @param ?string index_name
--  An index name (by default, primary index is used)
--
-- @tparam ?number opts.timeout
--  Function call timeout
--
-- @tparam ?table opts.fields
--  Field names for getting only a subset of fields
--
-- @tparam ?string|table opts.vshard_router
--  Cartridge vshard group name or vshard router instance.
--  Set this parameter if your space is not a part of the
--  default vshard cluster.
--
-- @return[1] result
-- @treturn[2] nil
-- @treturn[2] table Error description
function borders.min(space_name, index_id, opts)
    return get_border('min', space_name, index_id, opts)
end

--- Find the maximum value in the specified index
--
-- @function min
--
-- @param string space_name
--  A space name
--
-- @param ?string index_name
--  An index name (by default, primary index is used)
--
-- @tparam ?number opts.timeout
--  Function call timeout
--
-- @tparam ?table opts.fields
--  Field names for getting only a subset of fields
--
-- @tparam ?string|table opts.vshard_router
--  Cartridge vshard group name or vshard router instance.
--  Set this parameter if your space is not a part of the
--  default vshard cluster.
--
-- @return[1] result
-- @treturn[2] nil
-- @treturn[2] table Error description
function borders.max(space_name, index_id, opts)
    return get_border('max', space_name, index_id, opts)
end

return borders
