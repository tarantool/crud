local checks = require('checks')
local errors = require('errors')
local vshard = require('vshard')

local dev_checks = require('crud.common.dev_checks')
local call = require('crud.common.call')
local utils = require('crud.common.utils')
local schema = require('crud.common.schema')
local has_keydef, Keydef = pcall(require, 'crud.compare.keydef')
local select_comparators = require('crud.compare.comparators')

local BorderError = errors.new_class('BorderError', {capture_stack = false})

local borders = {}

local STAT_FUNC_NAME = '_crud.get_border_on_storage'


local function get_border_on_storage(border_name, space_name, index_id, field_names)
    dev_checks('string', 'string', 'number', '?table')

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
    })
end

function borders.init()
   _G._crud.get_border_on_storage = get_border_on_storage
end

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

local function call_get_border_on_router(border_name, space_name, index_name, opts)
    checks('string', 'string', '?string|number', {
        timeout = '?number',
        fields = '?table',
    })

    opts = opts or {}

    local space = utils.get_space(space_name, vshard.router.routeall())
    if space == nil then
        return nil, BorderError:new("Space %q doesn't exist", space_name), true
    end

    local index
    if index_name == nil then
        index = space.index[0]
    else
        index = space.index[index_name]
    end

    if index == nil then
        return nil, BorderError:new("Index %q of space %q doesn't exist", index_name, space_name), true
    end

    local primary_index = space.index[0]

    local cmp_key_parts = utils.merge_primary_key_parts(index.parts, primary_index.parts)
    local field_names = utils.enrich_field_names_with_cmp_key(opts.fields, cmp_key_parts, space:format())

    local replicasets = vshard.router.routeall()
    local call_opts = {
        mode = 'read',
        replicasets = replicasets,
        timeout = opts.timeout,
    }
    local results, err = call.map(
        STAT_FUNC_NAME,
        {border_name, space_name, index.id, field_names},
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
            local need_reload = schema.result_needs_reload(space, storage_result)
            return nil, BorderError:new("Failed to get %s: %s", border_name, storage_result.err), need_reload
        end

        local tuple = storage_result.res
        if tuple ~= nil and is_closer(compare_sign, comparator, tuple, res_tuple) then
            res_tuple = tuple
        end
    end

    local result = utils.format_result({res_tuple}, space, field_names)

    if opts.fields ~= nil then
        result = utils.cut_rows(result.rows, result.metadata, opts.fields)
    end

    return result
end

local function get_border(border_name, space_name, index_name, opts)
    return schema.wrap_func_reload(
        call_get_border_on_router, border_name, space_name, index_name, opts
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
-- @return[1] result
-- @treturn[2] nil
-- @treturn[2] table Error description
function borders.max(space_name, index_id, opts)
    return get_border('max', space_name, index_id, opts)
end

return borders
