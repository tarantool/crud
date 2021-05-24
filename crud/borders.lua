local checks = require('checks')
local errors = require('errors')
local vshard = require('vshard')

local dev_checks = require('crud.common.dev_checks')
local call = require('crud.common.call')
local utils = require('crud.common.utils')
local schema = require('crud.common.schema')
local Keydef = require('crud.compare.keydef')

local BorderError = errors.new_class('Border',  {capture_stack = false})

local borders = {}

local STAT_FUNC_NAME = '_crud.get_border_on_storage'


local function get_border_on_storage(border_name, space_name, index_name, field_names)
    dev_checks('string', 'string', 'string', '?table')

    assert(border_name == 'min' or border_name == 'max')

    local space = box.space[space_name]
    if space == nil then
        return nil, BorderError:new("Space %q doesn't exist", space_name)
    end

    local index = space.index[index_name]
    if index == nil then
        return nil, BorderError:new("Index %q of space doesn't exist", index_name, space_name)
    end

    local function get_border(index)
        return index[border_name](index)
    end

    return schema.wrap_func_result(space, get_border, {index}, {
        add_space_schema_hash = true,
        field_names = field_names,
    })
end

function borders.init()
   _G._crud.get_border_on_storage = get_border_on_storage
end

local function is_closer(border_name, keydef, tuple, res_tuple)
    assert(border_name == 'min' or border_name == 'max')

    if res_tuple == nil then
        return true
    end

    local cmp = keydef:compare(tuple, res_tuple)

    return border_name == 'min' and cmp < 0 or border_name == 'max' and cmp > 0
end

local function get_border(border_name, space_name, index_name, opts)
    checks('string', 'string', '?string', {
        timeout = '?number',
        fields = '?table',
    })

    opts = opts or {}

    local space = utils.get_space(space_name, vshard.router.routeall())
    if space == nil then
        return nil, BorderError:new("Space %q doesn't exist", space_name), true
    end

    if index_name == nil then
        index_name = space.index[0].name
    end
    local index = space.index[index_name]
    if index == nil then
        return nil, BorderError:new("Index %q of space doesn't exist", index_name, space_name)
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
        {border_name, space_name, index_name, field_names},
        call_opts
    )

    if err ~= nil then
        return nil, BorderError:new("Failed to get %s: %s", border_name, err)
    end

    local keydef = Keydef.new(replicasets, space_name, field_names, index_name)

    local tuples = {}
    for _, result in pairs(results) do
        if result[1].err ~= nil then
            return nil, BorderError:new("Failed to get %s: %s", border_name, result.err)
        end

        local tuple = result[1].res
        if tuple ~= nil then
            table.insert(tuples, tuple)
        end
    end

    local res_tuple = nil
    for _, tuple in ipairs(tuples) do
        if tuple ~= nil and is_closer(border_name, keydef, tuple, res_tuple) then
            res_tuple = tuple
        end
    end

    local result = utils.format_result({res_tuple}, space, field_names)

    if opts.fields ~= nil then
        result = utils.cut_rows(result.rows, result.metadata, opts.fields)
    end

    return result
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
function borders.min(space_name, index_name, opts)
    return get_border('min', space_name, index_name, opts)
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
function borders.max(space_name, index_name, opts)
    return get_border('max', space_name, index_name, opts)
end

return borders
