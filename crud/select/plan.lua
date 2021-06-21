local errors = require('errors')

local compare_conditions = require('crud.compare.conditions')
local utils = require('crud.common.utils')
local dev_checks = require('crud.common.dev_checks')

local compat = require('crud.common.compat')
local has_keydef = compat.exists('tuple.keydef', 'key_def')

local keydef_lib
if has_keydef then
    keydef_lib = compat.require('tuple.keydef', 'key_def')
end

local select_plan = {}

local IndexTypeError = errors.new_class('IndexTypeError', {capture_stack = false})
local FilterFieldsError = errors.new_class('FilterFieldsError',  {capture_stack = false})

local function index_is_allowed(index)
    return index.type == 'TREE'
end

local function get_index_for_condition(space_indexes, space_format, condition)
    -- If we use # (not table.maxn), we may lose indexes, when user drop some indexes.
    -- E.g: we have table with indexes id {1, 2, 3, nil, nil, 6}.
    -- If we use #{1, 2, 3, nil, nil, 6} (== 3) we will lose index with id = 6.
    -- See details: https://github.com/tarantool/crud/issues/103
    local max_index = table.maxn(space_indexes)
    for i = 0, max_index do
        local index = space_indexes[i]
        if index ~= nil then
            if index.name == condition.operand and index_is_allowed(index) then
                return index
            end
        end
    end

    for i = 0, max_index do
        local index = space_indexes[i]
        if index ~= nil then
            local first_part_fieldno = index.parts[1].fieldno
            local first_part_name = space_format[first_part_fieldno].name
            if first_part_name == condition.operand and index_is_allowed(index) then
                return index
            end
        end
    end
end

local function extract_sharding_key_from_scan_value(scan_value, scan_index, sharding_index)
    if #scan_value < #sharding_index.parts then
        return nil
    end

    if scan_index.id == sharding_index.id then
        return scan_value
    end

    local scan_value_fields_values = {}
    for i, scan_index_part in ipairs(scan_index.parts) do
        scan_value_fields_values[scan_index_part.fieldno] = scan_value[i]
    end

    -- check that sharding key is included in the scan index fields
    local sharding_key = {}
    for _, sharding_key_part in ipairs(sharding_index.parts) do
        local fieldno = sharding_key_part.fieldno

        -- sharding key isn't included in scan key
        if scan_value_fields_values[fieldno] == nil then
            return nil
        end

        local field_value = scan_value_fields_values[fieldno]

        -- sharding key contains nil values
        if field_value == nil then
            return nil
        end

        table.insert(sharding_key, field_value)
    end

    return sharding_key
end

-- We need to construct after_tuple by field_names
-- because if `fields` option is specified we have after_tuple with partial fields
-- and these fields are ordered by field_names + primary key + scan key
-- this order can be differ from order in space format
-- so we need to cast after_tuple to space format for scrolling tuples on storage
local function construct_after_tuple_by_fields(space_format, field_names, tuple)
    if tuple == nil then
        return nil
    end

    if field_names == nil then
        return tuple
    end

    local positions = {}
    local transformed_tuple = {}

    for i, field in ipairs(space_format) do
        positions[field.name] = i
    end

    for i, field_name in ipairs(field_names) do
        local fieldno = positions[field_name]
        if fieldno == nil then
            return nil, FilterFieldsError:new(
                    'Space format doesn\'t contain field named %q', field_name
            )
        end

        transformed_tuple[fieldno] = tuple[i]
    end

    return transformed_tuple
end

function select_plan.new(space, conditions, opts)
    dev_checks('table', '?table', {
        first = '?number',
        after_tuple = '?table|cdata',
        field_names = '?table',
        force_map_call = '?boolean',
    })

    conditions = conditions ~= nil and conditions or {}
    opts = opts or {}

    local space_name = space.name
    local space_indexes = space.index
    local space_format = space:format()

    if conditions == nil then -- also cdata<NULL>
        conditions = {}
    end

    local scan_index
    local scan_iter
    local scan_value
    local scan_condition_num

    -- search index to iterate over
    for i, condition in ipairs(conditions) do
        scan_index = get_index_for_condition(space_indexes, space_format, condition)

        if scan_index ~= nil then
            scan_iter = compare_conditions.get_tarantool_iter(condition)
            scan_value = condition.values
            scan_condition_num = i
            break
        end
    end

    -- default iteration index is primary index
    local primary_index = space_indexes[0]
    if scan_index == nil then

        if not index_is_allowed(primary_index) then
            return nil, IndexTypeError:new('An index that matches specified conditions was not found: ' ..
                'At least one of condition indexes or primary index should be of type TREE')
        end

        scan_index = primary_index
        scan_iter = box.index.GE -- default iteration is `next greater than previous`
        scan_value = {}
    end

    local cmp_key_parts = utils.merge_primary_key_parts(scan_index.parts, primary_index.parts)
    local field_names = utils.enrich_field_names_with_cmp_key(opts.field_names, cmp_key_parts, space_format)

    -- handle opts.first
    local total_tuples_count
    local scan_after_tuple, err = construct_after_tuple_by_fields(
            space_format, field_names, opts.after_tuple
    )
    if err ~= nil then
        return nil, err
    end

    if opts.first ~= nil then
        total_tuples_count = math.abs(opts.first)

        if opts.first < 0 then
            scan_iter = utils.invert_tarantool_iter(scan_iter)

            -- scan condition becomes border consition
            scan_condition_num = nil

            if scan_after_tuple ~= nil then
                if has_keydef then
                    local key_def = keydef_lib.new(scan_index.parts)
                    scan_value = key_def:extract_key(scan_after_tuple)
                else
                    scan_value = utils.extract_key(scan_after_tuple, scan_index.parts)
                end
            else
                scan_value = nil
            end
        end
    end

    local sharding_index = primary_index -- XXX: only sharding by primary key is supported

    -- get sharding key value
    local sharding_key
    if scan_value ~= nil and (scan_iter == box.index.EQ or scan_iter == box.index.REQ) then
        sharding_key = extract_sharding_key_from_scan_value(scan_value, scan_index, sharding_index)
    end

    if sharding_key ~= nil and opts.force_map_call ~= true then
        total_tuples_count = 1
        scan_iter = box.index.REQ
    end

    local plan = {
        conditions = conditions,
        space_name = space_name,
        index_id = scan_index.id,
        scan_value = scan_value,
        after_tuple = scan_after_tuple,
        scan_condition_num = scan_condition_num,
        tarantool_iter = scan_iter,
        total_tuples_count = total_tuples_count,
        sharding_key = sharding_key,
        field_names = field_names,
    }

    return plan
end

return select_plan
