local checks = require('checks')
local errors = require('errors')

local select_conditions = require('crud.select.conditions')
local utils = require('crud.common.utils')

local select_plan = {}

local SelectPlanError = errors.new_class('SelectPlanError', {capture_stack = false})
local IndexTypeError = errors.new_class('IndexTypeError', {capture_stack = false})
local ValidateConditionsError = errors.new_class('ValidateConditionsError', {capture_stack = false})

local function index_is_allowed(index)
    return index.type == 'TREE'
end

local function get_index_for_condition(space_indexes, space_format, condition)
    for i= 0, #space_indexes do
        local index = space_indexes[i]
        if index.name == condition.operand and index_is_allowed(index) then
            return index
        end
    end

    for i = 0, #space_indexes do
        local index = space_indexes[i]
        local first_part_fieldno = index.parts[1].fieldno
        local first_part_name = space_format[first_part_fieldno].name
        if first_part_name == condition.operand and index_is_allowed(index) then
            return index
        end
    end
end

local function validate_conditions(conditions, space_indexes, space_format)
    local field_names = {}
    for _, field_format in ipairs(space_format) do
        field_names[field_format.name] = true
    end

    local index_names = {}
    for _, index in ipairs(space_indexes) do
        index_names[index.name] = true
    end

    for _, condition in ipairs(conditions) do
        if index_names[condition.operand] == nil and field_names[condition.operand] == nil then
            return false, ValidateConditionsError:new("No field or index %q found", condition.operand)
        end
    end

    return true
end

local function is_scan_by_full_sharding_key_eq(scan_index, scan_value, scan_iter, sharding_index)
    if scan_value == nil then
        return false
    end

    if scan_iter ~= box.index.EQ and scan_iter ~= box.index.REQ then
        return false
    end

    local scan_index_fieldnos = {}
    for _, part in ipairs(scan_index.parts) do
        scan_index_fieldnos[part.fieldno] = true
    end

    -- check that sharding key is included in the scan index fields
    for part_num, sharding_index_part in ipairs(sharding_index.parts) do
        local fieldno = sharding_index_part.fieldno
        if scan_index_fieldnos[fieldno] == nil or scan_value[part_num] == nil then
            return false
        end
    end

    return true
end

function select_plan.new(space, conditions, opts)
    checks('table', '?table', {
        limit = '?number',
    })
    conditions = conditions ~= nil and conditions or {}
    opts = opts or {}

    local space_name = space.name
    local space_indexes = space.index
    local space_format = space:format()

    local ok, err = validate_conditions(conditions, space_indexes, space_format)
    if not ok then
        return nil, SelectPlanError:new('Passed bad conditions: %s', err)
    end

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
            scan_iter = select_conditions.get_tarantool_iter(condition)
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

    -- set total_tuples_count
    local total_tuples_count = opts.limit

    -- get sharding key value
    local sharding_key

    if is_scan_by_full_sharding_key_eq(scan_index, scan_value, scan_iter, primary_index) then
        total_tuples_count = 1
        scan_iter = box.index.REQ

        sharding_key = utils.extract_sharding_key_from_scan_key(
            scan_value, scan_index, primary_index
        )
    end

    local plan = {
        conditions = conditions,
        space_name = space_name,
        index_id = scan_index.id,
        scan_value = scan_value,
        scan_condition_num = scan_condition_num,
        iter = scan_iter,
        total_tuples_count = total_tuples_count,
        sharding_key = sharding_key,
    }

    return plan
end

return select_plan
