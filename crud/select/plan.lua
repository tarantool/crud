local errors = require('errors')
local select_conditions = require('crud.select.conditions')

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

function select_plan.gen_by_conditions(space, conditions)
    conditions = conditions ~= nil and conditions or {}

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

    local scan_index = nil
    local scan_iter = nil
    local scan_value = nil
    local scan_condition_num = nil


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
    if scan_index == nil then
        local primary_index = space_indexes[0]

        if not index_is_allowed(primary_index) then
            return nil, IndexTypeError:new('An index that matches specified conditions was not found: ' ..
                'At least one of condition indexes or primary index should be of type TREE')
        end

        scan_index = primary_index
        scan_iter = box.index.GE -- default iteration is `next greater than previous`
        scan_value = {}
    end

    local plan = {
        conditions = conditions,
        space_name = space_name,
        index_id = scan_index.id,
        scan_value = scan_value,
        scan_condition_num = scan_condition_num,
        iter = scan_iter,
    }

    return plan
end

function select_plan.is_scan_by_full_sharding_key_eq(plan, scan_index, sharding_index)
    if plan.scan_value == nil then
        return false
    end

    if plan.iter ~= box.index.EQ and plan.iter ~= box.index.REQ then
        return false
    end

    local scan_index_fieldnos = {}
    for _, part in ipairs(scan_index.parts) do
        scan_index_fieldnos[part.fieldno] = true
    end

    -- check that sharding key is included in the scan index fields
    for part_num, sharding_index_part in ipairs(sharding_index.parts) do
        local fieldno = sharding_index_part.fieldno
        if scan_index_fieldnos[fieldno] == nil or plan.scan_value[part_num] == nil then
            return false
        end
    end

    return true
end

return select_plan
