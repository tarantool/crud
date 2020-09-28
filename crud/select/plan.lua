local errors = require('errors')
local json = require('json')

local collations = require('crud.common.collations')
local select_conditions = require('crud.select.conditions')

local select_plan = {}

local SelectPlanError = errors.new_class('SelectPlanError', {capture_stack = false})
local ScannerError = errors.new_class('ScannerError', {capture_stack = false})
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

local function only_one_value_is_needed(scan_index_name, scan_iter, scan_value, primary_index)
    if scan_value == nil then
        return false
    end
    if scan_index_name == primary_index.name then
        if scan_iter == box.index.EQ or scan_iter == box.index.REQ then
            if #primary_index.parts == #scan_value then
                return true -- fully specified primary key
            end
        end
    end
    return false
end

--[[
    Function returns true if main query iteration can be stopped by fired opposite condition.

    For e.g.
    - iteration goes using `'id' > 10`
    - opposite condition `'id' < 100` becomes false
    - in such case we can exit from iteration
]]
local function is_early_exit_possible(scanner, condition)
    if scanner.index_name ~= condition.operand then
        return false
    end

    local condition_iter = select_conditions.get_tarantool_iter(condition)
    if scanner.iter == box.index.REQ or scanner.iter == box.index.LT or scanner.iter == box.index.LE then
        if condition_iter == box.index.GT or condition_iter == box.index.GE then
            return true
        end
    elseif scanner.iter == box.index.EQ or scanner.iter == box.index.GT or scanner.iter == box.index.GE then
        if condition_iter == box.index.LT or condition_iter == box.index.LE then
            return true
        end
    end

    return false
end

local function get_select_scanner(space_name, space_indexes, space_format, conditions, opts)
    opts = opts or {}

    if conditions == nil then -- also cdata<NULL>
        conditions = {}
    end

    local scan_space_name = space_name
    local scan_index = nil
    local scan_iter = nil
    local scan_value = nil
    local scan_condition_num = nil
    local scan_operator

    local scan_limit = opts.limit
    local scan_after_tuple = opts.after_tuple

    -- search index to iterate over
    for i, condition in ipairs(conditions) do
        scan_index = get_index_for_condition(space_indexes, space_format, condition)

        if scan_index ~= nil then
            scan_iter = select_conditions.get_tarantool_iter(condition)
            scan_value = condition.values
            scan_condition_num = i
            scan_operator = condition.operator
            break
        end
    end

    local primary_index = space_indexes[0]

    -- default iteration index is primary index
    if scan_index == nil then
        if not index_is_allowed(primary_index) then
            return nil, IndexTypeError:new('An index that matches specified conditions was not found: ' ..
                'At least one of condition indexes or primary index should be of type TREE')
        end

        scan_index = primary_index
        scan_iter = box.index.GE -- default iteration is `next greater than previous`
        scan_value = {}
        scan_operator = select_conditions.operators.GE
    end

    if only_one_value_is_needed(scan_index.name, scan_iter, scan_value, primary_index) then
        scan_iter = box.index.REQ
        scan_limit = 1
    end

    local scanner = {
        space_name = scan_space_name,
        index_id = scan_index.id,
        index_name = scan_index.name,
        iter = scan_iter,
        value = scan_value,
        condition_num = scan_condition_num,
        operator = scan_operator,
        limit = scan_limit,
        after_tuple = scan_after_tuple,
    }

    return scanner
end

local function get_index_fieldnos(index)
    local index_fieldnos = {}

    for _, part in ipairs(index.parts) do
        table.insert(index_fieldnos, part.fieldno)
    end

    return index_fieldnos
end

local function get_values_types(space_format, fieldnos)
    local values_types = {}

    for _, fieldno in ipairs(fieldnos) do
        local field_format = space_format[fieldno]
        assert(field_format ~= nil)

        table.insert(values_types, field_format.type)
    end

    return values_types
end

local function get_values_opts(index, fieldnos)
    local values_opts = {}
    for _, fieldno in ipairs(fieldnos) do
        local is_nullable = true
        local collation

        if index ~= nil then
            local index_part

            for _, part in ipairs(index.parts) do
                if part.fieldno == fieldno then
                    index_part = part
                    break
                end
            end

            assert(index_part ~= nil)

            is_nullable = index_part.is_nullable
            collation = collations.get(index_part)
        end

        table.insert(values_opts, {
            is_nullable = is_nullable,
            collation = collation,
        })
    end

    return values_opts
end

local function get_index_by_name(space_indexes, index_name)
    for _, index in ipairs(space_indexes) do
        if index.name == index_name then
            return index
        end
    end
end

local function get_filter_conditions(space_indexes, space_format, conditions, scanner)
    local fieldnos_by_names = {}

    for i, field_format in ipairs(space_format) do
        fieldnos_by_names[field_format.name] = i
    end

    local filter_conditions = {}

    for i, condition in ipairs(conditions) do
        if i ~= scanner.condition_num then
            -- Index check (including one and multicolumn)
            local fieldnos

            local index = get_index_by_name(space_indexes, condition.operand)
            if index ~= nil then
                fieldnos = get_index_fieldnos(index)
            elseif fieldnos_by_names[condition.operand] ~= nil then
                fieldnos = {
                    fieldnos_by_names[condition.operand],
                }
            else
                return nil, ScannerError('No field or index is found for condition %s', json.encode(condition))
            end

            table.insert(filter_conditions, {
                fieldnos = fieldnos,
                operator = condition.operator,
                values = condition.values,
                types = get_values_types(space_format, fieldnos),
                early_exit_is_possible = is_early_exit_possible(scanner, condition),
                values_opts = get_values_opts(index, fieldnos)
            })
        end
    end

    return filter_conditions
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

local function is_scan_by_full_sharding_key_eq(plan, scan_index, sharding_index)
    if scan_index.id ~= sharding_index.id then
        return false
    end

    if plan.scanner.value == nil then
        return false
    end

    if plan.scanner.iter ~= box.index.EQ and plan.scanner.iter ~= box.index.REQ then
        return false
    end

    local scan_index_fieldnos = {}
    for _, part in ipairs(scan_index.parts) do
        scan_index_fieldnos[part.fieldno] = true
    end

    -- check that sharding key is included in the scan index fields
    for part_num, sharding_index_part in ipairs(sharding_index.parts) do
        local fieldno = sharding_index_part.fieldno
        if scan_index_fieldnos[fieldno] == nil or plan.scanner.value[part_num] == nil then
            return false
        end
    end

    return true
end

function select_plan.new(space, conditions, opts)
    conditions = conditions ~= nil and conditions or {}
    opts = opts ~= nil and opts or {}

    local space_name = space.name
    local space_indexes = space.index
    local space_format = space:format()

    local ok, err = validate_conditions(conditions, space_indexes, space_format)
    if not ok then
        return nil, SelectPlanError:new('Passed bad conditions: %s', err)
    end

    -- compute scanner
    local scanner, err = get_select_scanner(space_name, space_indexes, space_format, conditions, {
        limit = opts.limit,
        after_tuple = opts.after_tuple,
    })
    if err ~= nil then
        return nil, SelectPlanError:new('Failed to get index to iterate over: %s', err)
    end

    -- compute filter conditions
    local filter_conditions, err = get_filter_conditions(space_indexes, space_format, conditions, scanner)
    if err ~= nil then
        return nil, SelectPlanError:new('Failed to compute filter conditions: %s', err)
    end

    local plan = {
        scanner = scanner,
        filter_conditions = filter_conditions,
    }

    local scan_index = space_indexes[scanner.index_id]
    local sharding_index = space_indexes[0] -- XXX: only sharding by primary key is supported
    if is_scan_by_full_sharding_key_eq(plan, scan_index, sharding_index) then
        plan.scanner.limit = 1
        plan.is_scan_by_full_sharding_key_eq = true
    end

    return plan
end

return select_plan
