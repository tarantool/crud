local errors = require('errors')
local log = require('log')

local select_filters = require('crud.select.filters')
local select_comparators = require('crud.select.comparators')

local utils = require('crud.common.utils')

local ScrollToAfterError = errors.new_class('ScrollToAfterError')
local ExecuteSelectError = errors.new_class('ExecuteSelectError')

local executor = {}

local function scroll_to_after_tuple(gen, param, state, space, scanner)
    local scan_index = space.index[scanner.index_id]
    local primary_index = space.index[0]

    local scroll_key_parts = utils.merge_primary_key_parts(scan_index.parts, primary_index.parts)

    local cmp_operator = select_comparators.get_cmp_operator(scanner.iter)
    local scroll_comparator, err = select_comparators.gen_tuples_comparator(cmp_operator, scroll_key_parts)
    if err ~= nil then
        return nil, ScrollToAfterError:new("Failed to generate comparator to scroll: %s", err)
    end

    while true do
        local tuple
        state, tuple = gen(param, state)

        if tuple == nil then
            return nil
        end

        if scroll_comparator(tuple, scanner.after_tuple) then
            return tuple
        end
    end
end

function executor.execute(plan)
    local scanner = plan.scanner

    if scanner.limit == 0 then
        return {}
    end

    local filter = select_filters.gen_code(plan.filter_conditions)
    local filer_func = select_filters.compile(filter)

    local tuples = {}
    local tuples_count = 0

    local space = box.space[scanner.space_name]
    local index = space.index[scanner.index_id]

    local scan_value = scanner.value
    if scanner.after_tuple ~= nil then
        if scan_value == nil then
            scan_value = scanner.after_tuple
        else
            local cmp_operator = select_comparators.get_cmp_operator(scanner.iter)
            local scan_comparator, err = select_comparators.gen_tuples_comparator(cmp_operator, index.parts)
            if err ~= nil then
                log.warn("Failed to generate comparator for scan value: %s", err)
            elseif scan_comparator(scanner.after_tuple, scan_value) then
                local after_tuple_key = utils.extract_key(scanner.after_tuple, index.parts)
                scan_value = after_tuple_key
            end
        end
    end

    local tuple
    local gen,param,state = index:pairs(scan_value, {iterator = scanner.iter})

    if scanner.after_tuple ~= nil then
        local err
        tuple, err = scroll_to_after_tuple(gen, param, state, space, scanner)
        if err ~= nil then
            return nil, ExecuteSelectError:new("Failed to scroll to the last tuple: %s", err)
        end

        if tuple == nil then
            return {}
        end
    end

    if tuple == nil then
        state, tuple = gen(param, state)
    end

    while true do
        if tuple == nil then
            break
        end

        local matched, early_exit = filer_func(tuple)

        if matched then
            table.insert(tuples, tuple)
            tuples_count = tuples_count + 1

            if scanner.limit ~= nil and tuples_count >= scanner.limit then
                break
            end
        elseif early_exit then
            break
        end

        state, tuple = gen(param, state)
    end

    return tuples
end

return executor
