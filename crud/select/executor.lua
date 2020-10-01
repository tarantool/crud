local checks = require('checks')
local errors = require('errors')
local log = require('log')

local select_comparators = require('crud.select.comparators')

local utils = require('crud.common.utils')

local ScrollToAfterError = errors.new_class('ScrollToAfterError')
local ExecuteSelectError = errors.new_class('ExecuteSelectError')

local executor = {}

local function scroll_to_after_tuple(gen, space, scan_index, iter, after_tuple)
    local primary_index = space.index[0]

    local scroll_key_parts = utils.merge_primary_key_parts(scan_index.parts, primary_index.parts)

    local cmp_operator = select_comparators.get_cmp_operator(iter)
    local scroll_comparator, err = select_comparators.gen_tuples_comparator(cmp_operator, scroll_key_parts)
    if err ~= nil then
        return nil, ScrollToAfterError:new("Failed to generate comparator to scroll: %s", err)
    end

    while true do
        local tuple
        gen.state, tuple = gen(gen.param, gen.state)

        if tuple == nil then
            return nil
        end

        if scroll_comparator(tuple, after_tuple) then
            return tuple
        end
    end
end

function executor.execute(space, index, scan_value, iter, filter_func, opts)
    checks('table', 'table', '?table', 'number', 'function', {
        after_tuple = '?cdata|table',
        limit = '?number',
    })

    opts = opts or {}

    if opts.limit == 0 then
        return {}
    end

    local tuples = {}
    local tuples_count = 0

    local value = scan_value
    if opts.after_tuple ~= nil then
        if value == nil then
            value = opts.after_tuple
        else
            local cmp_operator = select_comparators.get_cmp_operator(iter)
            local scan_comparator, err = select_comparators.gen_tuples_comparator(cmp_operator, index.parts)
            if err ~= nil then
                log.warn("Failed to generate comparator for scan value: %s", err)
            elseif scan_comparator(opts.after_tuple, scan_value) then
                local after_tuple_key = utils.extract_key(opts.after_tuple, index.parts)
                value = after_tuple_key
            end
        end
    end

    local tuple
    local gen = index:pairs(value, {iterator = iter})

    if opts.after_tuple ~= nil then
        local err
        tuple, err = scroll_to_after_tuple(gen, space, index, iter, opts.after_tuple)
        if err ~= nil then
            return nil, ExecuteSelectError:new("Failed to scroll to the after tuple: %s", err)
        end

        if tuple == nil then
            return {}
        end
    end

    if tuple == nil then
        gen.state, tuple = gen(gen.param, gen.state)
    end

    while true do
        if tuple == nil then
            break
        end

        local matched, early_exit = filter_func(tuple)

        if matched then
            table.insert(tuples, tuple)
            tuples_count = tuples_count + 1

            if opts.limit ~= nil and tuples_count >= opts.limit then
                break
            end
        elseif early_exit then
            break
        end

        gen.state, tuple = gen(gen.param, gen.state)
    end

    return tuples
end

return executor
