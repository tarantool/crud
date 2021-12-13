local errors = require('errors')
local fun = require('fun')

local dev_checks = require('crud.common.dev_checks')
local select_comparators = require('crud.compare.comparators')
local compat = require('crud.common.compat')
local has_keydef = compat.exists('tuple.keydef', 'key_def')

local keydef_lib
if has_keydef then
    keydef_lib = compat.require('tuple.keydef', 'key_def')
end

local utils = require('crud.common.utils')

local ExecuteSelectError = errors.new_class('ExecuteSelectError')

local executor = {}

local function scroll_to_after_tuple(gen, space, scan_index, tarantool_iter, after_tuple)
    local primary_index = space.index[0]

    local scroll_key_parts = utils.merge_primary_key_parts(scan_index.parts, primary_index.parts)

    local cmp_operator = select_comparators.get_cmp_operator(tarantool_iter)
    local scroll_comparator = select_comparators.gen_tuples_comparator(cmp_operator, scroll_key_parts)

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

local generate_value

if has_keydef then
    generate_value = function(after_tuple, scan_value, index_parts)
        local key_def = keydef_lib.new(index_parts)
        if key_def:compare_with_key(after_tuple, scan_value) < 0 then
            return key_def:extract_key(after_tuple)
        end
    end
else
    generate_value = function(after_tuple, scan_value, index_parts, tarantool_iter)
        local cmp_operator = select_comparators.get_cmp_operator(tarantool_iter)
        local scan_comparator = select_comparators.gen_tuples_comparator(cmp_operator, index_parts)
        local after_tuple_key = utils.extract_key(after_tuple, index_parts)
        if scan_comparator(after_tuple_key, scan_value) then
            return after_tuple_key
        end
    end
end

function executor.execute(space, index, filter_func, opts)
    dev_checks('table', 'table', 'function', {
        scan_value = 'table',
        after_tuple = '?table',
        tarantool_iter = 'number',
        limit = '?number',
    })

    opts = opts or {}

    local resp = { tuples_fetched = 0, tuples_lookup = 0, tuples = {} }

    if opts.limit == 0 then
        return resp
    end

    local value = opts.scan_value
    if opts.after_tuple ~= nil then
        local new_value = generate_value(opts.after_tuple, opts.scan_value, index.parts, opts.tarantool_iter)
        if new_value ~= nil then
            value = new_value
        end
    end

    local tuple
    local raw_gen, param, state = index:pairs(value, {iterator = opts.tarantool_iter})
    local gen = fun.wrap(function(param, state)
        local next_state, var = raw_gen(param, state)

        if var ~= nil then
            resp.tuples_lookup = resp.tuples_lookup + 1
        end

        return next_state, var
    end, param, state)

    if opts.after_tuple ~= nil then
        local err
        tuple, err = scroll_to_after_tuple(gen, space, index, opts.tarantool_iter, opts.after_tuple)
        if err ~= nil then
            return nil, ExecuteSelectError:new("Failed to scroll to the after_tuple: %s", err)
        end

        if tuple == nil then
            return resp
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
            table.insert(resp.tuples, tuple)
            resp.tuples_fetched = resp.tuples_fetched + 1

            if opts.limit ~= nil and resp.tuples_fetched >= opts.limit then
                break
            end
        elseif early_exit then
            break
        end

        gen.state, tuple = gen(gen.param, gen.state)
    end

    return resp
end

return executor
