local errors = require('errors')

local select_conditions = require('crud.select.conditions')
local type_comparators = require('crud.select.type_comparators')
local operators = select_conditions.operators

local utils = require('crud.common.utils')

local ComparatorsError = errors.new_class('ComparatorsError')

local comparators = {}

local function array_eq(lhs, rhs, len, _, eq_funcs)
    for i = 1, len do
        if not eq_funcs[i](lhs[i], rhs[i]) then
            return false
        end
    end

    return true
end

local function array_lt(lhs, rhs, len, lt_funcs, eq_funcs)
    for i = 1, len do
        if lt_funcs[i](lhs[i], rhs[i]) then
            return true
        elseif not eq_funcs[i](lhs[i], rhs[i]) then
            return false
        end
    end

    return false
end

local function array_le(lhs, rhs, len, lt_funcs, eq_funcs)
    for i = 1, len do
        if lt_funcs[i](lhs[i], rhs[i]) then
            return true
        elseif not eq_funcs[i](lhs[i], rhs[i]) then
            return false
        end
    end

    return true
end

local function array_gt(lhs, rhs, len, lt_funcs, eq_funcs)
    for i = 1, len do
        if lt_funcs[i](lhs[i], rhs[i]) then
            return false
        elseif not eq_funcs[i](lhs[i], rhs[i]) then
            return true
        end
    end

    return false
end

local function array_ge(lhs, rhs, len, lt_funcs, eq_funcs)
    for i = 1, len do
        if lt_funcs[i](lhs[i], rhs[i]) then
            return false
        elseif not eq_funcs[i](lhs[i], rhs[i]) then
            return true
        end
    end

    return true
end

local function gen_array_cmp_func(target, key_parts)
    local lt_funcs = {}
    local eq_funcs = {}

    for _, part in ipairs(key_parts) do
        local lt_func, eq_func = type_comparators.get_comparators_by_type(part)
        table.insert(lt_funcs, lt_func)
        table.insert(eq_funcs, eq_func)
    end

    return function(lhs, rhs)
        return target(lhs, rhs, #key_parts, lt_funcs, eq_funcs)
    end
end

local cmp_operators_by_tarantool_iter = {
    [box.index.GT] = operators.GT,
    [box.index.GE] = operators.GT,
    [box.index.EQ] = operators.GT,
    [box.index.LT] = operators.LT,
    [box.index.LE] = operators.LT,
    [box.index.REQ] = operators.LT,
}

local array_cmp_funcs_by_operators = {
    [operators.EQ] = array_eq,
    [operators.LT] = array_lt,
    [operators.LE] = array_le,
    [operators.GT] = array_gt,
    [operators.GE] = array_ge,
}

--[=[
    Each tarantool iterator returns tuples in a strictly defined order
    (scan key merged with primary key is used to guarantee that)
    GE, GT and EQ interators return tuples in ascending order
    LE, LT and REQ - in descending
--]=]
function comparators.get_cmp_operator(tarantool_iter)
    local cmp_operator = cmp_operators_by_tarantool_iter[tarantool_iter]
    assert(cmp_operator ~= nil, 'Unsupported Tarantool iterator: ' .. tostring(tarantool_iter))

    return cmp_operator
end

function comparators.gen_func(cmp_operator, key_parts)
    local cmp_func = array_cmp_funcs_by_operators[cmp_operator]
    if cmp_func == nil then
        return nil, ComparatorsError:new('Unsupported operator %q', cmp_operator)
    end

    local func, err = gen_array_cmp_func(cmp_func, key_parts)
    if err ~= nil then
        return nil, ComparatorsError:new('Failed to generate comparator function %q', cmp_operator)
    end

    return func
end

function comparators.gen_tuples_comparator(cmp_operator, key_parts)
    local keys_comparator, err = comparators.gen_func(cmp_operator, key_parts)
    if err ~= nil then
        return nil, ComparatorsError:new("Failed to generate comparator function: %s", err)
    end

    return function(lhs, rhs)
        local lhs_key = utils.extract_key(lhs, key_parts)
        local rhs_key = utils.extract_key(rhs, key_parts)

        return keys_comparator(lhs_key, rhs_key)
    end
end

return comparators
