local errors = require('errors')

local collations = require('crud.common.collations')
local select_conditions = require('crud.select.conditions')
local operators = select_conditions.operators

local utils = require('crud.common.utils')

local LessThenError = errors.new_class('LessThenError')
local GenFuncError = errors.new_class('GenFuncError')
local ComparatorsError = errors.new_class('ComparatorsError')

local comparators = {}

local function eq(lhs, rhs)
    return lhs == rhs
end

local function eq_unicode(lhs, rhs)
    if type(lhs) == 'string' and type(rhs) == 'string' then
        return utf8.cmp(lhs, rhs) == 0
    end

    return eq(lhs)
end

local function eq_unicode_ci(lhs, rhs)
    if type(lhs) == 'string' and type(rhs) == 'string' then
        return utf8.casecmp(lhs, rhs) == 0
    end

    return lhs == rhs
end

local function lt(lhs, rhs)
    if lhs == nil and rhs ~= nil then
        return true
    elseif rhs == nil then
        return false
    end

    -- boolean compare
    local lhs_is_boolean = type(lhs) == 'boolean'
    local rhs_is_boolean = type(rhs) == 'boolean'

    if lhs_is_boolean and rhs_is_boolean then
        return (not lhs) and rhs
    elseif lhs_is_boolean or rhs_is_boolean then
        LessThenError:assert(false, 'Could not compare boolean and not boolean')
    end

    -- general compare
    return lhs < rhs
end

local function lt_unicode(lhs, rhs)
    if type(lhs) == 'string' and type(rhs) == 'string' then
        return utf8.cmp(lhs, rhs) == -1
    end

    return lt(lhs, rhs)
end

local function lt_unicode_ci(lhs, rhs)
    if type(lhs) == 'string' and type(rhs) == 'string' then
        return utf8.casecmp(lhs, rhs) == -1
    end

    return lt(lhs, rhs)
end

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
        local collation = collations.get(part)
        if collations.is_default(collation) then
            table.insert(lt_funcs, lt)
            table.insert(eq_funcs, eq)
        elseif collation == collations.UNICODE then
            table.insert(lt_funcs, lt_unicode)
            table.insert(eq_funcs, eq_unicode)
        elseif collation == collations.UNICODE_CI then
            table.insert(lt_funcs, lt_unicode_ci)
            table.insert(eq_funcs, eq_unicode_ci)
        else
            return nil, GenFuncError:new('Unsupported Tarantool collation %q', collation)
        end
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
