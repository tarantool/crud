local errors = require('errors')

local select_conditions = require('crud.select.conditions')
local operators = select_conditions.operators

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

local function gen_array_cmp_func(target, index_parts)
    local lt_funcs = {}
    local eq_funcs = {}

    for _, part in ipairs(index_parts) do
        if part.collation == nil then
            table.insert(lt_funcs, lt)
            table.insert(eq_funcs, eq)
        elseif part.collation == 'unicode' then
            table.insert(lt_funcs, lt_unicode)
            table.insert(eq_funcs, eq_unicode)
        elseif part.collation == 'unicode_ci' then
            table.insert(lt_funcs, lt_unicode_ci)
            table.insert(eq_funcs, eq_unicode_ci)
        else
            return nil, GenFuncError:new('Unsupported tarantool collation %q', part.collation)
        end
    end

    return function(lhs, rhs)
        return target(lhs, rhs, #index_parts, lt_funcs, eq_funcs)
    end
end

local array_cmp_funcs_by_operators = {
    [operators.EQ] = array_eq,
    [operators.LT] = array_lt,
    [operators.LE] = array_le,
    [operators.GT] = array_gt,
    [operators.GE] = array_ge,
}

function comparators.gen_func(operator, index_parts)
    local cmp_func = array_cmp_funcs_by_operators[operator]

    if cmp_func == nil then
        return nil, ComparatorsError:new('Unsupported operator %q', operator)
    end

    local func, err = gen_array_cmp_func(cmp_func, index_parts)
    if err ~= nil then
        return nil, ComparatorsError:new('Failed to generate comparator function %q', operator)
    end

    return func
end

return comparators
