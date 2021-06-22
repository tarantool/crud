local errors = require('errors')

local compare_conditions = require('crud.compare.conditions')
local type_comparators = require('crud.compare.type_comparators')
local utils = require('crud.common.utils')

local compat = require('crud.common.compat')
local has_keydef = compat.exists('tuple.keydef', 'key_def')

local keydef_lib
if has_keydef then
    keydef_lib = compat.require('tuple.keydef', 'key_def')
end

local operators = compare_conditions.operators

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

-- if `fields` option is specified then tuples to compare contains
-- fields in order specified by field_names
-- for comparison tuples we need to change fieldno in key_parts according to field_names
function comparators.update_key_parts_by_field_names(space_format, field_names, key_parts)
    if field_names == nil then
        return key_parts
    end

    local fields_positions = {}
    local updated_key_parts = {}

    for i, field_name in ipairs(field_names) do
        fields_positions[field_name] = i
    end

    for _, part in ipairs(key_parts) do
        local field_name = space_format[part.fieldno].name
        local updated_part = {type = part.type,
                              fieldno = fields_positions[field_name],
                              is_nullable = part.is_nullable,
                              path = part.path}
        table.insert(updated_key_parts, updated_part)
    end

    return updated_key_parts
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
    GE, GT and EQ iterators return tuples in ascending order
    LE, LT and REQ - in descending
--]=]
function comparators.get_cmp_operator(tarantool_iter)
    local cmp_operator = cmp_operators_by_tarantool_iter[tarantool_iter]
    assert(cmp_operator ~= nil, 'Unsupported Tarantool iterator: ' .. tostring(tarantool_iter))

    return cmp_operator
end

function comparators.gen_func(cmp_operator, key_parts)
    local cmp_func = array_cmp_funcs_by_operators[cmp_operator]
    ComparatorsError:assert(cmp_func ~= nil, 'Unsupported operator %q', cmp_operator)

    return gen_array_cmp_func(cmp_func, key_parts)
end

function comparators.gen_tuples_comparator(cmp_operator, key_parts, field_names, space_format)
    local updated_key_parts = comparators.update_key_parts_by_field_names(
            space_format, field_names, key_parts
    )

    local keys_comparator = comparators.gen_func(cmp_operator, updated_key_parts)

    if has_keydef then
        local key_def = keydef_lib.new(updated_key_parts)
        return function(lhs, rhs)
            local lhs_key = key_def:extract_key(lhs)
            local rhs_key = key_def:extract_key(rhs)
            return keys_comparator(lhs_key, rhs_key)
        end
    else
        return function(lhs, rhs)
            local lhs_key = utils.extract_key(lhs, updated_key_parts)
            local rhs_key = utils.extract_key(rhs, updated_key_parts)
            return keys_comparator(lhs_key, rhs_key)
        end
    end
end

return comparators
