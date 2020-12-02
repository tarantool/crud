local json = require('json')
local errors = require('errors')

local utils = require('crud.common.utils')
local dev_checks = require('crud.common.dev_checks')
local collations = require('crud.common.collations')
local select_conditions = require('crud.select.conditions')

local ParseConditionsError = errors.new_class('ParseConditionsError', {capture_stack = false})
local GenFiltersError = errors.new_class('GenFiltersError', {capture_stack = false})

local filters = {}

--[[
    Function returns true if main query iteration can be stopped by fired opposite condition.

    For e.g.
    - iteration goes using `'id' > 10`
    - opposite condition `'id' < 100` becomes false
    - in such case we can exit from iteration
]]
local function is_early_exit_possible(index, iter, condition)
    if index == nil then
        return false
    end

    if index.name ~= condition.operand then
        return false
    end

    local condition_iter = select_conditions.get_tarantool_iter(condition)
    if iter == box.index.REQ or iter == box.index.LT or iter == box.index.LE then
        if condition_iter == box.index.GT or condition_iter == box.index.GE then
            return true
        end
    elseif iter == box.index.EQ or iter == box.index.GT or iter == box.index.GE then
        if condition_iter == box.index.LT or condition_iter == box.index.LE then
            return true
        end
    end

    return false
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
    for i = 0, #space_indexes do
       local index = space_indexes[i]
        if index.name == index_name then
            return index
        end
    end
end

local function parse(space, conditions, opts)
    dev_checks('table', '?table', {
        scan_condition_num = '?number',
        iter = 'number',
    })

    conditions = conditions ~= nil and conditions or {}

    local space_format = space:format()
    local space_indexes = space.index

    local fieldnos_by_names = {}

    for i, field_format in ipairs(space_format) do
        fieldnos_by_names[field_format.name] = i
    end

    local filter_conditions = {}

    for i, condition in ipairs(conditions) do
        if i ~= opts.scan_condition_num then
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
                return nil, ParseConditionsError('No field or index is found for condition %s', json.encode(condition))
            end

            table.insert(filter_conditions, {
                fieldnos = fieldnos,
                operator = condition.operator,
                values = condition.values,
                types = get_values_types(space_format, fieldnos),
                early_exit_is_possible = is_early_exit_possible(index, opts.iter, condition),
                values_opts = get_values_opts(index, fieldnos)
            })
        end
    end

    return filter_conditions
end

local function format_value(value)
    if type(value) == 'nil' then
        return 'nil'
    elseif value == nil then
        return 'NULL'
    elseif type(value) == 'string' then
        return ("%q"):format(value)
    elseif type(value) == 'number' then
        return tostring(value)
    elseif type(value) == 'boolean' then
        return tostring(value)
    elseif type(value) == 'cdata' then
        if utils.is_uuid(value) then
            return ("%q"):format(value)
        end
        return tostring(value)
    end
    assert(false, ('Unexpected value %s (type %s)'):format(value, type(value)))
end

local PARSE_ARGS_TEMPLATE = 'local tuple = ...'
local LIB_FUNC_HEADER_TEMPLATE = 'function M.%s(%s)'

local function concat_conditions(conditions, operator)
    return '(' .. table.concat(conditions, (' %s '):format(operator)) .. ')'
end

local function get_field_variable_name(fieldno)
    return string.format('field_%s', fieldno)
end

local function get_eq_func_name(id)
    return string.format('eq_%s', id)
end

local function get_cmp_func_name(id)
    return string.format('cmp_%s', id)
end

local function gen_tuple_fields_def_code(filter_conditions)
    -- get field numbers
    local fieldnos_added = {}
    local fieldnos = {}

    for _, cond in ipairs(filter_conditions) do
        for i = 1, #cond.values do
            local fieldno = cond.fieldnos[i]
            if not fieldnos_added[fieldno] then
                table.insert(fieldnos, fieldno)
                fieldnos_added[fieldno] = true
            end
        end
    end

    -- gen definitions for all used fields
    local fields_def_parts = {}

    for _, fieldno in ipairs(fieldnos) do
        table.insert(fields_def_parts, string.format(
            'local %s = tuple[%s]',
            get_field_variable_name(fieldno), fieldno
        ))
    end

    return table.concat(fields_def_parts, '\n')
end

local function format_comp_with_value(fieldno, func_name, value)
    return string.format(
        '%s(%s, %s)',
        func_name,
        get_field_variable_name(fieldno),
        format_value(value)
    )
end

local function add_strict_postfix(func_name, value_opts)
    if value_opts.is_nullable == false then
        return string.format('%s_strict', func_name)
    end

    return func_name
end

local function add_collation_postfix(func_name, value_opts)
    if collations.is_default(value_opts.collation) then
        return func_name
    end

    if value_opts.collation == collations.UNICODE then
        return string.format('%s_unicode', func_name)
    end

    if value_opts.collation == collations.UNICODE_CI then
        return string.format('%s_unicode_ci', func_name)
    end

    error('Unsupported collation: ' .. tostring(value_opts.collation))
end

local function format_eq(cond)
    local cond_strings = {}
    local values_opts = cond.values_opts or {}

    for j = 1, #cond.values do
        local fieldno = cond.fieldnos[j]
        local value = cond.values[j]
        local value_type = cond.types[j]
        local value_opts = values_opts[j] or {}

        local func_name = 'eq'

        if value_type == 'string' then
            func_name = add_collation_postfix('eq', value_opts)
            if collations.is_unicode(value_opts.collation) then
                func_name = add_strict_postfix(func_name, value_opts)
            end
        elseif value_type == 'uuid' then
            func_name = 'eq_uuid'
        end

        table.insert(cond_strings, format_comp_with_value(fieldno, func_name, value))
    end

    return cond_strings
end

local function format_lt(cond)
    local cond_strings = {}
    local values_opts = cond.values_opts or {}

    for j = 1, #cond.values do
        local fieldno = cond.fieldnos[j]
        local value = cond.values[j]
        local value_type = cond.types[j]
        local value_opts = values_opts[j] or {}

        local func_name = 'lt'

        if value_type == 'boolean' then
            func_name = 'lt_boolean'
        elseif value_type == 'string' then
            func_name = add_collation_postfix('lt', value_opts)
        elseif value_type == 'uuid' then
            func_name = 'lt_uuid'
        end
        func_name = add_strict_postfix(func_name, value_opts)

        table.insert(cond_strings, format_comp_with_value(fieldno, func_name, value))
    end

    return cond_strings
end

local function gen_eq_func_code(func_name, cond, func_args_code)
    local func_code_lines = {}

    local eq_conds = format_eq(cond)

    local header = LIB_FUNC_HEADER_TEMPLATE:format(func_name, func_args_code)
    table.insert(func_code_lines, header)

    local return_line = string.format(
        '    return %s', concat_conditions(eq_conds, 'and')
    )
    table.insert(func_code_lines, return_line)

    table.insert(func_code_lines, 'end')

    return table.concat(func_code_lines, '\n')
end

local results_by_operators = {
    [select_conditions.operators.LT] = {
        le = true, not_eq = false, default = false,
    },
    [select_conditions.operators.LE] = {
        le = true, not_eq = false, default = true,
    },
    [select_conditions.operators.GT] = {
        le = false, not_eq = true, default = false,
    },
    [select_conditions.operators.GE] = {
        le = false, not_eq = true, default = true,
    },
}

--[[
    This function generates code of function that compares arrays of values.
    All conditions are presented using `lt`, `eq` and `not` operations.

    Values are compared one by one, for each value two checkes are performed:
      - le:     field_N < value_N
      - not_eq: field_N != value_N

    If one of these conditions becomes true,
    results_by_operators[<operator>].<check> is returned.

    After checking all values, if there wasn't early return,
    results_by_operators[<operator>].default is returned.
--]]
local function gen_cmp_array_func_code(operator, func_name, cond, func_args_code)
    local func_code_lines = {}

    local eq_conds = format_eq(cond)
    local lt_conds = format_lt(cond)

    local header = LIB_FUNC_HEADER_TEMPLATE:format(func_name, func_args_code)
    table.insert(func_code_lines, header)

    assert(#lt_conds == #eq_conds)

    local results = results_by_operators[operator]
    assert(results ~= nil)

    for i = 1, #eq_conds do
        local comp_value_code = table.concat({
            string.format('    if %s then return %s end', lt_conds[i], results.le),
            string.format('    if not %s then return %s end', eq_conds[i], results.not_eq),
            '',
        }, '\n')

        table.insert(func_code_lines, comp_value_code)
    end

    local return_code = ('    return %s'):format(results.default)
    table.insert(func_code_lines, return_code)

    table.insert(func_code_lines, 'end')

    return table.concat(func_code_lines, '\n')
end

local function function_args_by_fieldnos(fieldnos)
    local arg_names = {}
    for _, fieldno in ipairs(fieldnos) do
        table.insert(arg_names, get_field_variable_name(fieldno))
    end
    return table.concat(arg_names, ', ')
end

local function gen_library_func(id, cond, func_args_code)
    local library_func_code, library_func_name

    if cond.operator == select_conditions.operators.EQ then
        library_func_name = get_eq_func_name(id)
        library_func_code = gen_eq_func_code(library_func_name, cond, func_args_code)
    else
        library_func_name = get_cmp_func_name(id)
        library_func_code = gen_cmp_array_func_code(
            cond.operator, library_func_name, cond, func_args_code
        )
    end

    return library_func_name, library_func_code
end

local function gen_filter_code(filter_conditions)
    if #filter_conditions == 0 then
        return { code = 'return true, false', library = 'return {}' }
    end

    local library_funcs_code_parts = {}

    local filter_code_parts = {}

    table.insert(filter_code_parts, PARSE_ARGS_TEMPLATE)
    table.insert(filter_code_parts, '')

    local tuple_fields_def = gen_tuple_fields_def_code(filter_conditions)

    table.insert(filter_code_parts, tuple_fields_def)
    table.insert(filter_code_parts, '')

    for i, cond in ipairs(filter_conditions) do
        local args_fieldnos = { unpack(cond.fieldnos, 1, #cond.values) }
        local func_args_code = function_args_by_fieldnos(args_fieldnos)

        local library_func_name, library_func_code = gen_library_func(i, cond, func_args_code)
        table.insert(library_funcs_code_parts, library_func_code)

        local field_check_code = string.format(
            'if not %s(%s) then return false, %s end',
            library_func_name, func_args_code, cond.early_exit_is_possible
        )
        table.insert(filter_code_parts, field_check_code)
    end

    table.insert(filter_code_parts, '')
    table.insert(filter_code_parts, 'return true, false')

    local library_funcs_code = table.concat(library_funcs_code_parts, '\n\n')
    local library_code = table.concat({
        'local M = {}',
        library_funcs_code,
        'return M'
    }, '\n\n')

    return {
        code = table.concat(filter_code_parts, '\n'),
        library = library_code,
    }
end

local function lt_nullable(lhs, rhs)
    if lhs == nil and rhs ~= nil then
        return true
    elseif rhs == nil then
        return false
    end
    return lhs < rhs
end

local function lt_strict(lhs, rhs)
    if rhs == nil then
        return false
    end
    return lhs < rhs
end

local function lt_unicode_nullable(lhs, rhs)
    if lhs == nil and rhs ~= nil then
        return true
    elseif rhs == nil then
        return false
    end
    return utf8.cmp(lhs, rhs) < 0
end

local function lt_unicode_strict(lhs, rhs)
    if rhs == nil then
        return false
    end
    return utf8.cmp(lhs, rhs) < 0
end

local function lt_boolean_nullable(lhs, rhs)
    if lhs == nil and rhs ~= nil then
        return true
    elseif rhs == nil then
        return false
    end
    return (not lhs) and rhs
end

local function lt_boolean_strict(lhs, rhs)
    if rhs == nil then
        return false
    end
    return (not lhs) and rhs
end

local function lt_uuid_nullable(lhs, rhs)
    if lhs == nil and rhs ~= nil then
        return true
    elseif rhs == nil then
        return false
    end
    return tostring(lhs) < tostring(rhs)
end

local function lt_uuid_strict(lhs, rhs)
    if rhs == nil then
        return false
    end
    return tostring(lhs) < tostring(rhs)
end

local function lt_unicode_ci_nullable(lhs, rhs)
    if lhs == nil and rhs ~= nil then
        return true
    elseif rhs == nil then
        return false
    end
    return utf8.casecmp(lhs, rhs) < 0
end

local function lt_unicode_ci_strict(lhs, rhs)
    if rhs == nil then
        return false
    end
    return utf8.casecmp(lhs, rhs) < 0
end

local function eq(lhs, rhs)
    return lhs == rhs
end

local function eq_uuid(lhs, rhs)
    if lhs == nil then
        return rhs == nil
    end
    return tostring(lhs) == tostring(rhs)
end

local function eq_uuid_strict(lhs, rhs)
    if rhs == nil then
        return false
    end
    return tostring(lhs) == tostring(rhs)
end

local function eq_unicode_nullable(lhs, rhs)
    if lhs == nil and rhs == nil then
        return true
    elseif rhs == nil then
        return false
    end
    return utf8.cmp(lhs, rhs) == 0
end

local function eq_unicode_strict(lhs, rhs)
    if rhs == nil then
        return false
    end
    return utf8.cmp(lhs, rhs) == 0
end

local function eq_unicode_ci_nullable(lhs, rhs)
    if lhs == nil and rhs == nil then
        return true
    elseif rhs == nil then
        return false
    end
    return utf8.casecmp(lhs, rhs) == 0
end

local function eq_unicode_ci_strict(lhs, rhs)
    if rhs == nil then
        return false
    end
    return utf8.casecmp(lhs, rhs) == 0
end

local library = {
    -- EQ
    eq = eq,
    eq_uuid = eq_uuid,
    eq_uuid_strict = eq_uuid_strict,
    -- nullable
    eq_unicode = eq_unicode_nullable,
    eq_unicode_ci = eq_unicode_ci_nullable,
    -- strict
    eq_unicode_strict = eq_unicode_strict,
    eq_unicode_ci_strict = eq_unicode_ci_strict,

    -- LT
    -- nullable
    lt = lt_nullable,
    lt_unicode = lt_unicode_nullable,
    lt_unicode_ci = lt_unicode_ci_nullable,
    lt_boolean = lt_boolean_nullable,
    lt_uuid = lt_uuid_nullable,
    -- strict
    lt_strict = lt_strict,
    lt_unicode_strict = lt_unicode_strict,
    lt_unicode_ci_strict = lt_unicode_ci_strict,
    lt_boolean_strict = lt_boolean_strict,
    lt_uuid_strict = lt_uuid_strict,

    utf8 = utf8,

    -- NULL
    NULL = box.NULL,
}

local function compile(filter_code)
    local lib, err = load(filter_code.library, 'library', 'bt', library)
    assert(lib, err)
    lib = lib()

    for name, f in pairs(library) do
        lib[name] = f
    end

    local func, err = load(filter_code.code, 'code', 'bt', lib)
    assert(func, err)
    return func
end

function filters.gen_func(space, conditions, opts)
    dev_checks('table', '?table', {
        iter = 'number',
        scan_condition_num = '?number',
    })

    local filter_conditions, err = parse(space, conditions, {
        scan_condition_num = opts.scan_condition_num,
        iter = opts.iter,
    })
    if err ~= nil then
        return nil, GenFiltersError:new("Failed to generate filters for specified conditions: %s", err)
    end

    local filter_code = gen_filter_code(filter_conditions)
    local filter_func = compile(filter_code)

    return filter_func
end

filters.internal = {
    parse = parse,
    gen_filter_code = gen_filter_code,
    compile = compile,
    get_index_by_name = get_index_by_name,
}

return filters
