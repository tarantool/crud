local select_conditions = require('elect.select.conditions')

local function format_value(value)
    if type(value) == 'nil' then
        return 'nil'
    elseif value == nil then
        return 'NULL'
    elseif type(value) == 'string' then
        return ("%q"):format(value)
    elseif type(value) == 'number' then
        return tostring(value)
    elseif type(value) == 'cdata' then
        return tostring(value)
    elseif type(value) == 'boolean' then
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

local function format_eq(cond)
    local cond_strings = {}
    local values_opts = cond.values_opts or {}

    for j = 1, #cond.values do
        local fieldno = cond.fieldnos[j]
        local value = cond.values[j]
        local value_opts = values_opts[j] or {}

        local postfix = ''
        if value_opts.is_nullable == false then
            postfix = '_strict'
        end

        local func_name
        if value_opts.collation ~= nil then
            if value_opts.collation == 'unicode' then
                func_name = 'eq_unicode' .. postfix
            elseif value_opts.collation == 'unicode_ci' then
                func_name = 'eq_unicode_ci' .. postfix
            else
                error('unknown collation: ' .. tostring(value_opts.collation))
            end
        else
            func_name = 'eq'
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

        local postfix = ''
        if value_opts.is_nullable == false then
            postfix = '_strict'
        end

        local func_name
        if value_opts.collation ~= nil then
            if value_opts.collation == 'unicode' then
                func_name = 'lt_unicode' .. postfix
            elseif value_opts.collation == 'unicode_ci' then
                func_name = 'lt_unicode_ci' .. postfix
            else
                error('unknown collation: ' .. tostring(value_opts.collation))
            end
        elseif value_type == 'boolean' then
            func_name = 'lt_boolean' .. postfix
        else
            func_name = 'lt' .. postfix
        end

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

local function gen_code(filter_conditions)
    if #filter_conditions == 0 then
        return { code = 'return true, false', library_code = 'return {}' }
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
        library_code = library_code,
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
    -- nullable
    eq_unicode = eq_unicode_nullable,
    eq_unicode_ci = eq_unicode_ci_nullable,
    -- strict
    eq_unicode_strict = eq_unicode_strict,
    eq_unicode_ci_strict = eq_unicode_ci_strict,

    -- LT
    -- nullable
    lt = lt_nullable,
    lt_unicode_ = lt_unicode_nullable,
    lt_unicode_ci = lt_unicode_ci_nullable,
    lt_boolean = lt_boolean_nullable,
    -- strict
    lt_strict = lt_strict,
    lt_unicode_strict = lt_unicode_strict,
    lt_unicode_ci_strict = lt_unicode_ci_strict,
    lt_boolean_strict = lt_boolean_strict,

    utf8 = utf8,

    -- NULL
    NULL = box.NULL,
}

local function compile(filter)
    local lib, err = load(filter.library_code, 'library', 'bt', library)
    assert(lib, err)
    lib = lib()

    for name, f in pairs(library) do
        lib[name] = f
    end

    local func, err = load(filter.code, 'code', 'bt', lib)
    assert(func, err)
    return func
end

return {
    gen_code = gen_code,
    compile = compile,
}
