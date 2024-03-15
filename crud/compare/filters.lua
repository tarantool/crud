local errors = require('errors')

local _, datetime = pcall(require, 'datetime')
local _, decimal = pcall(require, 'decimal')

local utils = require('crud.common.utils')
local dev_checks = require('crud.common.dev_checks')
local collations = require('crud.common.collations')
local compare_conditions = require('crud.compare.conditions')

local GenFiltersError = errors.new_class('GenFiltersError', {capture_stack = false})

local filters = {}

--[[
    Function returns true if main query iteration can be stopped by fired opposite condition.

    For e.g.
    - iteration goes using `'id' > 10`
    - opposite condition `'id' < 100` becomes false
    - in such case we can exit from iteration
]]
local function is_early_exit_possible(scan_index, tarantool_iter, condition)
    if scan_index.name ~= condition.operand then
        return false
    end

    local condition_iter = compare_conditions.get_tarantool_iter(condition)
    if tarantool_iter == box.index.REQ or tarantool_iter == box.index.LT or tarantool_iter == box.index.LE then
        if condition_iter == box.index.GT or condition_iter == box.index.GE then
            return true
        end
    elseif tarantool_iter == box.index.EQ or tarantool_iter == box.index.GT or tarantool_iter == box.index.GE then
        if condition_iter == box.index.LT or condition_iter == box.index.LE then
            return true
        end
    end

    return false
end

local function get_index_fieldnos(index)
    local index_fieldnos = {}

    for _, part in ipairs(index.parts) do
        if part.path ~= nil then
            table.insert(index_fieldnos, string.format("[%d]%s", part.fieldno, part.path))
        else
            table.insert(index_fieldnos, part.fieldno)
        end
    end

    return index_fieldnos
end

local function get_index_fields_types(index)
    local index_fields_types = {}

    for _, part in ipairs(index.parts) do
        table.insert(index_fields_types, part.type)
    end

    return index_fields_types
end

local function get_values_opts(index)
    local index_field_opts = {}

    for _, part in ipairs(index.parts) do
        table.insert(index_field_opts, {
            is_nullable = part.is_nullable == true,
            collation = collations.get(part)
        })
    end

    return index_field_opts
end

local function parse(space, scan_index, conditions, opts)
    dev_checks('table', 'table', '?table', {
        scan_condition_num = '?number',
        tarantool_iter = 'number',
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
            local fields
            local fields_types = {}
            local values_opts

            local index = space_indexes[condition.operand]

            if index ~= nil then
                fields = get_index_fieldnos(index)
                fields_types = get_index_fields_types(index)
                values_opts = get_values_opts(index)
            else
                local fieldno = fieldnos_by_names[condition.operand]

                if fieldno ~= nil then
                    fields = {fieldno}
                else
                    -- We assume this is jsonpath, so it is
                    -- not in fieldnos_by_name map.
                    fields = {condition.operand}
                end

                local field_format = space_format[fieldno]
                local is_nullable

                if field_format ~= nil then
                    fields_types = {field_format.type}
                    is_nullable = field_format.is_nullable == true
                end

                values_opts = {
                    {is_nullable = is_nullable, collation = nil},
                }
            end

            table.insert(filter_conditions, {
                fields = fields,
                operator = condition.operator,
                values = condition.values,
                types = fields_types,
                early_exit_is_possible = is_early_exit_possible(
                    scan_index,
                    opts.tarantool_iter,
                    condition
                ),
                values_opts = values_opts,
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
    elseif utils.tarantool_supports_decimals() and decimal.is_decimal(value) then
        -- decimal supports comparison with string.
        return ("%q"):format(tostring(value))
    elseif utils.is_uuid(value) then
        return ("%q"):format(value)
    elseif utils.tarantool_supports_datetimes() and datetime.is_datetime(value) then
        return ("%q"):format(value:format())
    elseif utils.is_interval(value) then
        -- As for Tarantool 3.0 and older, datetime intervals
        -- are not comparable. It's better to explicitly forbid them
        -- for now.
        -- https://github.com/tarantool/tarantool/issues/7659
        GenFiltersError:assert(false, ('datetime interval conditions are not supported'))
    elseif type(value) == 'cdata' then
        return tostring(value)
    end
    GenFiltersError:assert(false, ('Unexpected value %s (type %s)'):format(value, type(value)))
end

local PARSE_ARGS_TEMPLATE = 'local tuple = ...'
local LIB_FUNC_HEADER_TEMPLATE = 'function M.%s(%s)'

local function format_path(path)
    local path_type = type(path)
    if path_type == 'number' then
        return tostring(path)
    elseif path_type == 'string' then
        return ('%q'):format(path)
    end

    assert(false, ('Unexpected format: %s'):format(path_type))
end

local function concat_conditions(conditions, operator)
    return '(' .. table.concat(conditions, (' %s '):format(operator)) .. ')'
end

local function get_field_variable_name(field)
    local field_type = type(field)
    if field_type == 'number' then
        field = tostring(field)
    elseif field_type == 'string' then
        field = string.gsub(field, '([().^$%[%]%+%-%*%?%%\'"])', '_')
    end

    return string.format('field_%s', field)
end

local function get_eq_func_name(id)
    return string.format('eq_%s', id)
end

local function get_cmp_func_name(id)
    return string.format('cmp_%s', id)
end

local function gen_tuple_fields_def_code(filter_conditions)
    -- get field names
    local fields_added = {}
    local fields = {}

    for _, cond in ipairs(filter_conditions) do
        for i = 1, #cond.values do
            local field = cond.fields[i]

            if not fields_added[field] then
                table.insert(fields, field)
                fields_added[field] = true
            end
        end
    end

    -- gen definitions for all used fields
    local fields_def_parts = {}

    for _, field in ipairs(fields) do
        table.insert(fields_def_parts, string.format(
            'local %s = tuple[%s]',
            get_field_variable_name(field), format_path(field)
        ))
    end

    return table.concat(fields_def_parts, '\n')
end

local function format_comp_with_value(field, func_name, value)
    return string.format(
        '%s(%s, %s)',
        func_name,
        get_field_variable_name(field),
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
        local field = cond.fields[j]
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
        elseif value_type == 'datetime' then
            func_name = 'eq_datetime'
        end

        table.insert(cond_strings, format_comp_with_value(field, func_name, value))
    end

    return cond_strings
end

local function format_lt(cond)
    local cond_strings = {}
    local values_opts = cond.values_opts or {}

    for j = 1, #cond.values do
        local field = cond.fields[j]
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
        elseif value_type == 'datetime' then
            func_name = 'lt_datetime'
        end

        func_name = add_strict_postfix(func_name, value_opts)

        table.insert(cond_strings, format_comp_with_value(field, func_name, value))
    end

    return cond_strings
end

local function gen_eq_func_code(func_name, cond, func_args_code)
    local func_code_lines = {}

    local eq_conds = format_eq(cond)

    local header = LIB_FUNC_HEADER_TEMPLATE:format(func_name, func_args_code)
    table.insert(func_code_lines, header)

    local return_line
    if #eq_conds > 0 then
        return_line = string.format(
            '    return %s', concat_conditions(eq_conds, 'and')
        )
    else  -- nil condition is treated as no condition
        return_line = '    return true'
    end
    table.insert(func_code_lines, return_line)

    table.insert(func_code_lines, 'end')

    return table.concat(func_code_lines, '\n')
end

local results_by_operators = {
    [compare_conditions.operators.LT] = {
        le = true, not_eq = false, default = false,
    },
    [compare_conditions.operators.LE] = {
        le = true, not_eq = false, default = true,
    },
    [compare_conditions.operators.GT] = {
        le = false, not_eq = true, default = false,
    },
    [compare_conditions.operators.GE] = {
        le = false, not_eq = true, default = true,
    },
}

--[[
    This function generates code of function that compares arrays of values.
    All conditions are presented using `lt`, `eq` and `not` operations.

    Values are compared one by one, for each value two checkers are performed:
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

    if #eq_conds > 0 then
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
    else  -- nil condition is treated as no condition
        local return_line = '    return true'
        table.insert(func_code_lines, return_line)
    end

    table.insert(func_code_lines, 'end')

    return table.concat(func_code_lines, '\n')
end

local function function_args_by_field(fields)
    local arg_names = {}
    for _, field in ipairs(fields) do
        table.insert(arg_names, get_field_variable_name(field))
    end
    return table.concat(arg_names, ', ')
end

local function gen_library_func(id, cond, func_args_code)
    local library_func_code, library_func_name

    if cond.operator == compare_conditions.operators.EQ then
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
        local args_fields = { unpack(cond.fields, 1, #cond.values) }
        local func_args_code = function_args_by_field(args_fields)

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

local function opt_datetime_parse(v)
    if type(v) == 'string' then
        return datetime.parse(v)
    end

    return v
end

local function lt_datetime_nullable(lhs, rhs)
    if lhs == nil and rhs ~= nil then
        return true
    elseif rhs == nil then
        return false
    end
    return opt_datetime_parse(lhs) < opt_datetime_parse(rhs)
end

local function lt_datetime_strict(lhs, rhs)
    if rhs == nil then
        return false
    end
    return opt_datetime_parse(lhs) < opt_datetime_parse(rhs)
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

local function eq_datetime(lhs, rhs)
    if lhs == nil then
        return rhs == nil
    end
    return opt_datetime_parse(lhs) == opt_datetime_parse(rhs)
end

local function eq_datetime_strict(lhs, rhs)
    if rhs == nil then
        return false
    end
    return opt_datetime_parse(lhs) == opt_datetime_parse(rhs)
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
    eq_datetime = eq_datetime,
    eq_datetime_strict = eq_datetime_strict,
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
    lt_datetime = lt_datetime_nullable,
    -- strict
    lt_strict = lt_strict,
    lt_unicode_strict = lt_unicode_strict,
    lt_unicode_ci_strict = lt_unicode_ci_strict,
    lt_boolean_strict = lt_boolean_strict,
    lt_uuid_strict = lt_uuid_strict,
    lt_datetime_strict = lt_datetime_strict,

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

function filters.gen_func(space, scan_index, conditions, opts)
    dev_checks('table', 'table', '?table', {
        tarantool_iter = 'number',
        scan_condition_num = '?number',
    })

    local filter_conditions, err = parse(space, scan_index, conditions, {
        scan_condition_num = opts.scan_condition_num,
        tarantool_iter = opts.tarantool_iter,
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
}

return filters
