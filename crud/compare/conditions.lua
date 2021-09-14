local errors = require('errors')

local dev_checks = require('crud.common.dev_checks')

local ParseConditionError = errors.new_class('ParseConditionError')

local conditions = {}
conditions.funcs = {}

conditions.operators = {
    EQ = '==',
    LT = '<',
    LE = '<=',
    GT = '>',
    GE = '>=',
}

local tarantool_iter_by_cond_operators = {
    [conditions.operators.EQ] = box.index.EQ,
    [conditions.operators.LT] = box.index.LT,
    [conditions.operators.LE] = box.index.LE,
    [conditions.operators.GT] = box.index.GT,
    [conditions.operators.GE] = box.index.GE,
}

local function new_condition(opts)
    dev_checks({
        operator = 'string',
        operand = 'string|table',
        values = '?',
    })

    local values = opts.values
    if type(values) ~= 'table' then
        values = { values }
    end

    local obj = {
        operator = opts.operator,
        operand = opts.operand,
        values = values,
    }

    return obj
end

function conditions.get_tarantool_iter(condition)
    return tarantool_iter_by_cond_operators[condition.operator]
end

local cond_operators_by_func_names = {
    eq = conditions.operators.EQ,
    lt = conditions.operators.LT,
    le = conditions.operators.LE,
    gt = conditions.operators.GT,
    ge = conditions.operators.GE,
}

for func_name, operator in pairs(cond_operators_by_func_names) do
    assert(operator ~= nil)
    conditions.funcs[func_name] = function(operand, values)
        dev_checks('string|table', '?')
        return new_condition({
            operator = operator,
            operand = operand,
            values = values,
        })
    end
end

local funcs_by_symbols = {
    ['=='] = conditions.funcs.eq,
    ['='] = conditions.funcs.eq,
    ['<'] = conditions.funcs.lt,
    ['<='] = conditions.funcs.le,
    ['>'] = conditions.funcs.gt,
    ['>='] = conditions.funcs.ge,
}

function conditions.parse(user_conditions)
    if user_conditions == nil then
        return nil
    end

    if type(user_conditions) ~= 'table' then
        return nil, ParseConditionError:new("Conditions should be table, got %q", type(user_conditions))
    end

    local parsed_conditions = {}

    for i, user_condition in ipairs(user_conditions) do
        if type(user_condition) ~= 'table' then
            return nil, ParseConditionError:new(
                "Each condition should be table, got %q (condition %s)",
                type(user_condition), i
            )
        end

        if #user_condition > 3 or #user_condition < 2 then
            return nil, ParseConditionError:new(
                'Each condition should be {"<operator>", "<operand>", <value>} (condition %s)', i
            )
        end

        -- operator
        local operator_symbol = user_condition[1]
        if type(operator_symbol) ~= 'string' then
            return nil, ParseConditionError:new(
                "condition[1] should be string, got %q (condition %s)", type(operator_symbol), i
            )
        end

        local cond_func = funcs_by_symbols[operator_symbol]
        if cond_func == nil then
            return nil, ParseConditionError:new(
                "condition[1] %q isn't a valid condition operator, (condition %s)", operator_symbol, i
            )
        end

        -- operand
        local operand = user_condition[2]
        if type(operand) ~= 'string' then
            return nil, ParseConditionError:new(
                "condition[2] should be string, got %q (condition %s)", type(operand), i
            )
        end

        local value = user_condition[3]

        table.insert(parsed_conditions, cond_func(operand, value))
    end

    return parsed_conditions
end

return conditions
