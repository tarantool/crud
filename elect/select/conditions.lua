local checks = require('checks')

local conditions = {}
conditions.funcs = {}

local Condition = {}
Condition.__index = Condition
Condition.__type = 'condition'

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

function _G.checkers.condition_operator(p)
    for _, op in pairs(conditions.operators) do
        if op == p then
            return true
        end
    end
    return false
end

function Condition.new(opts)
    checks({
        operator = 'condition_operator',
        operand = 'string|strings_array',
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

    setmetatable(obj, Condition)

    return obj
end

function Condition:get_tarantool_iter()
    return tarantool_iter_by_cond_operators[self.operator]
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
        checks('string|strings_array', '?')
        return Condition.new({
            operator = operator,
            operand = operand,
            values = values
        })
    end
end

return conditions
