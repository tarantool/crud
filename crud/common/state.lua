local checks = require('checks')
local uuid = require('uuid')

local state = {}

local operations = {}

function state.get(id)
    checks('?string')
    id = id or uuid.str()
    local op_state = operations[id]
    if op_state == nil then
        op_state = {}
        op_state.id = id
        operations[op_state.id] = op_state
    end
    return op_state
end

function state.clear(id)
    checks('string')
    operations[id] = nil
    return true
end

return state
