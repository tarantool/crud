local uuid = require('uuid')

local state = {}

local states = {}

function state.get(id)
    id = id or uuid.str()
    local op_state = states[id]
    if op_state == nil then
        op_state = { id = id }
        states[id] = op_state
    end
    return op_state
end

function state.clear(id)
    assert(id ~= nil)
    states[id] = nil
    return true
end

return state
