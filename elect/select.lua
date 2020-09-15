local errors = require('errors')

require('elect.common.checkers')

local SelectError = errors.new_class('Select', {capture_stack = false})

-- local DEFAULT_BATCH_SIZE = 10

local select_module = {}

function select_module.init()

end

function select_module.call()
    return nil, SelectError:new("Select is not implemenetd yet")
end

return select_module
