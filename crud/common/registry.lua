local errors = require('errors')

local registry = {}

local registered_funcs = {}

local RegisterError = errors.new_class('Register')

--- Adds function to the functions registry
--
-- @function add
--
-- @tparam table funcs Functions to be add.
-- Should be passed as a {string: function} map.
--
function registry.add(funcs)
    for func_name in pairs(funcs) do
        if registry.is_registered(func_name) then
            return nil, RegisterError:new("Function %s is already registered", func_name)
        end
    end

    for func_name, func in pairs(funcs) do
        registered_funcs[func_name] = func
    end

    return true
end

function registry.get(func_name)
    return registered_funcs[func_name]
end

function registry.is_registered(func_name)
    return registry.get(func_name) ~= nil
end

function registry.clean()
    registered_funcs = {}
end

return registry
