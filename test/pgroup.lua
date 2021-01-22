local t = require('luatest')

local checks = require('checks')

local ParametrizedGroup = {}
ParametrizedGroup.__index = ParametrizedGroup

local pgroup = {}

function pgroup.new(name, params)
    -- params = { param_name = {'value-1', 'value-2'} }
    checks('string', 'table')

    local obj = {}

    obj.groups = {}

    local params_num = 0
    for param_name, param_values in pairs(params) do
        if params_num > 0 then
            error("Only one parameter is supported now")
        end

        for _, param_value in ipairs(param_values) do
            local group_name = string.format('%s[%s]', name, param_value)
            local group = t.group(group_name)

            group.params = group.params or {}

            group.params[param_name] = param_value

            table.insert(obj.groups, group)
        end

        params_num = params_num + 1
    end

    setmetatable(obj, ParametrizedGroup)
    return obj
end

local function set_hook(pgroup, hook_name, hook)
    checks('table', 'string', 'function')

    for _, g in ipairs(pgroup.groups) do
        g[hook_name](function() hook(g) end)
    end
end

function ParametrizedGroup:set_before_all(hook)
    checks('table', 'function')
    set_hook(self, 'before_all', hook)
end

function ParametrizedGroup:set_after_all(hook)
    checks('table', 'function')
    set_hook(self, 'after_all', hook)
end

function ParametrizedGroup:set_before_each(hook)
    checks('table', 'function')
    set_hook(self, 'before_each', hook)
end

function ParametrizedGroup:set_after_each(hook)
    checks('table', 'function')
    set_hook(self, 'after_each', hook)
end

function ParametrizedGroup:add(name, fn)
    checks('table', 'string', 'function')

    for _, g in ipairs(self.groups) do
        g[name] = function() fn(g) end
    end
end

return pgroup
