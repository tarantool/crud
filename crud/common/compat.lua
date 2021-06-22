local log = require('log')

local compat = {}

function compat.require(module_name, builtin_module_name)
    local module_cached_name = string.format('__crud_%s_cached', module_name)

    local module

    local module_cached = rawget(_G, module_cached_name)
    if module_cached ~= nil then
        module = module_cached
    elseif package.search(module_name) then
        -- we don't use pcall(require, module_name) here because it
        -- leads to ignoring errors other than 'No LuaRocks module found'
        log.info('%q module is used', module_name)
        module = require(module_name)
    else
        log.info('%q module is not found. Built-in %q is used', module_name, builtin_module_name)
        module = require(builtin_module_name)
    end

    rawset(_G, module_cached_name, module)

    return module
end

function compat.exists(module_name, builtin_module_name)
    local module_cached = rawget(_G, string.format('__crud_%s_cached', module_name))
    if module_cached ~= nil then
        return true
    end

    if package.search(module_name) then
        return true
    end

    if package.loaded[builtin_module_name] ~= nil then
        return true
    end

    return false
end

return compat
