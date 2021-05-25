local log = require('log')

local compat = {}

function compat.require(module_name, builtin_module_name)
    local module_cached_name = string.format('__crud_%s_cached', module_name)

    local module

    local module_cached = rawget(_G, module_cached_name)
    if module_cached ~= nil then
        module = module_cached
    elseif package.search(module_name) then
        log.info('%q module is used', module_name)
        module = require(module_name)
    else
        log.info('%q module is not found. Built-in %q is used', module_name, builtin_module_name)
        module = require(builtin_module_name)
    end

    rawset(_G, module_cached_name, module)

    if package.search('cartridge.hotreload') ~= nil then
        local hotreload = require('cartridge.hotreload')
        hotreload.whitelist_globals({module_cached_name})
    end

    return module
end

return compat
