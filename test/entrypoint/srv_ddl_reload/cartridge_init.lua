#!/usr/bin/env tarantool

require('strict').on()
_G.is_initialized = function() return false end

local fio = require('fio')
local log = require('log')
local errors = require('errors')
local cartridge = require('cartridge')

local crud_utils = require('crud.common.utils')

if package.setsearchroot ~= nil then
    package.setsearchroot()
else
    package.path = package.path .. debug.sourcedir() .. "/?.lua;"
end

local root = fio.dirname(fio.dirname(fio.dirname(debug.sourcedir())))
package.path = package.path .. root .. "/?.lua;"

package.preload['customers-storage'] = function()
    local customers_module = {
        sharding_func_default = function(key)
            local id = key[1]
            assert(id ~= nil)

            return id % 3000 + 1
        end,
        sharding_func_new = function(key)
            local id = key[1]
            assert(id ~= nil)

            return (id + 42) % 3000 + 1
        end,
    }
    rawset(_G, 'customers_module', customers_module)

    return {
        role_name = 'customers-storage',
        init = require('storage').init,
    }
end

local roles_reload_allowed = nil
if crud_utils.is_cartridge_hotreload_supported() then
    roles_reload_allowed = true
end

local ok, err = errors.pcall('CartridgeCfgError', cartridge.cfg, {
    advertise_uri = 'localhost:3301',
    http_port = 8081,
    bucket_count = 3000,
    roles = {
        'customers-storage',
        'cartridge.roles.crud-router',
        'cartridge.roles.crud-storage',
    },
    roles_reload_allowed = roles_reload_allowed,
})

if not ok then
    log.error('%s', err)
    os.exit(1)
end

_G.is_initialized = cartridge.is_healthy
