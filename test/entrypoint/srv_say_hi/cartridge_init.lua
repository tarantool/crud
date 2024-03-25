#!/usr/bin/env tarantool

require('strict').on()
_G.is_initialized = function() return false end

local fio = require('fio')
local log = require('log')
local errors = require('errors')
local cartridge = require('cartridge')

if package.setsearchroot ~= nil then
    package.setsearchroot()
else
    package.path = package.path .. debug.sourcedir() .. "/?.lua;"
end

local root = fio.dirname(fio.dirname(fio.dirname(debug.sourcedir())))
package.path = package.path .. root .. "/?.lua;"

package.preload['customers-storage'] = function()
    return {
        role_name = 'customers-storage',
    }
end

local ok, err = errors.pcall('CartridgeCfgError', cartridge.cfg, {
    advertise_uri = 'localhost:3301',
    http_port = 8081,
    bucket_count = 3000,
    roles = {
        'cartridge.roles.crud-storage',
        'cartridge.roles.crud-router',
        'customers-storage',
    },
})

if not ok then
    log.error('%s', err)
    os.exit(1)
end

require('all').init()

_G.is_initialized = cartridge.is_healthy
