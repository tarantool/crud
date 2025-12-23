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
    package.path = package.path .. debug.sourcedir() .. "/?.lua;" .. debug.sourcedir() .. "/?/init.lua;"
end

local root = fio.dirname(fio.dirname(fio.dirname(debug.sourcedir())))
package.path = package.path .. root .. "/?.lua;" .. root .. "/?/init.lua;"

package.preload['customers-storage'] = function()
    return {
        role_name = 'customers-storage',
        init = require('storage').init,
    }
end

local box_opts = {
    readahead = 10 * 1024 * 1024,
}
local ok, err = errors.pcall('CartridgeCfgError', cartridge.cfg, {
    advertise_uri = 'localhost:3301',
    http_port = 8081,
    bucket_count = 3000,
    roles = {
        'cartridge.roles.crud-router',
        'cartridge.roles.crud-storage',
        'customers-storage',
    }},
    box_opts
)

if not ok then
    log.error('%s', err)
    os.exit(1)
end

_G.is_initialized = cartridge.is_healthy
