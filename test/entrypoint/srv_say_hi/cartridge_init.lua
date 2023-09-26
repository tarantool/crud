#!/usr/bin/env tarantool

require('strict').on()
_G.is_initialized = function() return false end

local log = require('log')
local errors = require('errors')
local cartridge = require('cartridge')

if package.setsearchroot ~= nil then
    package.setsearchroot()
else
    package.path = package.path .. debug.sourcedir() .. "/?.lua;"
end

local ok, err

ok, err = errors.pcall('CartridgeCfgError', cartridge.cfg, {
    advertise_uri = 'localhost:3301',
    http_port = 8081,
    bucket_count = 3000,
    roles = {
        'cartridge.roles.crud-storage',
        'cartridge.roles.crud-router',
    },
})

if not ok then
    log.error('%s', err)
    os.exit(1)
end

require('all_init')()

_G.is_initialized = cartridge.is_healthy
