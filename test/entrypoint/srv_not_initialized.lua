#!/usr/bin/env tarantool

require('strict').on()
_G.is_initialized = function() return false end

local log = require('log')
local errors = require('errors')
local cartridge = require('cartridge')
local elect = require('elect')

local ok, err

ok, err = errors.pcall('CartridgeCfgError', cartridge.cfg, {
    advertise_uri = 'localhost:3301',
    http_port = 8081,
    bucket_count = 3000,
    roles = {
        'cartridge.roles.vshard-storage',
        'cartridge.roles.vshard-router',
    },
})

if not ok then
    log.error('%s', err)
    os.exit(1)
end

-- elect.init() isn't called

ok, err = elect.register({
    say_hi = function() return "Hi" end,
})

if not ok then
    log.error('%s', err)
    os.exit(1)
end


_G.is_initialized = cartridge.is_healthy
