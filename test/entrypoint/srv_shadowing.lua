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

-- initialize elect
elect.init()

rawset(_G, "global_func", function() return "global" end)
rawset(_G, "common_func", function() return "common-global" end)

ok, err = elect.register({
    registered_func = function() return "registered" end,
    common_func = function() return "common-registered" end,
})

if not ok then
    log.error('%s', err)
    os.exit(1)
end


_G.is_initialized = cartridge.is_healthy
