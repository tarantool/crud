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

package.preload['customers-storage'] = function()
    -- set sharding func in dot.notation
    -- in _G for sharding func tests
    return {
        role_name = 'customers-storage',
        init = require('storage_init'),
    }
end

local ok, err = errors.pcall('CartridgeCfgError', cartridge.cfg, {
    advertise_uri = 'localhost:3301',
    http_port = 8081,
    bucket_count = 3000,
    roles = {
        'customers-storage',
        'cartridge.roles.crud-router',
        'cartridge.roles.crud-storage',
    }},
    -- Increase readahead for performance tests.
    -- Performance tests on HP ProBook 440 G5 16 Gb
    -- bump into default readahead limit and thus not
    -- give a full picture.
    { readahead = 20 * 1024 * 1024 }
)

if not ok then
    log.error('%s', err)
    os.exit(1)
end

_G.is_initialized = cartridge.is_healthy
