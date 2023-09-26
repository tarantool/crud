#!/usr/bin/env tarantool

require('strict').on()
_G.is_initialized = function() return false end

local log = require('log')
local errors = require('errors')
local cartridge = require('cartridge')

local crud_utils = require('crud.common.utils')

if package.setsearchroot ~= nil then
    package.setsearchroot()
else
    package.path = package.path .. debug.sourcedir() .. "/?.lua;"
end

package.preload['customers-storage'] = function()
    return {
        role_name = 'customers-storage',
        init = require('storage_init'),
    }
end

local roles_reload_allowed = nil
if crud_utils.is_cartridge_hotreload_supported() then
    roles_reload_allowed = true
end

local is_metrics = pcall(require, 'metrics')
local roles = {
    'cartridge.roles.crud-router',
    'cartridge.roles.crud-storage',
    'customers-storage',
}
if is_metrics then
    table.insert(roles, 'cartridge.roles.metrics')
end

local ok, err = errors.pcall('CartridgeCfgError', cartridge.cfg, {
    advertise_uri = 'localhost:3301',
    http_port = 8081,
    bucket_count = 3000,
    roles = roles,
    roles_reload_allowed = roles_reload_allowed,
})

if not ok then
    log.error('%s', err)
    os.exit(1)
end

_G.is_initialized = cartridge.is_healthy
