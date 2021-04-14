#!/usr/bin/env tarantool

require('strict').on()
_G.is_initialized = function() return false end

local log = require('log')
local errors = require('errors')
local cartridge = require('cartridge')

local roles_reload_allowed = nil
if not os.getenv('TARANTOOL_FORBID_HOTRELOAD') then
    roles_reload_allowed = true
end

package.preload['customers-storage'] = function()
    return {
        role_name = 'customers-storage',
        init = function()
            local engine = os.getenv('ENGINE') or 'memtx'
            local customers_space = box.schema.space.create('customers', {
                format = {
                    {name = 'id', type = 'unsigned'},
                    {name = 'bucket_id', type = 'unsigned'},
                },
                if_not_exists = true,
                engine = engine,
            })

            customers_space:create_index('id', {
                parts = { {field = 'id'} },
                unique = true,
                type = 'TREE',
                if_not_exists = true,
            })

            customers_space:create_index('bucket_id', {
                parts = { {field = 'bucket_id'} },
                unique = false,
                type = 'TREE',
                if_not_exists = true,
            })
        end,
        dependencies = {'cartridge.roles.crud-storage'}
    }
end

local ok, err = errors.pcall('CartridgeCfgError', cartridge.cfg, {
    advertise_uri = 'localhost:3301',
    http_port = 8081,
    bucket_count = 3000,
    roles = {
        'customers-storage',
        'cartridge.roles.crud-router',
        'cartridge.roles.crud-storage'
    },
    roles_reload_allowed = roles_reload_allowed
})

if not ok then
    log.error('%s', err)
    os.exit(1)
end

_G.is_initialized = cartridge.is_healthy
