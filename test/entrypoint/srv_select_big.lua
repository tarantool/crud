#!/usr/bin/env tarantool

require('strict').on()
_G.is_initialized = function() return false end

local log = require('log')
local errors = require('errors')
local cartridge = require('cartridge')

package.preload['values-storage'] = function()
    return {
        role_name = 'values-storage',
        init = function()
            local values_space = box.schema.space.create('values', {
                format = {
                    {name = 'key', type = 'unsigned'},
                    {name = 'bucket_id', type = 'unsigned'},
                    {name = 'value', type = 'unsigned'},
                },
                if_not_exists = true,
            })
            values_space:create_index('key', {
                parts = { {field = 'key'} },
                if_not_exists = true,
            })
            values_space:create_index('bucket_id', {
                parts = { {field = 'bucket_id'} },
                unique = false,
                if_not_exists = true,
            })
            values_space:create_index('value', {
                parts = { {field = 'value'} },
                unique = false,
                if_not_exists = true,
            })
        end,
        dependencies = {'cartridge.roles.crud-storage'},
    }
end

local ok, err = errors.pcall('CartridgeCfgError', cartridge.cfg, {
    advertise_uri = 'localhost:3301',
    http_port = 8081,
    bucket_count = 3000,
    roles = {
        'cartridge.roles.vshard-router',
        'values-storage',
    },
})

if not ok then
    log.error('%s', err)
    os.exit(1)
end

_G.is_initialized = cartridge.is_healthy
