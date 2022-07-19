#!/usr/bin/env tarantool

require('strict').on()
_G.is_initialized = function() return false end

local log = require('log')
local errors = require('errors')
local cartridge = require('cartridge')

package.preload['customers-storage'] = function()
    return {
        role_name = 'customers-storage',
        init = function(opts)
            if opts.is_master then
                box.schema.space.create('customers')

                box.space['customers']:format{
                    {name = 'id',           is_nullable = false, type = 'unsigned'},
                    {name = 'bucket_id',    is_nullable = false, type = 'unsigned'},
                    {name = 'sharding_key', is_nullable = false, type = 'unsigned'},
                }

                box.space['customers']:create_index('pk',        {parts = { 'id' }})
                box.space['customers']:create_index('bucket_id', {parts = { 'bucket_id' }})
            end
        end,
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
    }}
)

if not ok then
    log.error('%s', err)
    os.exit(1)
end

_G.is_initialized = cartridge.is_healthy
