#!/usr/bin/env tarantool

require('strict').on()
_G.is_initialized = function() return false end

local log = require('log')
local errors = require('errors')
local cartridge = require('cartridge')

package.preload['customers-storage'] = function()
    local engine = os.getenv('ENGINE') or 'memtx'
    return {
        role_name = 'customers-storage',
        init = function(opts)
            if not opts.is_master then
                return
            end
            -- authTemplates
            local authTemplates_space = box.schema.space.create('authTemplates', {
                engine = engine,
                if_not_exists = true,
            })
            log.info('authTemplates space was configured')

            authTemplates_space:format({
                {name='msisdn',type='string'},
                {name='channel',type='string'},
                {name='password',type='string'},
                {name='counter',type='unsigned'},
                {name='create_date',type='unsigned'},
                {name='ttl',type='unsigned'},
                {name='bucket_id', type='unsigned'},
            })
            log.info('authTemplates was formatted')

            authTemplates_space:create_index('authTemplates_msisdn_channel_idx',
                {parts={{field='msisdn'}, {field='channel'}},
                type = 'TREE',
                if_not_exists=true})
            log.info('authTemplates_msisdn_channel_idx')

            authTemplates_space:create_index('bucket_id', {parts={{field='bucket_id'}},
                unique=false,
                if_not_exists=true})
            log.info('authTemplates_bucket_id')

            authTemplates_space:create_index('ttl', {parts={{field='ttl'}},
                unique=false,
                if_not_exists=true})
            log.info('authTemplates_ttl')
        end,
    }
end

local ok, err = errors.pcall('CartridgeCfgError', cartridge.cfg, {
    advertise_uri = 'localhost:3301',
    http_port = 8081,
    bucket_count = 3000,
    roles = {
        'cartridge.roles.crud-router',
        'cartridge.roles.crud-storage',
        'customers-storage',
    },
})

if not ok then
    log.error('%s', err)
    os.exit(1)
end

_G.is_initialized = cartridge.is_healthy
