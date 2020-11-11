#!/usr/bin/env tarantool

require('strict').on()
_G.is_initialized = function() return false end

local log = require('log')
local errors = require('errors')
local cartridge = require('cartridge')
local crud_utils = require('crud.common.utils')

package.preload['customers-storage'] = function()
    local engine = os.getenv('ENGINE') or 'memtx'
    return {
        role_name = 'customers-storage',
        init = function()
            local customers_space = box.schema.space.create('customers', {
                format = {
                    {name = 'id', type = 'unsigned'},
                    {name = 'bucket_id', type = 'unsigned'},
                    {name = 'name', type = 'string'},
                    {name = 'last_name', type = 'string'},
                    {name = 'age', type = 'number'},
                    {name = 'city', type = 'string'},
                },
                if_not_exists = true,
                engine = engine,
            })
            --primary index
            customers_space:create_index('id_index', {
                parts = { {field = 'id'} },
                if_not_exists = true,
            })
            customers_space:create_index('bucket_id', {
                parts = { {field = 'bucket_id'} },
                unique = false,
                if_not_exists = true,
            })
            customers_space:create_index('age_index', {
                parts = { {field = 'age'} },
                unique = false,
                if_not_exists = true,
            })
            --indexes with same names as fields
            customers_space:create_index('age', {
                parts = { {field = 'age'} },
                unique = false,
                if_not_exists = true,
            })
            customers_space:create_index('id', {
                parts = { {field = 'id'} },
                if_not_exists = true,
            })
            customers_space:create_index('full_name', {
                parts = {
                    { field = 'name', collation = 'unicode_ci' },
                    { field = 'last_name', collation = 'unicode_ci' },
                },
                unique = false,
                if_not_exists = true,
            })

            if crud_utils.tarantool_supports_uuids() then
                local goods_space = box.schema.space.create('goods', {
                    format = {
                        {name = 'uuid', type = 'uuid'},
                        {name = 'bucket_id', type = 'unsigned'},
                        {name = 'name', type = 'string'},
                        {name = 'category_id', type = 'uuid'},
                    },
                    if_not_exists = true,
                    engine = engine,
                })
                --primary index
                goods_space:create_index('uuid', {
                    parts = { {field = 'uuid'} },
                    if_not_exists = true,
                })
                goods_space:create_index('bucket_id', {
                    parts = { {field = 'bucket_id'} },
                    unique = false,
                    if_not_exists = true,
                })
            end
        end,
        dependencies = {'cartridge.roles.crud-storage'},
    }
end

package.preload['customers-router'] = function()
    return {
        role_name = 'customers-router',
        init = function() end,
        dependencies = {'cartridge.roles.crud-router'},
    }
end

local ok, err = errors.pcall('CartridgeCfgError', cartridge.cfg, {
    advertise_uri = 'localhost:3301',
    http_port = 8081,
    bucket_count = 3000,
    roles = {
        'customers-storage',
        'customers-router',
    },
})

if not ok then
    log.error('%s', err)
    os.exit(1)
end

_G.is_initialized = cartridge.is_healthy
