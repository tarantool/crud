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
        init = function()
            rawset(_G, 'create_space', function()
                local customers_space = box.schema.space.create('customers', {
                    format = {
                        {name = 'id', type = 'unsigned'},
                        {name = 'bucket_id', type = 'unsigned'},
                        {name = 'value', type = 'string'},
                        {name = 'number', type = 'integer', is_nullable = true},
                    },
                    if_not_exists = true,
                    engine = engine,
                })

                -- primary index
                customers_space:create_index('id_index', {
                    parts = { {field = 'id'} },
                    if_not_exists = true,
                })
            end)

            rawset(_G, 'create_bucket_id_index', function()
                box.space.customers:create_index('bucket_id', {
                    parts = { {field = 'bucket_id'} },
                    if_not_exists = true,
                    unique = false,
                })
            end)

            rawset(_G, 'set_value_type_to_unsigned', function()
                local new_format = {}

                for _, field_format in ipairs(box.space.customers:format()) do
                    if field_format.name == 'value' then
                        field_format.type = 'unsigned'
                    end
                    table.insert(new_format, field_format)
                end

                box.space.customers:format(new_format)
            end)

            rawset(_G, 'add_extra_field', function()
                local new_format = box.space.customers:format()
                table.insert(new_format, {name = 'extra', type = 'string', is_nullable = true})
                box.space.customers:format(new_format)
            end)

            rawset(_G, 'add_value_index', function()
                box.space.customers:create_index('value_index', {
                    parts = { {field = 'value'} },
                    if_not_exists = true,
                    unique = false,
                })
            end)

            rawset(_G, 'create_number_value_index', function()
                box.space.customers:create_index('number_value_index', {
                    parts = { {field = 'number'}, {field = 'value'} },
                    if_not_exists = true,
                    unique = false,
                })
            end)

            rawset(_G, 'alter_number_value_index', function()
                box.space.customers.index.number_value_index:alter({
                    parts = { {field = 'value'}, {field = 'number'} },
                })
            end)
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
