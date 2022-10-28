#!/usr/bin/env tarantool

require('strict').on()
_G.is_initialized = function() return false end

local log = require('log')
local errors = require('errors')
local cartridge = require('cartridge')

package.preload['customers-storage'] = function()
    return {
        role_name = 'customers-storage',
        init = function()
            local engine = os.getenv('ENGINE') or 'memtx'
            local customers_space = box.schema.space.create('customers', {
                format = {
                    {name = 'id', type = 'unsigned'},
                    {name = 'bucket_id', type = 'unsigned'},
                    {name = 'name', type = 'string'},
                    {name = 'age', type = 'number'},
                },
                if_not_exists = true,
                engine = engine,
            })
            customers_space:create_index('id', {
                parts = { {field = 'id'} },
                if_not_exists = true,
            })
            customers_space:create_index('bucket_id', {
                parts = { {field = 'bucket_id'} },
                unique = false,
                if_not_exists = true,
            })

            local developers_space = box.schema.space.create('developers', {
                format = {
                    {name = 'id', type = 'unsigned'},
                    {name = 'bucket_id', type = 'unsigned'},
                },
                if_not_exists = true,
                engine = engine,
            })
            developers_space:create_index('id', {
                parts = { {field = 'id'} },
                if_not_exists = true,
            })
            developers_space:create_index('bucket_id', {
                parts = { {field = 'bucket_id'} },
                unique = false,
                if_not_exists = true,
            })

            rawset(_G, 'add_extra_field', function(name)
                local new_format = box.space.developers:format()
                table.insert(new_format, {name = name, type = 'any', is_nullable = true})
                box.space.developers:format(new_format)
            end)

            -- Space with huge amount of nullable fields
            -- an object that inserted in such space should get
            -- explicit nulls in absence fields otherwise
            -- Tarantool serializers could consider such object as map (not array).
            local tags_space = box.schema.space.create('tags', {
                format = {
                    {name = 'id', type = 'unsigned'},
                    {name = 'bucket_id', type = 'unsigned'},
                    {name = 'is_red', type = 'boolean', is_nullable = true},
                    {name = 'is_green', type = 'boolean', is_nullable = true},
                    {name = 'is_blue', type = 'boolean', is_nullable = true},
                    {name = 'is_yellow', type = 'boolean', is_nullable = true},
                    {name = 'is_sweet', type = 'boolean', is_nullable = true},
                    {name = 'is_dirty', type = 'boolean', is_nullable = true},
                    {name = 'is_long', type = 'boolean', is_nullable = true},
                    {name = 'is_short', type = 'boolean', is_nullable = true},
                    {name = 'is_useful', type = 'boolean', is_nullable = true},
                    {name = 'is_correct', type = 'boolean', is_nullable = true},
                },
                if_not_exists = true,
                engine = engine,
            })

            tags_space:create_index('id', {
                parts = { {field = 'id'} },
                if_not_exists = true,
            })
            tags_space:create_index('bucket_id', {
                parts = { {field = 'bucket_id'} },
                unique = false,
                if_not_exists = true,
            })

            local sequence_space = box.schema.space.create('notebook', {
                format = {
                    {name = 'local_id', type = 'unsigned', is_nullable = false},
                    {name = 'bucket_id', type = 'unsigned', is_nullable = false},
                    {name = 'record', type = 'string', is_nullable = false},
                },
                if_not_exists = true,
                engine = engine,
            })

            box.schema.sequence.create('local_id', {if_not_exists = true})

            sequence_space:create_index('local_id', {
                parts = { {field = 'local_id'} },
                unique = true,
                if_not_exists = true,
                sequence = 'local_id',
            })
            sequence_space:create_index('bucket_id', {
                parts = { {field = 'bucket_id'} },
                unique = false,
                if_not_exists = true,
            })
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
    },
})

if not ok then
    log.error('%s', err)
    os.exit(1)
end

_G.is_initialized = cartridge.is_healthy
