#!/usr/bin/env tarantool

require('strict').on()
_G.is_initialized = function() return false end

local log = require('log')
local errors = require('errors')
local cartridge = require('cartridge')
local ddl = require('ddl')

package.preload['customers-storage'] = function()
    return {
        role_name = 'customers-storage',
        init = function()
            local engine = os.getenv('ENGINE') or 'memtx'
            local schema = {
                spaces = {
                    customers = {
                        engine = engine,
                        is_local = true,
                        temporary = false,
                        format = {
                            {name = 'id', is_nullable = false, type = 'unsigned'},
                            {name = 'bucket_id', is_nullable = false, type = 'unsigned'},
                            {name = 'name', is_nullable = false, type = 'string'},
                            {name = 'age', is_nullable = false, type = 'number'},
                        },
                        indexes = {{
                            name = 'id',
                            type = 'TREE',
                            unique = true,
                            parts = {
                                {path = 'id', is_nullable = false, type = 'unsigned'},
                            },
                        }, {
                            name = 'bucket_id',
                            type = 'TREE',
                            unique = false,
                            parts = {
                                {path = 'bucket_id', is_nullable = false, type = 'unsigned'},
                            }
                        }},
                        sharding_key = {'id', 'name'},
                    },
                }
            }

            rawset(_G, 'add_extra_field', function(name)
                local new_format = box.space.developers:format()
                table.insert(new_format, {name = name, type = 'any', is_nullable = true})
                box.space.developers:format(new_format)
            end)

            if not box.cfg.read_only then
                local ok, err = ddl.set_schema(schema)
                if not ok then
                    error(err)
                end
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
    },
})

if not ok then
    log.error('%s', err)
    os.exit(1)
end

_G.is_initialized = cartridge.is_healthy
