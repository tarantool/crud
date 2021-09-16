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
            local customers_schema = {
                engine = engine,
                is_local = true,
                temporary = false,
                format = {
                    {name = 'id', is_nullable = false, type = 'unsigned'},
                    {name = 'bucket_id', is_nullable = false, type = 'unsigned'},
                    {name = 'name', is_nullable = false, type = 'string'},
                    {name = 'age', is_nullable = false, type = 'number'},
                },
                indexes = {
                    -- This table is intentionally blank.
                },
            }

            local primary_index = {
                name = 'id',
                type = 'TREE',
                unique = true,
                parts = {
                    {path = 'id', is_nullable = false, type = 'unsigned'},
                    {path = 'name', is_nullable = false, type = 'string'},
                },
            }
            local primary_index_id = {
                name = 'id',
                type = 'TREE',
                unique = true,
                parts = {
                    {path = 'id', is_nullable = false, type = 'unsigned'},
                },
            }
            local bucket_id_index = {
                name = 'bucket_id',
                type = 'TREE',
                unique = false,
                parts = {
                    {path = 'bucket_id', is_nullable = false, type = 'unsigned'},
                }
            }
            local name_index = {
                name = 'name',
                type = 'TREE',
                unique = true,
                parts = {
                    {path = 'name', is_nullable = false, type = 'string'},
                },
            }
            local secondary_index = {
                name = 'secondary',
                type = 'TREE',
                unique = false,
                parts = {
                    {path = 'id', is_nullable = false, type = 'unsigned'},
                    {path = 'name', is_nullable = false, type = 'string'},
                },
            }

            local customers_name_key_schema = table.deepcopy(customers_schema)
            customers_name_key_schema.sharding_key = {'name'}
            table.insert(customers_name_key_schema.indexes, primary_index)
            table.insert(customers_name_key_schema.indexes, bucket_id_index)

            local customers_name_key_uniq_index_schema = table.deepcopy(customers_schema)
            customers_name_key_uniq_index_schema.sharding_key = {'name'}
            table.insert(customers_name_key_uniq_index_schema.indexes, primary_index)
            table.insert(customers_name_key_uniq_index_schema.indexes, bucket_id_index)
            table.insert(customers_name_key_uniq_index_schema.indexes, name_index)

            local customers_name_key_non_uniq_index_schema = table.deepcopy(customers_schema)
            customers_name_key_non_uniq_index_schema.sharding_key = {'name'}
            name_index.unique = false
            table.insert(customers_name_key_non_uniq_index_schema.indexes, primary_index)
            table.insert(customers_name_key_non_uniq_index_schema.indexes, bucket_id_index)
            table.insert(customers_name_key_non_uniq_index_schema.indexes, name_index)

            local customers_secondary_idx_name_key_schema = table.deepcopy(customers_schema)
            customers_secondary_idx_name_key_schema.sharding_key = {'name'}
            table.insert(customers_secondary_idx_name_key_schema.indexes, primary_index_id)
            table.insert(customers_secondary_idx_name_key_schema.indexes, secondary_index)
            table.insert(customers_secondary_idx_name_key_schema.indexes, bucket_id_index)

            local schema = {
                spaces = {
                    customers_name_key = customers_name_key_schema,
                    customers_name_key_uniq_index = customers_name_key_uniq_index_schema,
                    customers_name_key_non_uniq_index = customers_name_key_non_uniq_index_schema,
                    customers_secondary_idx_name_key = customers_secondary_idx_name_key_schema,
                }
            }

            if not box.info.ro then
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
