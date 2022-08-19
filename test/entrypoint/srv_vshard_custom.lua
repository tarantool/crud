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
        init = function(opts)
            local engine = os.getenv('ENGINE') or 'memtx'

            if opts.is_master then
                local customers_space = box.schema.space.create('customers', {
                    format = {
                        {name = 'id', is_nullable = false, type = 'unsigned'},
                        {name = 'bucket_id', is_nullable = false, type = 'unsigned'},
                        {name = 'name', is_nullable = false, type = 'string'},
                        {name = 'age', is_nullable = false, type = 'number'},
                    },
                    if_not_exists = true,
                    engine = engine,
                    id = 542,
                })

                customers_space:create_index('pk', {
                    parts = { {field = 'id'} },
                    if_not_exists = true,
                    unique = true,
                })
                customers_space:create_index('bucket_id', {
                    parts = { {field = 'bucket_id'} },
                    unique = false,
                    if_not_exists = true,
                })
                customers_space:create_index('age', {
                    parts = { {field = 'age'} },
                    unique = false,
                    if_not_exists = true,
                })
            end
        end,
        dependencies = { 'cartridge.roles.crud-storage' }
    }
end

package.preload['customers-storage-ddl'] = function()
    return {
        role_name = 'customers-storage-ddl',
        init = function()
            local engine = os.getenv('ENGINE') or 'memtx'

            local customers_schema = {
                engine = engine,
                temporary = false,
                is_local = false,
                format = {
                    {name = 'id', is_nullable = false, type = 'unsigned'},
                    {name = 'bucket_id', is_nullable = false, type = 'unsigned'},
                    {name = 'name', is_nullable = false, type = 'string'},
                    {name = 'age', is_nullable = false, type = 'number'},
                },
                indexes = {
                    {
                        name = 'pk',
                        type = 'TREE',
                        unique = true,
                        parts = {
                            {path = 'id', is_nullable = false, type = 'unsigned'},
                            {path = 'name', is_nullable = false, type = 'string'},
                        },
                    },
                    {
                        name = 'bucket_id',
                        type = 'TREE',
                        unique = false,
                        parts = {
                            {path = 'bucket_id', is_nullable = false, type = 'unsigned'},
                        },
                    },
                    {
                        name = 'name',
                        type = 'TREE',
                        unique = false,
                        parts = {
                            {path = 'name', is_nullable = false, type = 'string'},
                        },
                    },
                    {
                        name = 'age',
                        type = 'TREE',
                        unique = false,
                        parts = {
                            {path = 'age', is_nullable = false, type = 'number'},
                        },
                    },
                },
                sharding_key = { 'name' }
            }

            local schema = {
                spaces = {
                    ['customers_ddl'] = customers_schema,
                }
            }

            local _, err = ddl.set_schema(schema)
            if err ~= nil then
                error(err)
            end
        end,
        dependencies = { 'cartridge.roles.crud-storage' }
    }
end

package.preload['locations-storage'] = function()
    return {
        role_name = 'locations-storage',
        init = function(opts)
            local engine = os.getenv('ENGINE') or 'memtx'

            if opts.is_master then
                local locations_space = box.schema.space.create('locations', {
                    format = {
                        {name = 'name', is_nullable = false, type = 'string'},
                        {name = 'bucket_id', is_nullable = false, type = 'unsigned'},
                        {name = 'type', is_nullable = false, type = 'string'},
                        {name = 'workers', is_nullable = false, type = 'number'},
                    },
                    if_not_exists = true,
                    engine = engine,
                })

                locations_space:create_index('pk', {
                    parts = { {field = 'name'} },
                    if_not_exists = true,
                    unique = true,
                })
                locations_space:create_index('bucket_id', {
                    parts = { {field = 'bucket_id'} },
                    unique = false,
                    if_not_exists = true,
                })
                locations_space:create_index('workers', {
                    parts = { {field = 'workers'} },
                    unique = false,
                    if_not_exists = true,
                })
            end
        end,
        dependencies = { 'cartridge.roles.crud-storage' }
    }
end


package.preload['locations-storage-ddl'] = function()
    return {
        role_name = 'locations-storage-ddl',
        init = function()
            local engine = os.getenv('ENGINE') or 'memtx'

            local locations_schema = {
                engine = engine,
                temporary = false,
                is_local = false,
                format = {
                    {name = 'name', is_nullable = false, type = 'string'},
                    {name = 'bucket_id', is_nullable = false, type = 'unsigned'},
                    {name = 'type', is_nullable = false, type = 'string'},
                    {name = 'workers', is_nullable = false, type = 'unsigned'},
                },
                indexes = {
                    {
                        name = 'pk',
                        type = 'TREE',
                        unique = true,
                        parts = {
                            {path = 'name', is_nullable = false, type = 'string'},
                            {path = 'type', is_nullable = false, type = 'string'},
                        },
                    },
                    {
                        name = 'bucket_id',
                        type = 'TREE',
                        unique = false,
                        parts = {
                            {path = 'bucket_id', is_nullable = false, type = 'unsigned'},
                        },
                    },
                    {
                        name = 'type',
                        type = 'TREE',
                        unique = false,
                        parts = {
                            {path = 'type', is_nullable = false, type = 'string'},
                        },
                    },
                    {
                        name = 'workers',
                        type = 'TREE',
                        unique = false,
                        parts = {
                            {path = 'workers', is_nullable = false, type = 'unsigned'},
                        },
                    },
                },
                sharding_key = { 'type' }
            }

            local schema = {
                spaces = {
                    ['locations_ddl'] = locations_schema,
                }
            }

            local _, err = ddl.set_schema(schema)
            if err ~= nil then
                error(err)
            end
        end,
        dependencies = { 'cartridge.roles.crud-storage' }
    }
end

local ok, err = errors.pcall('CartridgeCfgError', cartridge.cfg, {
    bucket_count = nil,
    vshard_groups = {
        'customers',
        'locations',
    },
    roles = {
        'customers-storage',
        'customers-storage-ddl',
        'locations-storage',
        'locations-storage-ddl',
        'cartridge.roles.crud-router',
        'cartridge.roles.crud-storage',
    },
})

if not ok then
    log.error('%s', err)
    os.exit(1)
end

_G.is_initialized = cartridge.is_healthy
