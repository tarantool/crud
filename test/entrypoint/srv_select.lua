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
            box.schema.space.create('no_index_space', {
                format = {
                    {name = 'id', type = 'unsigned'},
                    {name = 'bucket_id', type = 'unsigned'},
                    {name = 'name', type = 'string'},
                },
                if_not_exists = true,
                engine = engine,
            })

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
            -- primary index
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
            -- indexes with same names as fields
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
                    {field = 'name', collation = 'unicode_ci'},
                    {field = 'last_name', collation = 'unicode_ci'} ,
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

            local coord_space = box.schema.space.create('coord', {
                format = {
                      {name = 'x', type = 'unsigned'},
                      {name = 'y', type = 'unsigned'},
                      {name = 'bucket_id', type = 'unsigned'},
                },
                if_not_exists = true,
                engine = engine,
            })
            -- primary index
            coord_space:create_index('primary', {
                parts = {
                    {field = 'x'},
                    {field = 'y'},
                },
                if_not_exists = true,
            })
            coord_space:create_index('bucket_id', {
                parts = { {field = 'bucket_id'} },
                if_not_exists = true,
            })

            local book_translation = box.schema.space.create('book_translation', {
                format = {
                    { name = 'id', type = 'unsigned' },
                    { name = 'bucket_id', type = 'unsigned' },
                    { name = 'language', type = 'string' },
                    { name = 'edition', type = 'integer' },
                    { name = 'translator', type = 'string' },
                    { name = 'comments', type = 'string', is_nullable = true },
                },
                if_not_exists = true,
            })

            book_translation:create_index('id', {
                parts = { 'id', 'language', 'edition' },
                if_not_exists = true,
            })

            book_translation:create_index('bucket_id', {
                parts = { 'bucket_id' },
                unique = false,
                if_not_exists = true,
            })

            local developers_space = box.schema.space.create('developers', {
                format = {
                    {name = 'id', type = 'unsigned'},
                    {name = 'bucket_id', type = 'unsigned'},
                    {name = 'name', type = 'string'},
                    {name = 'last_name', type = 'string'},
                    {name = 'age', type = 'number'},
                    {name = 'additional', type = 'any'},
                },
                if_not_exists = true,
                engine = engine,
            })

            -- primary index
            developers_space:create_index('id_index', {
                parts = { 'id' },
                if_not_exists = true,
            })

            developers_space:create_index('bucket_id', {
                parts = { 'bucket_id' },
                unique = false,
                if_not_exists = true,
            })

            if crud_utils.tarantool_supports_jsonpath_indexes() then
                local cars_space = box.schema.space.create('cars', {
                    format = {
                        {name = 'id', type = 'map'},
                        {name = 'bucket_id', type = 'unsigned'},
                        {name = 'age', type = 'number'},
                        {name = 'manufacturer', type = 'string'},
                        {name = 'data', type = 'map'}
                    },
                    if_not_exists = true,
                    engine = engine,
                })

                -- primary index
                cars_space:create_index('id_ind', {
                    parts = {
                        {1, 'unsigned', path = 'car_id.signed'},
                    },
                    if_not_exists = true,
                })

                cars_space:create_index('bucket_id', {
                    parts = { 'bucket_id' },
                    unique = false,
                    if_not_exists = true,
                })

                cars_space:create_index('data_index', {
                    parts = {
                        {5, 'str', path = 'car.color'},
                        {5, 'str', path = 'car.model'},
                    },
                    unique = false,
                    if_not_exists = true,
                })
            end
        end,
    }
end

local box_opts = {
    readahead = 10 * 1024 * 1024,
}
local ok, err = errors.pcall('CartridgeCfgError', cartridge.cfg, {
    advertise_uri = 'localhost:3301',
    http_port = 8081,
    bucket_count = 3000,
    roles = {
        'cartridge.roles.crud-router',
        'cartridge.roles.crud-storage',
        'customers-storage',
    }},
    box_opts
)

if not ok then
    log.error('%s', err)
    os.exit(1)
end

_G.is_initialized = cartridge.is_healthy
