local ddl = require('ddl')
local helper = require('test.helper')

local some_module = {
    sharding_func =
    function(key)
        if key ~= nil and key[1] ~= nil then
            return key[1] % 10
        end
    end
}
rawset(_G, 'some_module', some_module)

return {
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
        local age_index = {
            name = 'age',
            type = 'TREE',
            unique = false,
            parts = {
                {path = 'age', is_nullable = false, type = 'number'},
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

        local three_fields_index = {
            name = 'three_fields',
            type = 'TREE',
            unique = false,
            parts = {
                {path = 'age', is_nullable = false, type = 'number'},
                {path = 'name', is_nullable = false, type = 'string'},
                {path = 'id', is_nullable = false, type = 'unsigned'},
            },
        }

        local customers_id_schema = table.deepcopy(customers_schema)
        customers_id_schema.sharding_key = {'id'}
        table.insert(customers_id_schema.indexes, primary_index_id)
        table.insert(customers_id_schema.indexes, bucket_id_index)
        table.insert(customers_id_schema.indexes, age_index)

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

        local customers_age_key_schema = table.deepcopy(customers_schema)
        customers_age_key_schema.sharding_key = {'age'}
        table.insert(customers_age_key_schema.indexes, primary_index)
        table.insert(customers_age_key_schema.indexes, bucket_id_index)

        local customers_name_age_key_different_indexes_schema = table.deepcopy(customers_schema)
        customers_name_age_key_different_indexes_schema.sharding_key = {'name', 'age'}
        table.insert(customers_name_age_key_different_indexes_schema.indexes, primary_index)
        table.insert(customers_name_age_key_different_indexes_schema.indexes, bucket_id_index)
        table.insert(customers_name_age_key_different_indexes_schema.indexes, age_index)

        local customers_name_age_key_three_fields_index_schema = table.deepcopy(customers_schema)
        customers_name_age_key_three_fields_index_schema.sharding_key = {'name', 'age'}
        table.insert(customers_name_age_key_three_fields_index_schema.indexes, primary_index_id)
        table.insert(customers_name_age_key_three_fields_index_schema.indexes, bucket_id_index)
        table.insert(customers_name_age_key_three_fields_index_schema.indexes, three_fields_index)

        local customers_id_key_schema = table.deepcopy(customers_schema)
        customers_id_key_schema.sharding_key = {'id'}
        table.insert(customers_id_key_schema.indexes, primary_index)
        table.insert(customers_id_key_schema.indexes, bucket_id_index)
        table.insert(customers_id_key_schema.indexes, name_index)

        local customers_body_func_schema = table.deepcopy(customers_id_key_schema)
        customers_body_func_schema.sharding_func = { body = 'function(key) return key[1] % 10 end' }

        local customers_G_func_schema = table.deepcopy(customers_id_key_schema)
        customers_G_func_schema.sharding_func = 'some_module.sharding_func'

        local customers_empty_sharding_func_schema = table.deepcopy(customers_id_key_schema)

        local customers_vshard_mpcrc32_schema = table.deepcopy(customers_id_key_schema)
        customers_vshard_mpcrc32_schema.sharding_func = 'vshard.router.bucket_id_mpcrc32'

        local customers_vshard_strcrc32_schema = table.deepcopy(customers_id_key_schema)
        customers_vshard_strcrc32_schema.sharding_func = 'vshard.router.bucket_id_strcrc32'

        local schema = {
            spaces = {
                customers = customers_id_schema,
                customers_name_key = customers_name_key_schema,
                customers_name_key_uniq_index = customers_name_key_uniq_index_schema,
                customers_name_key_non_uniq_index = customers_name_key_non_uniq_index_schema,
                customers_secondary_idx_name_key = customers_secondary_idx_name_key_schema,
                customers_age_key = customers_age_key_schema,
                customers_name_age_key_different_indexes = customers_name_age_key_different_indexes_schema,
                customers_name_age_key_three_fields_index = customers_name_age_key_three_fields_index_schema,
                customers_G_func = customers_G_func_schema,
                customers_body_func = customers_body_func_schema,
                customers_empty_sharding_func = customers_empty_sharding_func_schema,
                customers_vshard_mpcrc32 = customers_vshard_mpcrc32_schema,
                customers_vshard_strcrc32 = customers_vshard_strcrc32_schema,
            }
        }

        rawset(_G, 'set_sharding_key', function(space_name, sharding_key_def)
            local fieldno_sharding_key = 2
            box.space['_ddl_sharding_key']:update(space_name, {{'=', fieldno_sharding_key, sharding_key_def}})
        end)
        rawset(_G, 'set_sharding_func', function(space_name, fieldno_sharding_func, sharding_func_def)
            local record = {space_name, box.NULL, box.NULL}
            record[fieldno_sharding_func] = sharding_func_def
            box.space['_ddl_sharding_func']:replace(record)
        end)

        helper.wrap_schema_init(function()
            local ok, err = ddl.set_schema(schema)
            if not ok then
                error(err)
            end
        end)()
    end,
    wait_until_ready = helper.wait_schema_init,
}
