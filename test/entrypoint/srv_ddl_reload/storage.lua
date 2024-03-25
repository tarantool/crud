local ddl = require('ddl')

return {
    init = function()
        local customers_module = {
            sharding_func_default = function(key)
                local id = key[1]
                assert(id ~= nil)

                return id % 3000 + 1
            end,
            sharding_func_new = function(key)
                local id = key[1]
                assert(id ~= nil)

                return (id + 42) % 3000 + 1
            end,
        }
        rawset(_G, 'customers_module', customers_module)

        local engine = os.getenv('ENGINE') or 'memtx'

        local customers_schema_raw = {
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
                    name = 'id',
                    type = 'TREE',
                    unique = true,
                    parts = {
                        {path = 'id', is_nullable = false, type = 'unsigned'},
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
            sharding_key = { 'name' },
        }

        local customers_schema = table.deepcopy(customers_schema_raw)
        customers_schema.sharding_key = { 'name' }

        local customers_pk_schema = table.deepcopy(customers_schema_raw)
        customers_pk_schema.sharding_key = { 'id' }
        customers_pk_schema.sharding_func = 'customers_module.sharding_func_default'

        local schema = {
            spaces = {
                ['customers'] = customers_schema,
                ['customers_pk'] = customers_pk_schema,
            }
        }

        rawset(_G, 'reset_to_default_schema', function()
            if box.info.ro == true then
                return
            end

            if box.space['_ddl_sharding_key'] ~= nil then
                box.space['_ddl_sharding_key']:truncate()
                box.space['_ddl_sharding_key']:insert{'customers', customers_schema.sharding_key}
                box.space['_ddl_sharding_key']:insert{'customers_pk', customers_pk_schema.sharding_key}
            end

            if box.space['_ddl_sharding_func'] ~= nil then
                box.space['_ddl_sharding_func']:truncate()
                box.space['_ddl_sharding_func']:insert{'customers_pk', customers_pk_schema.sharding_func, box.NULL}
            end

            local _, err = ddl.set_schema(schema)
            if err ~= nil then
                error(err)
            end
        end)

        rawset(_G, 'set_sharding_key', function(space_name, sharding_key_def)
            if box.info.ro == true then
                return
            end

            local current_schema, err = ddl.get_schema()
            if err ~= nil then
                error(err)
            end

            box.space['_ddl_sharding_key']:replace{space_name, sharding_key_def}
            current_schema.spaces[space_name].sharding_key = sharding_key_def

            local _, err = ddl.set_schema(current_schema)
            if err ~= nil then
                error(err)
            end
        end)

        rawset(_G, 'set_sharding_func_name', function(space_name, sharding_func_name)
            if box.info.ro == true then
                return
            end

            local current_schema, err = ddl.get_schema()
            if err ~= nil then
                error(err)
            end

            local t = {space_name, sharding_func_name, box.NULL}
            box.space['_ddl_sharding_func']:replace(t)
            current_schema.spaces[space_name].sharding_func = sharding_func_name

            local _, err = ddl.set_schema(current_schema)
            if err ~= nil then
                error(err)
            end
        end)

        rawset(_G, 'set_sharding_func_body', function(space_name, sharding_func_body)
            if box.info.ro == true then
                return
            end

            local current_schema, err = ddl.get_schema()
            if err ~= nil then
                error(err)
            end

            local t = {space_name, box.NULL, sharding_func_body}
            box.space['_ddl_sharding_func']:replace(t)
            current_schema.spaces[space_name].sharding_func = { body = sharding_func_body }

            local _, err = ddl.set_schema(current_schema)
            if err ~= nil then
                error(err)
            end
        end)

        rawset(_G, 'create_new_space', function()
            if box.info.ro == true then
                return
            end

            local new_schema = table.deepcopy(schema)
            new_schema.spaces['customers_new'] = table.deepcopy(customers_schema_raw)
            new_schema.spaces['customers_new'].sharding_func = {
                body = [[
                    function(key)
                        local vshard = require('vshard')
                        return vshard.router.bucket_id_mpcrc32(key)
                    end
                ]]
            }

            local _, err = ddl.set_schema(new_schema)
            if err ~= nil then
                error(err)
            end
        end)
    end,
}
