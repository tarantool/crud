local helper = require('test.helper')

return {
    init = helper.wrap_schema_init(function()
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
                {name = 'name', type = 'string'},
                {name = 'login', type = 'string'},
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
        developers_space:create_index('login', {
            parts = { {field = 'login'} },
            unique = true,
            if_not_exists = true,
        })

        local customers_sharded_by_age_space = box.schema.space.create('customers_sharded_by_age', {
            format = {
                {name = 'id', type = 'unsigned'},
                {name = 'bucket_id', type = 'unsigned'},
                {name = 'name', type = 'string'},
                {name = 'age', type = 'number'},
            },
            if_not_exists = true,
            engine = engine,
        })
        customers_sharded_by_age_space:create_index('id', {
            parts = { {field = 'id'} },
            if_not_exists = true,
        })
        customers_sharded_by_age_space:create_index('bucket_id', {
            parts = { {field = 'bucket_id'} },
            unique = false,
            if_not_exists = true,
        })

        -- https://github.com/tarantool/migrations/blob/a7c31a17f6ac02d4498b4203c23e495856861444/migrator/utils.lua#L35-L53
        if box.space._ddl_sharding_key == nil then
            local sharding_space = box.schema.space.create('_ddl_sharding_key', {
                format = {
                    {name = 'space_name', type = 'string', is_nullable = false},
                    {name = 'sharding_key', type = 'array', is_nullable = false}
                },
                if_not_exists = true,
            })
            sharding_space:create_index(
                'space_name', {
                    type = 'TREE',
                    unique = true,
                    parts = {{'space_name', 'string', is_nullable = false}},
                    if_not_exists = true,
                }
            )
        end
        box.space._ddl_sharding_key:replace{'customers_sharded_by_age', {'age'}}
    end),
    wait_until_ready = helper.wait_schema_init,
}
