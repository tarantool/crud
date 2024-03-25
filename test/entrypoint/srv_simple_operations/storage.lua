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

        rawset(_G, 'add_extra_field', function(space_name, field_name)
            local space = box.space[space_name]
            local new_format = space:format()
            table.insert(new_format, {name = field_name, type = 'any', is_nullable = true})
            space:format(new_format)
        end)

        rawset(_G, 'create_space_for_gh_326_cases', function()
            local countries_space = box.schema.space.create('countries', {
                format = {
                    {name = 'id', type = 'unsigned'},
                    {name = 'bucket_id', type = 'unsigned'},
                    {name = 'name', type = 'string'},
                    {name = 'population', type = 'unsigned'},
                },
                if_not_exists = true,
                engine = os.getenv('ENGINE') or 'memtx',
            })
            countries_space:create_index('id', {
                parts = { {field = 'id'} },
                if_not_exists = true,
            })
            countries_space:create_index('bucket_id', {
                parts = { {field = 'bucket_id'} },
                unique = false,
                if_not_exists = true,
            })
        end)

        rawset(_G, 'drop_space_for_gh_326_cases', function()
            box.space['countries']:drop()
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
    end),
    wait_until_ready = helper.wait_schema_init,
}
