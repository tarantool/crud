return function()
    if box.info.ro == true then
        return
    end

    local engine = os.getenv('ENGINE') or 'memtx'
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
end
