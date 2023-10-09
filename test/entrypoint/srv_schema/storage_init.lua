return function()
    if box.info.ro == true then
        return
    end

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

    local shops_space = box.schema.space.create('shops', {
        format = {
            {name = 'registry_id', type = 'unsigned'},
            {name = 'bucket_id', type = 'unsigned'},
            {name = 'name', type = 'string'},
            {name = 'address', type = 'string'},
            {name = 'owner', type = 'string', is_nullable = true},
        },
        if_not_exists = true,
        engine = engine,
    })
    shops_space:create_index('registry', {
        parts = { {field = 'registry_id'} },
        if_not_exists = true,
    })
    shops_space:create_index('bucket_id', {
        parts = { {field = 'bucket_id'} },
        unique = false,
        if_not_exists = true,
    })
    shops_space:create_index('address', {
        parts = { {field = 'address'} },
        unique = true,
        if_not_exists = true,
    })
end
