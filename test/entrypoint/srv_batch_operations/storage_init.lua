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
end
