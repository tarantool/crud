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
            {name = 'last_name', type = 'string'},
            {name = 'age', type = 'number'},
            {name = 'city', type = 'string'},
        },
        if_not_exists = true,
        engine = engine,
        id = 542,
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
end
