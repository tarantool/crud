local M = {}

local function register_function(func_name)
    rawset(
        _G._crud,
        ('%s_on_storage'):format(func_name),
        require(('crud.%s'):format(func_name)).storage_api[('%s_on_storage'):format(func_name)]
    )
end

function M.setup()
    os.execute("rm -f *.xlog *.snap")
    box.cfg{
        wal_mode='none',
        checkpoint_interval = 3600, checkpoint_count = 1,
        memtx_memory = 1024^3,
    }
    local customers_space = box.schema.space.create('customers', {
        format = {
            {name = 'id', type = 'unsigned'},
            {name = 'bucket_id', type = 'unsigned'},
            {name = 'name', type = 'string'},
            {name = 'age', type = 'number'},
        },
        if_not_exists = true,
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
    box.schema.user.grant('guest', 'super', nil, nil, {if_not_exists = true})

    rawset(_G, '_crud', {})
    register_function('replace')
    register_function('get')
    register_function('delete')
    register_function('insert')
    register_function('update')
    register_function('upsert')
    register_function('select')
end

return M
