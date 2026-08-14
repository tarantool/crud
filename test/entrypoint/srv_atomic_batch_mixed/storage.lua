local helper = require('test.helper')

return {
    init = helper.wrap_schema_init(function()
        local function create_space(space_name, engine)
            local space = box.schema.space.create(space_name, {
                format = {
                    {name = 'id', type = 'unsigned'},
                    {name = 'bucket_id', type = 'unsigned'},
                    {name = 'name', type = 'string'},
                    {name = 'age', type = 'number'},
                },
                if_not_exists = true,
                engine = engine,
            })

            space:create_index('id', {
                parts = {{field = 'id'}},
                if_not_exists = true,
            })
            space:create_index('bucket_id', {
                parts = {{field = 'bucket_id'}},
                unique = false,
                if_not_exists = true,
            })
        end

        create_space('customers_memtx', 'memtx')
        create_space('customers_vinyl', 'vinyl')
    end),
    wait_until_ready = helper.wait_schema_init,
}