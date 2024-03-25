local helper = require('test.helper')

return {
    init = helper.wrap_schema_init(function()
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
            parts = { {field = 'id' } },
            if_not_exists = true,
        })
        customers_space:create_index('bucket_id', {
            parts = { {field = 'bucket_id' } },
            unique = false,
            if_not_exists = true,
        })
    end),
    wait_until_ready = helper.wait_schema_init,
}
