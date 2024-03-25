local helper = require('test.helper')

return {
    init = helper.wrap_schema_init(function()
        box.schema.space.create('customers', {if_not_exists = true})

        box.space['customers']:format{
            {name = 'id',           is_nullable = false, type = 'unsigned'},
            {name = 'bucket_id',    is_nullable = false, type = 'unsigned'},
            {name = 'sharding_key', is_nullable = false, type = 'unsigned'},
        }

        box.space['customers']:create_index('pk', {parts = { 'id' }, if_not_exists = true})
        box.space['customers']:create_index('bucket_id', {parts = { 'bucket_id' }, if_not_exists = true})
    end),
    wait_until_ready = helper.wait_schema_init,
}
