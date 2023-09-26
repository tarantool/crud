return function()
    if box.info.ro == true then
        return
    end
    box.schema.space.create('customers')

    box.space['customers']:format{
        {name = 'id',           is_nullable = false, type = 'unsigned'},
        {name = 'bucket_id',    is_nullable = false, type = 'unsigned'},
        {name = 'sharding_key', is_nullable = false, type = 'unsigned'},
    }

    box.space['customers']:create_index('pk',        {parts = { 'id' }})
    box.space['customers']:create_index('bucket_id', {parts = { 'bucket_id' }})
end
