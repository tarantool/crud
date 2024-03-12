local datetime_supported, datetime = pcall(require, 'datetime')
local decimal_supported, _ = pcall(require, 'decimal')

local crud_utils = require('crud.common.utils')

return function()
    if box.info.ro == true then
        return
    end

    local engine = os.getenv('ENGINE') or 'memtx'
    box.schema.space.create('no_index_space', {
        format = {
            {name = 'id', type = 'unsigned'},
            {name = 'bucket_id', type = 'unsigned'},
            {name = 'name', type = 'string'},
        },
        if_not_exists = true,
        engine = engine,
    })

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
    -- indexes with same names as fields
    customers_space:create_index('age', {
        parts = { {field = 'age'} },
        unique = false,
        if_not_exists = true,
    })
    customers_space:create_index('id', {
        parts = { {field = 'id'} },
        if_not_exists = true,
    })
    customers_space:create_index('full_name', {
        parts = {
            {field = 'name', collation = 'unicode_ci'},
            {field = 'last_name', collation = 'unicode_ci'} ,
        },
        unique = false,
        if_not_exists = true,
    })

    if crud_utils.tarantool_supports_uuids() then
        local goods_space = box.schema.space.create('goods', {
            format = {
                {name = 'uuid', type = 'uuid'},
                {name = 'bucket_id', type = 'unsigned'},
                {name = 'name', type = 'string'},
                {name = 'category_id', type = 'uuid'},
            },
            if_not_exists = true,
            engine = engine,
        })
        --primary index
        goods_space:create_index('uuid', {
            parts = { {field = 'uuid'} },
            if_not_exists = true,
        })
        goods_space:create_index('bucket_id', {
            parts = { {field = 'bucket_id'} },
            unique = false,
            if_not_exists = true,
        })
    end

    local coord_space = box.schema.space.create('coord', {
        format = {
              {name = 'x', type = 'unsigned'},
              {name = 'y', type = 'unsigned'},
              {name = 'bucket_id', type = 'unsigned'},
        },
        if_not_exists = true,
        engine = engine,
    })
    -- primary index
    coord_space:create_index('primary', {
        parts = {
            {field = 'x'},
            {field = 'y'},
        },
        if_not_exists = true,
    })
    coord_space:create_index('bucket_id', {
        parts = { {field = 'bucket_id'} },
        unique = false,
        if_not_exists = true,
    })

    local book_translation = box.schema.space.create('book_translation', {
        format = {
            { name = 'id', type = 'unsigned' },
            { name = 'bucket_id', type = 'unsigned' },
            { name = 'language', type = 'string' },
            { name = 'edition', type = 'integer' },
            { name = 'translator', type = 'string' },
            { name = 'comments', type = 'string', is_nullable = true },
        },
        if_not_exists = true,
    })

    book_translation:create_index('id', {
        parts = { 'id', 'language', 'edition' },
        if_not_exists = true,
    })

    book_translation:create_index('bucket_id', {
        parts = { 'bucket_id' },
        unique = false,
        if_not_exists = true,
    })

    local developers_space = box.schema.space.create('developers', {
        format = {
            {name = 'id', type = 'unsigned'},
            {name = 'bucket_id', type = 'unsigned'},
            {name = 'name', type = 'string'},
            {name = 'last_name', type = 'string'},
            {name = 'age', type = 'number'},
            {name = 'additional', type = 'any'},
        },
        if_not_exists = true,
        engine = engine,
    })

    -- primary index
    developers_space:create_index('id_index', {
        parts = { 'id' },
        if_not_exists = true,
    })

    developers_space:create_index('bucket_id', {
        parts = { 'bucket_id' },
        unique = false,
        if_not_exists = true,
    })

    if crud_utils.tarantool_supports_jsonpath_indexes() then
        local cars_space = box.schema.space.create('cars', {
            format = {
                {name = 'id', type = 'map'},
                {name = 'bucket_id', type = 'unsigned'},
                {name = 'age', type = 'number'},
                {name = 'manufacturer', type = 'string'},
                {name = 'data', type = 'map'}
            },
            if_not_exists = true,
            engine = engine,
        })

        -- primary index
        cars_space:create_index('id_ind', {
            parts = {
                {1, 'unsigned', path = 'car_id.signed'},
            },
            if_not_exists = true,
        })

        cars_space:create_index('bucket_id', {
            parts = { 'bucket_id' },
            unique = false,
            if_not_exists = true,
        })

        cars_space:create_index('data_index', {
            parts = {
                {5, 'str', path = 'car.color'},
                {5, 'str', path = 'car.model'},
            },
            unique = false,
            if_not_exists = true,
        })
    end

    local logins_space = box.schema.space.create('logins', {
        format = {
            {name = 'id', type = 'unsigned'},
            {name = 'bucket_id', type = 'unsigned'},
            {name = 'city', type = 'string'},
            {name = 'name', type = 'string'},
            {name = 'last_login', type = 'number'},
        },
        if_not_exists = true,
        engine = engine,
    })

    logins_space:create_index('id_index', {
        parts = { 'id' },
        if_not_exists = true,
    })

    logins_space:create_index('bucket_id', {
        parts = { 'bucket_id' },
        unique = false,
        if_not_exists = true,
    })

    logins_space:create_index('city', {
        parts = { 'city' },
        unique = false,
        if_not_exists = true,
    })

    logins_space:create_index('last_login', {
        parts = { 'last_login' },
        unique = false,
        if_not_exists = true,
    })

    if decimal_supported then
        local decimal_format = {
            {name = 'id', type = 'unsigned'},
            {name = 'bucket_id', type = 'unsigned'},
            {name = 'decimal_field', type = 'decimal'},
        }


        local decimal_nonindexed_space = box.schema.space.create('decimal_nonindexed', {
            if_not_exists = true,
            engine = engine,
        })

        decimal_nonindexed_space:format(decimal_format)

        decimal_nonindexed_space:create_index('id_index', {
            parts = { 'id' },
            if_not_exists = true,
        })

        decimal_nonindexed_space:create_index('bucket_id', {
            parts = { 'bucket_id' },
            unique = false,
            if_not_exists = true,
        })


        local decimal_indexed_space = box.schema.space.create('decimal_indexed', {
            if_not_exists = true,
            engine = engine,
        })

        decimal_indexed_space:format(decimal_format)

        decimal_indexed_space:create_index('id_index', {
            parts = { 'id' },
            if_not_exists = true,
        })

        decimal_indexed_space:create_index('bucket_id', {
            parts = { 'bucket_id' },
            unique = false,
            if_not_exists = true,
        })

        decimal_indexed_space:create_index('decimal_index', {
            parts = { 'decimal_field' },
            unique = false,
            if_not_exists = true,
        })


        local decimal_pk_space = box.schema.space.create('decimal_pk', {
            if_not_exists = true,
            engine = engine,
        })

        decimal_pk_space:format(decimal_format)

        decimal_pk_space:create_index('decimal_index', {
            parts = { 'decimal_field' },
            if_not_exists = true,
        })

        decimal_pk_space:create_index('bucket_id', {
            parts = { 'bucket_id' },
            unique = false,
            if_not_exists = true,
        })


        local decimal_multipart_index_space = box.schema.space.create('decimal_multipart_index', {
            if_not_exists = true,
            engine = engine,
        })

        decimal_multipart_index_space:format(decimal_format)

        decimal_multipart_index_space:create_index('id_index', {
            parts = { 'id' },
            if_not_exists = true,
        })

        decimal_multipart_index_space:create_index('bucket_id', {
            parts = { 'bucket_id' },
            unique = false,
            if_not_exists = true,
        })

        decimal_multipart_index_space:create_index('decimal_index', {
            parts = { 'id', 'decimal_field' },
            unique = false,
            if_not_exists = true,
        })
    end

    if datetime_supported then
        local datetime_format = {
            {name = 'id', type = 'unsigned'},
            {name = 'bucket_id', type = 'unsigned'},
            {name = 'datetime_field', type = 'datetime'},
        }


        local datetime_nonindexed_space = box.schema.space.create('datetime_nonindexed', {
            if_not_exists = true,
            engine = engine,
        })

        datetime_nonindexed_space:format(datetime_format)

        datetime_nonindexed_space:create_index('id_index', {
            parts = { 'id' },
            if_not_exists = true,
        })

        datetime_nonindexed_space:create_index('bucket_id', {
            parts = { 'bucket_id' },
            unique = false,
            if_not_exists = true,
        })


        local datetime_indexed_space = box.schema.space.create('datetime_indexed', {
            if_not_exists = true,
            engine = engine,
        })

        datetime_indexed_space:format(datetime_format)

        datetime_indexed_space:create_index('id_index', {
            parts = { 'id' },
            if_not_exists = true,
        })

        datetime_indexed_space:create_index('bucket_id', {
            parts = { 'bucket_id' },
            unique = false,
            if_not_exists = true,
        })

        datetime_indexed_space:create_index('datetime_index', {
            parts = { 'datetime_field' },
            unique = false,
            if_not_exists = true,
        })


        local datetime_pk_space = box.schema.space.create('datetime_pk', {
            if_not_exists = true,
            engine = engine,
        })

        datetime_pk_space:format(datetime_format)

        datetime_pk_space:create_index('datetime_index', {
            parts = { 'datetime_field' },
            if_not_exists = true,
        })

        datetime_pk_space:create_index('bucket_id', {
            parts = { 'bucket_id' },
            unique = false,
            if_not_exists = true,
        })


        local datetime_multipart_index_space = box.schema.space.create('datetime_multipart_index', {
            if_not_exists = true,
            engine = engine,
        })

        datetime_multipart_index_space:format(datetime_format)

        datetime_multipart_index_space:create_index('id_index', {
            parts = { 'id' },
            if_not_exists = true,
        })

        datetime_multipart_index_space:create_index('bucket_id', {
            parts = { 'bucket_id' },
            unique = false,
            if_not_exists = true,
        })

        datetime_multipart_index_space:create_index('datetime_index', {
            parts = { 'id', 'datetime_field' },
            unique = false,
            if_not_exists = true,
        })
    end

    local interval_supported = datetime_supported and (datetime.interval ~= nil)
    if interval_supported then
        -- Interval is non-indexable.
        local interval_space = box.schema.space.create('interval', {
            if_not_exists = true,
            engine = engine,
        })

        interval_space:format({
            {name = 'id', type = 'unsigned'},
            {name = 'bucket_id', type = 'unsigned'},
            {name = 'interval_field', type = 'interval'},
        })

        interval_space:create_index('id_index', {
            parts = { 'id' },
            if_not_exists = true,
        })

        interval_space:create_index('bucket_id', {
            parts = { 'bucket_id' },
            unique = false,
            if_not_exists = true,
        })
    end
end
