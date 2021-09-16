local t = require('luatest')
local sharding_key_module = require('crud.common.sharding_key')
local cache = require('crud.common.sharding_key_cache')
local utils = require('crud.common.utils')

local g = t.group('sharding_key')

g.before_each(function()
    local sharding_key_format = {
        {name = 'space_name', type = 'string', is_nullable = false},
        {name = 'sharding_key', type = 'array', is_nullable = false}
    }
    -- Create a space _ddl_sharding_key with a tuple that
    -- contains a space name and it's sharding key.
    if type(box.cfg) ~= 'table' then
        box.cfg{}
    end
    box.schema.space.create('_ddl_sharding_key', {
        format = sharding_key_format,
    })
    box.space._ddl_sharding_key:create_index('pk')
    box.schema.space.create('fetch_on_storage')
end)

g.after_each(function()
    -- Cleanup.
    if box.space._ddl_sharding_key ~= nil then
        box.space._ddl_sharding_key:drop()
    end
    box.space.fetch_on_storage:drop()
    cache.drop_caches()
end)

g.test_as_index_object_positive = function()
    local space_name = 'as_index_object'
    local space_format = {
        {name = 'id', type = 'unsigned'},
        {name = 'name', type = 'string'},
        {name = 'age', type = 'unsigned'},
    }
    local sharding_key_def = {'name', 'age'}

    local index_obj, err = sharding_key_module.internal.as_index_object(space_name,
                                                                        space_format,
                                                                        sharding_key_def)
    t.assert_equals(err, nil)
    t.assert_equals(index_obj, {
        parts = {
            {fieldno = 2},
            {fieldno = 3},
        }
    })
end

g.test_as_index_object_negative = function()
    local space_name = 'as_index_object'
    local space_format = {
        {name = 'id', type = 'unsigned'},
        {name = 'name', type = 'string'},
        {name = 'age', type = 'unsigned'},
    }
    local sharding_key_def = {'dude', 'age'}

    local index_obj, err = sharding_key_module.internal.as_index_object(space_name,
                                                                        space_format,
                                                                        sharding_key_def)
    t.assert_str_contains(err.err,
        'No such field (dude) in a space format (as_index_object)')
    t.assert_equals(index_obj, nil)
end

g.test_get_format_fieldno_map = function()
    local space_format = {
        {name = 'id', type = 'unsigned'},
        {name = 'name', type = 'string'},
        {name = 'age', type = 'unsigned'},
    }

    local fieldno_map = utils.get_format_fieldno_map(space_format)
    t.assert_equals(fieldno_map, {age = 3, id = 1, name = 2})
end

g.test_fetch_on_storage_positive = function()
    local space_name = 'fetch_on_storage'
    local sharding_key_def = {'name', 'age'}
    box.space._ddl_sharding_key:insert({space_name, sharding_key_def})

    local metadata_map = sharding_key_module.fetch_on_storage()

    t.assert_equals(metadata_map, {
        [space_name] = {
            sharding_key_def = sharding_key_def,
            space_format = {}
        },
    })
end

g.test_fetch_on_storage_negative = function()
    -- Test checks return value when _ddl_sharding_key is absent.
    box.space._ddl_sharding_key:drop()

    local metadata_map = sharding_key_module.fetch_on_storage()
    t.assert_equals(metadata_map, nil)
end

g.test_extract_from_index_sharding_key_direct_order = function()
    local primary_index_parts = {
        {fieldno = 1},
        {fieldno = 2},
    }
    local sharding_key_as_index_obj = {
        parts = {
            {fieldno = 1},
            {fieldno = 2},
        }
    }
    local primary_key = {'name', 'age'}

    local extract_from_index = sharding_key_module.internal.extract_from_index
    local sharding_key = extract_from_index(primary_key,
                                            primary_index_parts,
                                            sharding_key_as_index_obj)
    t.assert_equals(sharding_key, {'name', 'age'})
end

g.test_extract_from_index_sharding_key_reverse_order = function()
    local primary_index_parts = {
        {fieldno = 1},
        {fieldno = 2},
    }
    local sharding_key_as_index_obj = {
        parts = {
            {fieldno = 2},
            {fieldno = 1},
        }
    }
    local primary_key = {'name', 'age'}

    local extract_from_index = sharding_key_module.internal.extract_from_index
    local sharding_key = extract_from_index(primary_key,
                                            primary_index_parts,
                                            sharding_key_as_index_obj)
    t.assert_equals(sharding_key, {'age', 'name'})
end

g.test_extract_from_index_sharding_key_single_field = function()
    local primary_index_parts = {
        {fieldno = 1},
        {fieldno = 2},
        {fieldno = 3},
    }
    local sharding_key_as_index_obj = {
        parts = {
            {fieldno = 2},
        }
    }
    local primary_key = {'name', 'age', 'location'}

    local extract_from_index = sharding_key_module.internal.extract_from_index
    local sharding_key = extract_from_index(primary_key,
                                            primary_index_parts,
                                            sharding_key_as_index_obj)
    t.assert_equals(sharding_key, {'age'})
end

g.test_extract_from_index_sharding_key_none_fields = function()
    local primary_index_parts = {
        {fieldno = 1},
        {fieldno = 3},
    }
    local sharding_key_as_index_obj = {
        parts = {
            {fieldno = 2},
        }
    }
    local primary_key = {'name', 'age', 'location'}

    local extract_from_index = sharding_key_module.internal.extract_from_index
    local ok, err = pcall(extract_from_index, primary_key,
                                              primary_index_parts,
                                              sharding_key_as_index_obj)
    t.assert_equals(ok, false)
    t.assert_str_contains(err, 'assertion failed')
end

g.test_get_index_fieldno_map = function()
    local index_parts = {
        {fieldno = 2},
        {fieldno = 3},
    }

    local fieldno_map = utils.get_index_fieldno_map(index_parts)
    t.assert_equals(fieldno_map, {
        [2] = 1,
        [3] = 2
    })
end

g.test_is_part_of_pk_positive = function()
    local space_name = 'is_part_of_pk'
    local index_parts = {
        {fieldno = 2},
        {fieldno = 3},
    }
    local sharding_key_as_index_obj = {
        parts = {
            {fieldno = 2},
        }
    }

    local is_part_of_pk = sharding_key_module.internal.is_part_of_pk
    local res = is_part_of_pk(space_name, index_parts, sharding_key_as_index_obj)
    t.assert_equals(res, true)
end

g.test_is_part_of_pk_negative = function()
    local space_name = 'is_part_of_pk'
    local index_parts = {
        {fieldno = 1},
        {fieldno = 3},
    }
    local sharding_key_as_index_obj = {
        parts = {
            {fieldno = 2},
        }
    }

    local is_part_of_pk = sharding_key_module.internal.is_part_of_pk
    local res = is_part_of_pk(space_name, index_parts, sharding_key_as_index_obj)
    t.assert_equals(res, false)
end
