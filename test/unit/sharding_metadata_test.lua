local t = require('luatest')
local ffi = require('ffi')
local sharding_metadata_module = require('crud.common.sharding.sharding_metadata')
local sharding_key_module = require('crud.common.sharding.sharding_key')
local sharding_func_module = require('crud.common.sharding.sharding_func')
local sharding_utils = require('crud.common.sharding.utils')
local router_cache = require('crud.common.sharding.router_metadata_cache')
local storage_cache = require('crud.common.sharding.storage_metadata_cache')
local utils = require('crud.common.utils')

local helpers = require('test.helper')

local g = t.group('sharding_metadata')

g.before_each(function()
    local sharding_key_format = {
        {name = 'space_name', type = 'string', is_nullable = false},
        {name = 'sharding_key', type = 'array', is_nullable = false}
    }

    local sharding_func_format = {
        {name = 'space_name', type = 'string', is_nullable = false},
        {name = 'sharding_func_name', type = 'string', is_nullable = true},
        {name = 'sharding_func_body', type = 'string', is_nullable = true},
    }

    if type(box.cfg) ~= 'table' then
        helpers.box_cfg()
    end

    -- Create a space _ddl_sharding_key with a tuple that
    -- contains a space name and it's sharding key.
    box.schema.space.create('_ddl_sharding_key', {
        format = sharding_key_format,
    })
    box.space._ddl_sharding_key:create_index('pk')

    -- Create a space _ddl_sharding_func with a tuple that
    -- contains a space name and it's sharding func name/body.
    box.schema.space.create('_ddl_sharding_func', {
        format = sharding_func_format,
    })
    box.space._ddl_sharding_func:create_index('pk')

    box.schema.space.create('fetch_on_storage')
end)

-- Since Tarantool 3.0 triggers still live after a space drop. To properly
-- clean up for the unit tests we need to remove all triggers from
-- the space. This is necessary because `crud` adds its own triggers to the
-- `ddl` spaces.
--
-- In practice `ddl` does not drop this spaces so it is the tests problem.
local function drop_ddl_space(space)
    for _, t in pairs(space:on_replace()) do
        space:on_replace(nil, t)
    end
    space:drop()
end

g.after_each(function()
    -- Cleanup.
    if box.space._ddl_sharding_key ~= nil then
        drop_ddl_space(box.space._ddl_sharding_key)
    end

    if box.space._ddl_sharding_func ~= nil then
        drop_ddl_space(box.space._ddl_sharding_func)
    end

    box.space.fetch_on_storage:drop()
    router_cache.drop_caches()
    storage_cache.drop_caches()
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

g.test_fetch_sharding_metadata_on_storage_positive = function()
    local space_name = 'fetch_on_storage'
    local sharding_key_def = {'name', 'age'}
    local sharding_func_def = 'sharding_func_name'

    box.space._ddl_sharding_key:insert({space_name, sharding_key_def})
    box.space._ddl_sharding_func:insert({space_name, sharding_func_def})

    local metadata_map = sharding_metadata_module.fetch_on_storage()

    t.assert_equals(metadata_map, {
        [space_name] = {
            sharding_key_def = sharding_key_def,
            sharding_key_hash = sharding_utils.compute_hash(sharding_key_def),
            sharding_func_def = sharding_func_def,
            sharding_func_hash = sharding_utils.compute_hash(sharding_func_def),
            space_format = {}
        },
    })
end

g.test_fetch_sharding_key_on_storage_positive = function()
    box.space._ddl_sharding_func:drop()

    local space_name = 'fetch_on_storage'
    local sharding_key_def = {'name', 'age'}
    box.space._ddl_sharding_key:insert({space_name, sharding_key_def})

    local metadata_map = sharding_metadata_module.fetch_on_storage()

    t.assert_equals(metadata_map, {
        [space_name] = {
            sharding_key_def = sharding_key_def,
            sharding_key_hash = sharding_utils.compute_hash(sharding_key_def),
            space_format = {}
        },
    })
end

g.test_fetch_sharding_func_name_on_storage_positive = function()
    box.space._ddl_sharding_key:drop()

    local space_name = 'fetch_on_storage'
    local sharding_func_def = 'sharding_func_name'
    box.space._ddl_sharding_func:insert({space_name, sharding_func_def})

    local metadata_map = sharding_metadata_module.fetch_on_storage()

    t.assert_equals(metadata_map, {
        [space_name] = {
            sharding_func_def = sharding_func_def,
            sharding_func_hash = sharding_utils.compute_hash(sharding_func_def),
        },
    })
end

g.test_fetch_sharding_func_body_on_storage_positive = function()
    box.space._ddl_sharding_key:drop()

    local space_name = 'fetch_on_storage'
    local sharding_func_def = 'function(key) return key end'
    box.space._ddl_sharding_func:insert({space_name, nil, sharding_func_def})

    local metadata_map = sharding_metadata_module.fetch_on_storage()

    t.assert_equals(metadata_map, {
        [space_name] = {
            sharding_func_def = {body = sharding_func_def},
            sharding_func_hash = sharding_utils.compute_hash({body = sharding_func_def}),
        },
    })
end

g.test_fetch_sharding_metadata_on_storage_negative = function()
    -- Test checks return value when _ddl_sharding_key
    -- and _ddl_sharding_func are absent.
    box.space._ddl_sharding_key:drop()
    box.space._ddl_sharding_func:drop()

    local metadata_map = sharding_metadata_module.fetch_on_storage()
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
    local cache = router_cache.get_instance({name = 'dummy'})
    local res = is_part_of_pk(cache, space_name, index_parts, sharding_key_as_index_obj)
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
    local cache = router_cache.get_instance({name = 'dummy'})
    local res = is_part_of_pk(cache, space_name, index_parts, sharding_key_as_index_obj)
    t.assert_equals(res, false)
end

g.test_as_callable_object_func_body = function()
    local sharding_func_def = {body = 'function(key) return key end'}

    local callable_obj, err = sharding_func_module.internal.as_callable_object(sharding_func_def,
                                                                              'space_name')
    t.assert_equals(err, nil)
    t.assert_equals(type(callable_obj), 'function')
    t.assert_equals(callable_obj(5), 5)
end

g.test_as_callable_object_G_func = function()
    local some_module = {
        sharding_func = function(key) return key % 10 end
    }
    local module_name = 'some_module'
    local sharding_func_def = 'some_module.sharding_func'
    rawset(_G, module_name, some_module)

    local callable_obj, err = sharding_func_module.internal.as_callable_object(sharding_func_def,
                                                                              'space_name')
    t.assert_equals(err, nil)
    t.assert_equals(callable_obj, some_module.sharding_func)

    rawset(_G, module_name, nil)
end

g.test_as_callable_object_func_body_negative = function()
    local sharding_func_def = {body = 'function(key) return key'}

    local callable_obj, err = sharding_func_module.internal.as_callable_object(sharding_func_def,
                                                                              'space_name')
    t.assert_equals(callable_obj, nil)
    t.assert_str_contains(err.err,
            'Body is incorrect in sharding_func for space (space_name)')
end

g.test_as_callable_object_G_func_not_exist = function()
    local sharding_func_def = 'some_module.sharding_func'

    local callable_obj, err = sharding_func_module.internal.as_callable_object(sharding_func_def,
                                                                              'space_name')
    t.assert_equals(callable_obj, nil)
    t.assert_str_contains(err.err,
            'Wrong sharding function specified in _ddl_sharding_func space for (space_name) space')
end

g.test_as_callable_object_G_func_keyword = function()
    local sharding_func_def = 'and'
    rawset(_G, sharding_func_def, function(key) return key % 10 end)

    local callable_obj, err = sharding_func_module.internal.as_callable_object(sharding_func_def,
                                                                             'space_name')
    t.assert_equals(callable_obj, nil)
    t.assert_str_contains(err.err,
            'Wrong sharding function specified in _ddl_sharding_func space for (space_name) space')

    rawset(_G, sharding_func_def, nil)
end

g.test_as_callable_object_G_func_begin_with_digit = function()
    local sharding_func_def = '5incorrect_name'
    rawset(_G, sharding_func_def, function(key) return key % 10 end)

    local callable_obj, err = sharding_func_module.internal.as_callable_object(sharding_func_def,
                                                                              'space_name')
    t.assert_equals(callable_obj, nil)
    t.assert_str_contains(err.err,
            'Wrong sharding function specified in _ddl_sharding_func space for (space_name) space')

    rawset(_G, sharding_func_def, nil)
end

g.test_as_callable_object_G_func_incorrect_symbol = function()
    local sharding_func_def = 'incorrect-name'
    rawset(_G, sharding_func_def, function(key) return key % 10 end)

    local callable_obj, err = sharding_func_module.internal.as_callable_object(sharding_func_def,
                                                                              'space_name')
    t.assert_equals(callable_obj, nil)
    t.assert_str_contains(err.err,
            'Wrong sharding function specified in _ddl_sharding_func space for (space_name) space')

    rawset(_G, sharding_func_def, nil)
end

g.test_as_callable_object_invalid_type = function()
    local sharding_func_def = 5

    local callable_obj, err = sharding_func_module.internal.as_callable_object(sharding_func_def,
                                                                              'space_name')
    t.assert_equals(callable_obj, nil)
    t.assert_str_contains(err.err,
            'Wrong sharding function specified in _ddl_sharding_func space for (space_name) space')
end

g.test_is_callable_func = function()
    local sharding_func_obj = function(key) return key end

    local ok = sharding_func_module.internal.is_callable(sharding_func_obj)
    t.assert_equals(ok, true)
end

g.test_is_callable_table_positive = function()
    local sharding_func_table = setmetatable({}, {
        __call = function(_, key) return key end
    })

    local ok = sharding_func_module.internal.is_callable(sharding_func_table)
    t.assert_equals(ok, true)
end

g.test_is_callable_table_negative = function()
    local sharding_func_table = setmetatable({}, {})

    local ok = sharding_func_module.internal.is_callable(sharding_func_table)
    t.assert_equals(ok, false)
end

g.test_is_callable_userdata_positive = function()
    local sharding_func_userdata = newproxy(true)
    local mt = getmetatable(sharding_func_userdata)
    mt.__call = function(_, key) return key end

    local ok = sharding_func_module.internal.is_callable(sharding_func_userdata)
    t.assert_equals(ok, true)
end

g.test_is_callable_userdata_negative = function()
    local sharding_func_userdata = newproxy(true)
    local mt = getmetatable(sharding_func_userdata)
    mt.__call = {}

    local ok = sharding_func_module.internal.is_callable(sharding_func_userdata)
    t.assert_equals(ok, false)
end

g.test_is_callable_cdata = function()
    ffi.cdef[[
        typedef struct
        {
            int data;
        } test_check_struct_t;
    ]]
    ffi.metatype('test_check_struct_t', {
        __call = function(_, key) return key end
    })
    local sharding_func_cdata = ffi.new('test_check_struct_t')

    local ok = sharding_func_module.internal.is_callable(sharding_func_cdata)
    t.assert_equals(ok, true)
end
