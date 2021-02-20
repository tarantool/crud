local fio = require('fio')

local t = require('luatest')
local crud = require('crud')

local helpers = require('test.helper')

local pgroup = helpers.pgroup.new('simple_operations', {
    engine = {'memtx', 'vinyl'},
})

pgroup:set_before_all(function(g)
    g.cluster = helpers.Cluster:new({
        datadir = fio.tempdir(),
        server_command = helpers.entrypoint('srv_simple_operations'),
        use_vshard = true,
        replicasets = helpers.get_test_replicasets(),
        env = {
            ['ENGINE'] = g.params.engine,
        },
    })

    g.cluster:start()
end)

pgroup:set_after_all(function(g) helpers.stop_cluster(g.cluster) end)

pgroup:set_before_each(function(g)
    helpers.truncate_space_on_cluster(g.cluster, 'customers')
end)

pgroup:add('test_non_existent_space', function(g)
    local result, err = g.cluster.main_server.net_box:call(
        'crud.insert', {'non_existent_space', {0, box.NULL, 'Fedor', 59}})

    t.assert_equals(result, nil)
    t.assert_str_contains(err.err, 'Space "non_existent_space" doesn\'t exist')

    -- insert_object
    local result, err = g.cluster.main_server.net_box:call(
        'crud.insert_object', {'non_existent_space', {id = 0, name = 'Fedor', age = 59}})

    t.assert_equals(result, nil)
    t.assert_str_contains(err.err, 'Space "non_existent_space" doesn\'t exist')

    -- get
    local result, err = g.cluster.main_server.net_box:call('crud.get', {'non_existent_space', 0})

    t.assert_equals(result, nil)
    t.assert_str_contains(err.err, 'Space "non_existent_space" doesn\'t exist')

    -- update
    local result, err = g.cluster.main_server.net_box:call(
        'crud.update', {'non_existent_space', 0, {{'+', 'age', 1}}})

    t.assert_equals(result, nil)
    t.assert_str_contains(err.err, 'Space "non_existent_space" doesn\'t exist')

    -- delete
    local result, err = g.cluster.main_server.net_box:call('crud.delete', {'non_existent_space', 0})

    t.assert_equals(result, nil)
    t.assert_str_contains(err.err, 'Space "non_existent_space" doesn\'t exist')

    -- replace
    local result, err = g.cluster.main_server.net_box:call(
        'crud.replace', {'non_existent_space', {0, box.NULL, 'Fedor', 59}})

    t.assert_equals(result, nil)
    t.assert_str_contains(err.err, 'Space "non_existent_space" doesn\'t exist')

    -- replace_object
    local result, err = g.cluster.main_server.net_box:call(
        'crud.replace', {'non_existent_space', {id = 0, name = 'Fedor', age = 59}})

    t.assert_equals(result, nil)
    t.assert_str_contains(err.err, 'Space "non_existent_space" doesn\'t exist')

    -- upsert
    local result, err = g.cluster.main_server.net_box:call(
       'crud.upsert', {'non_existent_space', {0, box.NULL, 'Fedor', 59}, {{'+', 'age', 1}}})

    t.assert_equals(result, nil)
    t.assert_str_contains(err.err, 'Space "non_existent_space" doesn\'t exist')

    -- upsert_object
    local result, err = g.cluster.main_server.net_box:call(
        'crud.upsert', {'non_existent_space', {id = 0, name = 'Fedor', age = 59}, {{'+', 'age', 1}}})

    t.assert_equals(result, nil)
    t.assert_str_contains(err.err, 'Space "non_existent_space" doesn\'t exist')
end)

pgroup:add('test_insert_object_get', function(g)
    -- insert_object
    local result, err = g.cluster.main_server.net_box:call(
        'crud.insert_object', {'customers', {id = 1, name = 'Fedor', age = 59}})

    t.assert_equals(err, nil)
    t.assert_equals(result.metadata, {
        {name = 'id', type = 'unsigned'},
        {name = 'bucket_id', type = 'unsigned'},
        {name = 'name', type = 'string'},
        {name = 'age', type = 'number'},
    })
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, {{id = 1, name = 'Fedor', age = 59, bucket_id = 477}})

    -- get
    local result, err = g.cluster.main_server.net_box:call('crud.get', {'customers', 1})

    t.assert_equals(err, nil)
    t.assert(result ~= nil)
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, {{id = 1, name = 'Fedor', age = 59, bucket_id = 477}})

    -- insert_object again
    local obj, err = g.cluster.main_server.net_box:call(
        'crud.insert_object', {'customers', {id = 1, name = 'Alexander', age = 37}})

    t.assert_equals(obj, nil)
    t.assert_str_contains(err.err, "Duplicate key exists")

    -- bad format
    local obj, err = g.cluster.main_server.net_box:call(
       'crud.insert_object', {'customers', {id = 2, name = 'Alexander'}})

    t.assert_equals(obj, nil)
    t.assert_str_contains(err.err, "Field \"age\" isn't nullable")
end)

pgroup:add('test_insert_get', function(g)
    -- insert
    local result, err = g.cluster.main_server.net_box:call(
        'crud.insert', {'customers', {2, box.NULL, 'Ivan', 20}})

    t.assert_equals(err, nil)
    t.assert_equals(result.metadata, {
        {name = 'id', type = 'unsigned'},
        {name = 'bucket_id', type = 'unsigned'},
        {name = 'name', type = 'string'},
        {name = 'age', type = 'number'},
    })
    t.assert_equals(result.rows, {{2, 401, 'Ivan', 20}})

    -- get
    local result, err = g.cluster.main_server.net_box:call('crud.get', {'customers', 2})

    t.assert_equals(err, nil)
    t.assert(result ~= nil)
    t.assert_equals(result.rows, {{2, 401, 'Ivan', 20}})

    -- insert again
    local obj, err = g.cluster.main_server.net_box:call('crud.insert', {'customers', {2, box.NULL, 'Ivan', 20}})

    t.assert_equals(obj, nil)
    t.assert_str_contains(err.err, "Duplicate key exists")

    -- get non-existent
    local result, err = g.cluster.main_server.net_box:eval([[
        local crud = require('crud')
        return crud.get('customers', 100)
    ]])

    t.assert_equals(err, nil)
    t.assert_equals(#result.rows, 0)
end)

pgroup:add('test_update', function(g)
    -- insert tuple
    local result, err = g.cluster.main_server.net_box:call(
        'crud.insert_object', {'customers', {id = 22, name = 'Leo', age = 72}})

    t.assert_equals(err, nil)
    t.assert_equals(result.metadata, {
        {name = 'id', type = 'unsigned'},
        {name = 'bucket_id', type = 'unsigned'},
        {name = 'name', type = 'string'},
        {name = 'age', type = 'number'},
    })
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, {{id = 22, name = 'Leo', age = 72, bucket_id = 655}})

    -- update age and name fields
    local result, err = g.cluster.main_server.net_box:call('crud.update', {'customers', 22, {
            {'+', 'age', 10},
            {'=', 'name', 'Leo Tolstoy'},
    }})

    t.assert_equals(err, nil)
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, {{id = 22, name = 'Leo Tolstoy', age = 82, bucket_id = 655}})

    -- get
    local result, err = g.cluster.main_server.net_box:call('crud.get', {'customers', 22})

    t.assert_equals(err, nil)
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, {{id = 22, name = 'Leo Tolstoy', age = 82, bucket_id = 655}})

    -- bad key
    local result, err = g.cluster.main_server.net_box:call(
        'crud.update', {'customers', 'bad-key', {{'+', 'age', 10},}})

    t.assert_equals(result, nil)
    t.assert_str_contains(err.err, "Supplied key type of part 0 does not match index part type:")

    -- update by field numbers
    local result, err = g.cluster.main_server.net_box:call('crud.update', {'customers', 22, {
            {'-', 4, 10},
            {'=', 3, 'Leo'}
    }})

    t.assert_equals(err, nil)
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, {{id = 22, name = 'Leo', age = 72, bucket_id = 655}})

    -- get
    local result, err = g.cluster.main_server.net_box:call('crud.get', {'customers', 22})

    t.assert_equals(err, nil)
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, {{id = 22, name = 'Leo', age = 72, bucket_id = 655}})
end)

pgroup:add('test_delete', function(g)
    -- insert tuple
    local result, err = g.cluster.main_server.net_box:call(
        'crud.insert_object', {'customers', {id = 33, name = 'Mayakovsky', age = 36}})

    t.assert_equals(err, nil)
    t.assert_equals(result.metadata, {
        {name = 'id', type = 'unsigned'},
        {name = 'bucket_id', type = 'unsigned'},
        {name = 'name', type = 'string'},
        {name = 'age', type = 'number'},
    })
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, {{id = 33, name = 'Mayakovsky', age = 36, bucket_id = 907}})

    -- delete
    local result, err = g.cluster.main_server.net_box:call('crud.delete', {'customers', 33})

    t.assert_equals(err, nil)
    if g.params.engine == 'memtx' then
        local objects = crud.unflatten_rows(result.rows, result.metadata)
        t.assert_equals(objects, {{id = 33, name = 'Mayakovsky', age = 36, bucket_id = 907}})
    else
        t.assert_equals(#result.rows, 0)
    end

    -- get
    local result, err = g.cluster.main_server.net_box:call('crud.get', {'customers', 33})

    t.assert_equals(err, nil)
    t.assert_equals(#result.rows, 0)

    -- bad key
    local result, err = g.cluster.main_server.net_box:call('crud.delete', {'customers', 'bad-key'})

    t.assert_equals(result, nil)
    t.assert_str_contains(err.err, "Supplied key type of part 0 does not match index part type:")
end)

pgroup:add('test_replace_object', function(g)
    -- get
    local result, err = g.cluster.main_server.net_box:call('crud.get', {'customers', 44})

    t.assert_equals(err, nil)
    t.assert_equals(result.metadata, {
        {name = 'id', type = 'unsigned'},
        {name = 'bucket_id', type = 'unsigned'},
        {name = 'name', type = 'string'},
        {name = 'age', type = 'number'},
    })
    t.assert_equals(#result.rows, 0)

    -- replace_object
    local result, err = g.cluster.main_server.net_box:call(
        'crud.replace_object', {'customers', {id = 44, name = 'John Doe', age = 25}})

    t.assert_equals(err, nil)
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, {{id = 44, name = 'John Doe', age = 25, bucket_id = 2805}})

    -- replace_object
    local result, err = g.cluster.main_server.net_box:call(
        'crud.replace_object', {'customers', {id = 44, name = 'Jane Doe', age = 18}})

    t.assert_equals(err, nil)
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, {{id = 44, name = 'Jane Doe', age = 18, bucket_id = 2805}})
end)

pgroup:add('test_replace', function(g)
    local result, err = g.cluster.main_server.net_box:call(
        'crud.replace', {'customers', {45, box.NULL, 'John Fedor', 99}})

    t.assert_equals(err, nil)
    t.assert_equals(result.metadata, {
        {name = 'id', type = 'unsigned'},
        {name = 'bucket_id', type = 'unsigned'},
        {name = 'name', type = 'string'},
        {name = 'age', type = 'number'},
    })
    t.assert_equals(result.rows, {{45, 392, 'John Fedor', 99}})

    -- replace again
    local result, err = g.cluster.main_server.net_box:call(
        'crud.replace', {'customers', {45, box.NULL, 'John Fedor', 100}})

    t.assert_equals(err, nil)
    t.assert_equals(result.rows, {{45, 392, 'John Fedor', 100}})
end)

pgroup:add('test_upsert_object', function(g)
    -- upsert_object first time
    local result, err = g.cluster.main_server.net_box:call(
        'crud.upsert_object', {'customers', {id = 66, name = 'Jack Sparrow', age = 25}, {
             {'+', 'age', 25},
             {'=', 'name', 'Leo Tolstoy'},
    }})

    t.assert_equals(#result.rows, 0)
    t.assert_equals(result.metadata, {
        {name = 'id', type = 'unsigned'},
        {name = 'bucket_id', type = 'unsigned'},
        {name = 'name', type = 'string'},
        {name = 'age', type = 'number'},
    })
    t.assert_equals(err, nil)

    -- get
    local result, err = g.cluster.main_server.net_box:call('crud.get', {'customers', 66})

    t.assert_equals(err, nil)
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, {{id = 66, name = 'Jack Sparrow', age = 25, bucket_id = 486}})

    -- upsert_object the same query second time when tuple exists
    local result, err = g.cluster.main_server.net_box:call(
       'crud.upsert_object', {'customers', {id = 66, name = 'Jack Sparrow', age = 25}, {
            {'+', 'age', 25},
            {'=', 'name', 'Leo Tolstoy'},
    }})

    t.assert_equals(#result.rows, 0)
    t.assert_equals(err, nil)

    -- get
    local result, err = g.cluster.main_server.net_box:call('crud.get', {'customers', 66})

    t.assert_equals(err, nil)
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, {{id = 66, name = 'Leo Tolstoy', age = 50, bucket_id = 486}})
end)

pgroup:add('test_upsert', function(g)
    -- upsert tuple first time
    local result, err = g.cluster.main_server.net_box:call(
       'crud.upsert', {'customers', {67, box.NULL, 'Saltykov-Shchedrin', 63}, {
                          {'=', 'name', 'Mikhail Saltykov-Shchedrin'},
    }})

    t.assert_equals(#result.rows, 0)
    t.assert_equals(result.metadata, {
        {name = 'id', type = 'unsigned'},
        {name = 'bucket_id', type = 'unsigned'},
        {name = 'name', type = 'string'},
        {name = 'age', type = 'number'},
    })
    t.assert_equals(err, nil)

    -- get
    local result, err = g.cluster.main_server.net_box:call('crud.get', {'customers', 67})

    t.assert_equals(err, nil)
    t.assert_equals(result.rows, {{67, 1143, 'Saltykov-Shchedrin', 63}})

    -- upsert the same query second time when tuple exists
    local result, err = g.cluster.main_server.net_box:call(
       'crud.upsert', {'customers', {67, box.NULL, 'Saltykov-Shchedrin', 63}, {
                          {'=', 'name', 'Mikhail Saltykov-Shchedrin'}}})

    t.assert_equals(#result.rows, 0)
    t.assert_equals(err, nil)

    -- get
    local result, err = g.cluster.main_server.net_box:call('crud.get', {'customers', 67})

    t.assert_equals(err, nil)
    t.assert_equals(result.rows, {{67, 1143, 'Mikhail Saltykov-Shchedrin', 63}})
end)

pgroup:add('test_intermediate_nullable_fields_update', function(g)
    local result, err = g.cluster.main_server.net_box:call(
        'crud.insert', {'developers', {1, box.NULL}})
    t.assert_equals(err, nil)

    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, {
        {
            id = 1,
            bucket_id = 477
        }
    })

    helpers.call_on_servers(g.cluster, {'s1-master', 's2-master'}, function(server)
        server.net_box:call('add_extra_field', {'extra_1'})
        server.net_box:call('add_extra_field', {'extra_2'})
        server.net_box:call('add_extra_field', {'extra_3'})
        server.net_box:call('add_extra_field', {'extra_4'})
        server.net_box:call('add_extra_field', {'extra_5'})
        server.net_box:call('add_extra_field', {'extra_6'})
    end)

    -- TODO: delete this, when issue (https://github.com/tarantool/crud/issues/98) will be closed
    g.cluster.main_server.net_box:call('crud.select',
        {'developers', nil})

    result, err = g.cluster.main_server.net_box:call('crud.update',
        {'developers', 1, {{'=', 'extra_3', 'extra_value_3'}}})

    t.assert_equals(err, nil)
    objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, {
        {
            id = 1,
            bucket_id = 477,
            extra_1 = box.NULL,
            extra_2 = box.NULL,
            extra_3 = 'extra_value_3',
        }
    })

    result, err = g.cluster.main_server.net_box:call('crud.update',
        {'developers', 1, {{'=', 8, 'extra_value_6'}}}) -- update extra_6 field

    t.assert_equals(err, nil)
    objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, {
        {
            id = 1,
            bucket_id = 477,
            extra_1 = box.NULL,
            extra_2 = box.NULL,
            extra_3 = 'extra_value_3',
            extra_4 = box.NULL,
            extra_5 = box.NULL,
            extra_6 = 'extra_value_6'
        }
    })
end)

pgroup:add('test_object_with_nullable_fields', function(g)
    -- Insert
    local result, err = g.cluster.main_server.net_box:call(
        'crud.insert_object', {'tags', {id = 1, is_green = true}})
    t.assert_equals(err, nil)

    -- {1, 477, NULL, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL}
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, {
        {
            bucket_id = 477,
            id = 1,
            is_blue = box.NULL,
            is_correct = box.NULL,
            is_dirty = box.NULL,
            is_green = true,
            is_long = box.NULL,
            is_red = box.NULL,
            is_short = box.NULL,
            is_sweet = box.NULL,
            is_useful = box.NULL,
            is_yellow = box.NULL,
        }
    })

    -- Update
    -- {1, 477, NULL, true, NULL, NULL, true, NULL, NULL, NULL, NULL, NULL}
    -- Shouldn't failed because of https://github.com/tarantool/tarantool/issues/3378
    result, err = g.cluster.main_server.net_box:call(
        'crud.update', {'tags', 1, {{'=', 'is_sweet', true}}})
    t.assert_equals(err, nil)
    objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, {
        {
            bucket_id = 477,
            id = 1,
            is_blue = box.NULL,
            is_correct = box.NULL,
            is_dirty = box.NULL,
            is_green = true,
            is_long = box.NULL,
            is_red = box.NULL,
            is_short = box.NULL,
            is_sweet = true,
            is_useful = box.NULL,
            is_yellow = box.NULL,
        }
    })

    -- Replace
    -- {2, 401, NULL, true, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL}
    result, err = g.cluster.main_server.net_box:call(
        'crud.replace_object', {'tags', {id = 2, is_green = true}})
    t.assert_equals(err, nil)
    objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, {
        {
            bucket_id = 401,
            id = 2,
            is_blue = box.NULL,
            is_correct = box.NULL,
            is_dirty = box.NULL,
            is_green = true,
            is_long = box.NULL,
            is_red = box.NULL,
            is_short = box.NULL,
            is_sweet = box.NULL,
            is_useful = box.NULL,
            is_yellow = box.NULL,
        }
    })

    -- Upsert: first is insert then update
    -- {3, 2804, NULL, NULL, NULL, NULL, NULL, true, NULL, NULL, NULL, NULL}
    local _, err = g.cluster.main_server.net_box:call(
        'crud.upsert_object', {'tags', {id = 3, is_dirty = true}, {
             {'=', 'is_dirty', true},
    }})
    t.assert_equals(err, nil)

    -- {3, 2804, NULL, NULL, NULL, NULL, NULL, true, NULL, true, true, NULL}
    -- Shouldn't failed because of https://github.com/tarantool/tarantool/issues/3378
    _, err = g.cluster.main_server.net_box:call(
        'crud.upsert_object', {'tags', {id = 3, is_dirty = true}, {
             {'=', 'is_useful', true},
    }})
    t.assert_equals(err, nil)

    -- Get
    result, err = g.cluster.main_server.net_box:call('crud.get', {'tags', 3})
    t.assert_equals(err, nil)
    objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, {
        {
            bucket_id = 2804,
            id = 3,
            is_blue = box.NULL,
            is_correct = box.NULL,
            is_dirty = true,
            is_green = box.NULL,
            is_long = box.NULL,
            is_red = box.NULL,
            is_short = box.NULL,
            is_sweet = box.NULL,
            is_useful = true,
            is_yellow = box.NULL,
        }
    })
end)

pgroup:add('test_get_partial_result', function(g)
    -- insert_object
    local result, err = g.cluster.main_server.net_box:call(
            'crud.insert_object', {
                'customers',
                {id = 1, name = 'Elizabeth', age = 24}
            }
    )

    t.assert_equals(err, nil)
    t.assert_equals(result.metadata, {
        {name = 'id', type = 'unsigned'},
        {name = 'bucket_id', type = 'unsigned'},
        {name = 'name', type = 'string'},
        {name = 'age', type = 'number'},
    })
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, {{id = 1, name = 'Elizabeth', age = 24, bucket_id = 477}})

    -- get
    local result, err = g.cluster.main_server.net_box:call(
            'crud.get', {'customers', 1, {fields = {'id', 'name'}}}
    )

    t.assert_equals(err, nil)
    t.assert_equals(result.metadata, {
        {name = 'id', type = 'unsigned'},
        {name = 'name', type = 'string'},
    })
    t.assert_equals(result.rows, {{1, 'Elizabeth'}})
end)

pgroup:add('test_insert_tuple_partial_result', function(g)
    -- insert
    local result, err = g.cluster.main_server.net_box:call( 'crud.insert', {
        'customers', {1, box.NULL, 'Elizabeth', 24}, {fields = {'id', 'name'}}
    })

    t.assert_equals(err, nil)
    t.assert_equals(result.metadata, {
        {name = 'id', type = 'unsigned'},
        {name = 'name', type = 'string'},
    })
    t.assert_equals(result.rows, {{1, 'Elizabeth'}})
end)

pgroup:add('test_insert_object_partial_result', function(g)
    -- insert_object
    local result, err = g.cluster.main_server.net_box:call(
            'crud.insert_object', {
                'customers',
                {id = 1, name = 'Elizabeth', age = 24},
                {fields = {'id', 'name'}}
            }
    )

    t.assert_equals(err, nil)
    t.assert_equals(result.metadata, {
        {name = 'id', type = 'unsigned'},
        {name = 'name', type = 'string'},
    })
    t.assert_equals(result.rows, {{1, 'Elizabeth'}})
end)

pgroup:add('test_delete_partial_result', function(g)
    -- insert_object
    local result, err = g.cluster.main_server.net_box:call(
            'crud.insert_object', {
                'customers',
                {id = 1, name = 'Elizabeth', age = 24}
            }
    )

    t.assert_equals(err, nil)
    t.assert_equals(result.metadata, {
        {name = 'id', type = 'unsigned'},
        {name = 'bucket_id', type = 'unsigned'},
        {name = 'name', type = 'string'},
        {name = 'age', type = 'number'},
    })
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, {{id = 1, name = 'Elizabeth', age = 24, bucket_id = 477}})

    -- delete
    local result, err = g.cluster.main_server.net_box:call(
            'crud.delete', {
                'customers', 1, {fields = {'id', 'name'}}
            }
    )

    t.assert_equals(err, nil)
    t.assert_equals(result.metadata, {
        {name = 'id', type = 'unsigned'},
        {name = 'name', type = 'string'},
    })

    if g.params.engine == 'memtx' then
        local objects = crud.unflatten_rows(result.rows, result.metadata)
        t.assert_equals(objects, {{id = 1, name = 'Elizabeth'}})
    else
        t.assert_equals(#result.rows, 0)
    end
end)

pgroup:add('test_update_partial_result', function(g)
    -- insert_object
    local result, err = g.cluster.main_server.net_box:call(
            'crud.insert_object', {
                'customers',
                {id = 1, name = 'Elizabeth', age = 23}
            }
    )

    t.assert_equals(err, nil)
    t.assert_equals(result.metadata, {
        {name = 'id', type = 'unsigned'},
        {name = 'bucket_id', type = 'unsigned'},
        {name = 'name', type = 'string'},
        {name = 'age', type = 'number'},
    })
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, {{id = 1, name = 'Elizabeth', age = 23, bucket_id = 477}})

    -- update
    local result, err = g.cluster.main_server.net_box:call('crud.update', {
        'customers', 1, {{'+', 'age', 1},},  {fields = {'id', 'age'}}
    })

    t.assert_equals(err, nil)
    t.assert_equals(result.metadata, {
        {name = 'id', type = 'unsigned'},
        {name = 'age', type = 'number'},
    })
    t.assert_equals(result.rows, {{1, 24}})
end)

pgroup:add('test_replace_tuple_partial_result', function(g)
    local result, err = g.cluster.main_server.net_box:call(
            'crud.replace', {
                'customers',
                {1, box.NULL, 'Elizabeth', 23},
                {fields = {'id', 'age'}}
            }
    )

    t.assert_equals(err, nil)
    t.assert_equals(result.metadata, {
        {name = 'id', type = 'unsigned'},
        {name = 'age', type = 'number'},
    })
    t.assert_equals(result.rows, {{1, 23}})

    -- replace again
    local result, err = g.cluster.main_server.net_box:call(
            'crud.replace', {
                'customers',
                {1, box.NULL, 'Elizabeth', 24},
                {fields = {'id', 'age'}}
            }
    )

    t.assert_equals(err, nil)
    t.assert_equals(result.metadata, {
        {name = 'id', type = 'unsigned'},
        {name = 'age', type = 'number'},
    })
    t.assert_equals(result.rows, {{1, 24}})
end)

pgroup:add('test_replace_object_partial_result', function(g)
    -- get
    local result, err = g.cluster.main_server.net_box:call('crud.get', {
        'customers', 1
    })

    t.assert_equals(err, nil)
    t.assert_equals(#result.rows, 0)
    t.assert_equals(result.metadata, {
        {name = 'id', type = 'unsigned'},
        {name = 'bucket_id', type = 'unsigned'},
        {name = 'name', type = 'string'},
        {name = 'age', type = 'number'},
    })

    -- replace_object
    local result, err = g.cluster.main_server.net_box:call(
            'crud.replace_object', {
                'customers',
                {id = 1, name = 'Elizabeth', age = 23},
                {fields = {'id', 'age'}}
            }
    )

    t.assert_equals(err, nil)
    t.assert_equals(result.metadata, {
        {name = 'id', type = 'unsigned'},
        {name = 'age', type = 'number'},
    })
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, {{id = 1, age = 23}})

    -- replace_object
    local result, err = g.cluster.main_server.net_box:call(
            'crud.replace_object', {
                'customers',
                {id = 1, name = 'Elizabeth', age = 24},
                {fields = {'id', 'age'}}
            }
    )

    t.assert_equals(err, nil)
    t.assert_equals(result.metadata, {
        {name = 'id', type = 'unsigned'},
        {name = 'age', type = 'number'},
    })
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, {{id = 1, age = 24}})
end)

pgroup:add('test_upsert_tuple_partial_result', function(g)
    -- upsert tuple first time
    local result, err = g.cluster.main_server.net_box:call('crud.upsert', {
        'customers',
        {1, box.NULL, 'Elizabeth', 23},
        {{'+', 'age', 1},},
        {fields = {'id', 'age'}}
    })

    t.assert_equals(err, nil)
    t.assert_equals(#result.rows, 0)
    t.assert_equals(result.metadata, {
        {name = 'id', type = 'unsigned'},
        {name = 'age', type = 'number'},
    })

    -- upsert second time
    result, err = g.cluster.main_server.net_box:call('crud.upsert', {
        'customers',
        {1, box.NULL, 'Elizabeth', 23},
        {{'+', 'age', 1},},
        {fields = {'id', 'age'}}
    })

    t.assert_equals(err, nil)
    t.assert_equals(#result.rows, 0)
    t.assert_equals(result.metadata, {
        {name = 'id', type = 'unsigned'},
        {name = 'age', type = 'number'},
    })
end)

pgroup:add('test_upsert_object_partial_result', function(g)
    -- upsert_object first time
    local result, err = g.cluster.main_server.net_box:call('crud.upsert_object', {
            'customers',
            {id = 1, name = 'Elizabeth', age = 23},
            {{'+', 'age', 1},},
            {fields = {'id', 'age'}}
    })

    t.assert_equals(err, nil)
    t.assert_equals(#result.rows, 0)
    t.assert_equals(result.metadata, {
        {name = 'id', type = 'unsigned'},
        {name = 'age', type = 'number'},
    })

    -- upsert_object second time
    result, err = g.cluster.main_server.net_box:call('crud.upsert_object', {
        'customers',
        {id = 1, name = 'Elizabeth', age = 23},
        {{'+', 'age', 1},},
        {fields = {'id', 'age'}}
    })

    t.assert_equals(err, nil)
    t.assert_equals(#result.rows, 0)
    t.assert_equals(result.metadata, {
        {name = 'id', type = 'unsigned'},
        {name = 'age', type = 'number'},
    })
end)

pgroup:add('test_partial_result_bad_input', function(g)
    -- insert_object
    local result, err = g.cluster.main_server.net_box:call(
            'crud.insert_object', {
                'customers',
                {id = 1, name = 'Elizabeth', age = 24},
                {fields = {'id', 'lastname', 'name'}}
            }
    )

    t.assert_equals(result, nil)
    t.assert_str_contains(err.err, 'Space format doesn\'t contain field named "lastname"')

    -- get
    result, err = g.cluster.main_server.net_box:call('crud.get', {
        'customers', 1, {fields = {'id', 'lastname', 'name'}}
    })

    t.assert_equals(result, nil)
    t.assert_str_contains(err.err, 'Space format doesn\'t contain field named "lastname"')

    -- update
    result, err = g.cluster.main_server.net_box:call('crud.update', {
        'customers', 1, {{'+', 'age', 1},},
        {fields = {'id', 'lastname', 'age'}}
    })

    t.assert_equals(result, nil)
    t.assert_str_contains(err.err, 'Space format doesn\'t contain field named "lastname"')

    -- delete
    result, err = g.cluster.main_server.net_box:call(
            'crud.delete', {
                'customers', 1,
                {fields = {'id', 'lastname', 'name'}}
            }
    )

    t.assert_equals(result, nil)
    t.assert_str_contains(err.err, 'Space format doesn\'t contain field named "lastname"')

    -- replace
    result, err = g.cluster.main_server.net_box:call(
            'crud.replace', {
                'customers',
                {1, box.NULL, 'Elizabeth', 23},
                {fields = {'id', 'lastname', 'age'}}
            }
    )

    t.assert_equals(result, nil)
    t.assert_str_contains(err.err, 'Space format doesn\'t contain field named "lastname"')

    -- replace_object
    local result, err = g.cluster.main_server.net_box:call(
            'crud.replace_object', {
                'customers',
                {id = 1, name = 'Elizabeth', age = 24},
                {fields = {'id', 'lastname', 'age'}}
            }
    )

    t.assert_equals(result, nil)
    t.assert_str_contains(err.err, 'Space format doesn\'t contain field named "lastname"')

    -- upsert
    result, err = g.cluster.main_server.net_box:call('crud.upsert', {
        'customers',
        {1, box.NULL, 'Elizabeth', 24},
        {{'+', 'age', 1},},
        {fields = {'id', 'lastname', 'age'}}
    })

    t.assert_equals(result, nil)
    t.assert_str_contains(err.err, 'Space format doesn\'t contain field named "lastname"')

    -- upsert_object
    result, err = g.cluster.main_server.net_box:call('crud.upsert_object', {
        'customers',
        {id = 1, name = 'Elizabeth', age = 24},
        {{'+', 'age', 1},},
        {fields = {'id', 'lastname', 'age'}}
    })

    t.assert_equals(result, nil)
    t.assert_str_contains(err.err, 'Space format doesn\'t contain field named "lastname"')
end)

