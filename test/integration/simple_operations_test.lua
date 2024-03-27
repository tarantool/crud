local t = require('luatest')
local crud = require('crud')

local helpers = require('test.helper')

local pgroup = t.group('simple_operations', helpers.backend_matrix({
    {engine = 'memtx'},
}))

pgroup.before_all(function(g)
    helpers.start_default_cluster(g, 'srv_simple_operations')
end)

pgroup.after_all(function(g)
    helpers.stop_cluster(g.cluster, g.params.backend)
end)

pgroup.before_each(function(g)
    helpers.truncate_space_on_cluster(g.cluster, 'customers')
    helpers.truncate_space_on_cluster(g.cluster, 'tags')
    helpers.truncate_space_on_cluster(g.cluster, 'notebook')
    helpers.reset_sequence_on_cluster(g.cluster, 'local_id')
end)

pgroup.test_non_existent_space = function(g)
    local result, err = g.router:call(
        'crud.insert', {'non_existent_space', {0, box.NULL, 'Fedor', 59}})

    t.assert_equals(result, nil)
    t.assert_str_contains(err.err, 'Space "non_existent_space" doesn\'t exist')

    -- insert_object
    local result, err = g.router:call(
        'crud.insert_object', {'non_existent_space', {id = 0, name = 'Fedor', age = 59}})

    t.assert_equals(result, nil)
    t.assert_str_contains(err.err, 'Space "non_existent_space" doesn\'t exist')

    -- get
    local result, err = g.router:call('crud.get', {'non_existent_space', 0})

    t.assert_equals(result, nil)
    t.assert_str_contains(err.err, 'Space "non_existent_space" doesn\'t exist')

    -- update
    local result, err = g.router:call(
        'crud.update', {'non_existent_space', 0, {{'+', 'age', 1}}})

    t.assert_equals(result, nil)
    t.assert_str_contains(err.err, 'Space "non_existent_space" doesn\'t exist')

    -- delete
    local result, err = g.router:call('crud.delete', {'non_existent_space', 0})

    t.assert_equals(result, nil)
    t.assert_str_contains(err.err, 'Space "non_existent_space" doesn\'t exist')

    -- replace
    local result, err = g.router:call(
        'crud.replace', {'non_existent_space', {0, box.NULL, 'Fedor', 59}})

    t.assert_equals(result, nil)
    t.assert_str_contains(err.err, 'Space "non_existent_space" doesn\'t exist')

    -- replace_object
    local result, err = g.router:call(
        'crud.replace', {'non_existent_space', {id = 0, name = 'Fedor', age = 59}})

    t.assert_equals(result, nil)
    t.assert_str_contains(err.err, 'Space "non_existent_space" doesn\'t exist')

    -- upsert
    local result, err = g.router:call(
       'crud.upsert', {'non_existent_space', {0, box.NULL, 'Fedor', 59}, {{'+', 'age', 1}}})

    t.assert_equals(result, nil)
    t.assert_str_contains(err.err, 'Space "non_existent_space" doesn\'t exist')

    -- upsert_object
    local result, err = g.router:call(
        'crud.upsert', {'non_existent_space', {id = 0, name = 'Fedor', age = 59}, {{'+', 'age', 1}}})

    t.assert_equals(result, nil)
    t.assert_str_contains(err.err, 'Space "non_existent_space" doesn\'t exist')
end

pgroup.test_insert_object_get = function(g)
    -- insert_object
    local result, err = g.router:call(
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
    local result, err = g.router:call('crud.get', {
        'customers', 1, {mode = 'write'},
    })

    t.assert_equals(err, nil)
    t.assert_not_equals(result, nil)
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, {{id = 1, name = 'Fedor', age = 59, bucket_id = 477}})

    -- insert_object again
    local obj, err = g.router:call(
        'crud.insert_object', {'customers', {id = 1, name = 'Alexander', age = 37}})

    t.assert_equals(obj, nil)
    t.assert_str_contains(err.err, "Duplicate key exists")

    -- bad format
    local obj, err = g.router:call(
       'crud.insert_object', {'customers', {id = 2, name = 'Alexander'}})

    t.assert_equals(obj, nil)
    t.assert_str_contains(err.err, "Field \"age\" isn't nullable")
end

pgroup.test_insert_get = function(g)
    -- insert
    local result, err = g.router:call(
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
    local result, err = g.router:call('crud.get', {
        'customers', 2, {mode = 'write'},
    })

    t.assert_equals(err, nil)
    t.assert_not_equals(result, nil)
    t.assert_equals(result.rows, {{2, 401, 'Ivan', 20}})

    -- insert again
    local obj, err = g.router:call('crud.insert', {'customers', {2, box.NULL, 'Ivan', 20}})

    t.assert_equals(obj, nil)
    t.assert_str_contains(err.err, "Duplicate key exists")

    -- get non-existent
    local result, err = g.router:eval([[
        local crud = require('crud')
        return crud.get('customers', 100, {mode = 'write'})
    ]])

    t.assert_equals(err, nil)
    t.assert_equals(#result.rows, 0)
end

pgroup.test_update = function(g)
    -- insert tuple
    local result, err = g.router:call(
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
    local result, err = g.router:call('crud.update', {'customers', 22, {
            {'+', 'age', 10},
            {'=', 'name', 'Leo Tolstoy'},
    }})

    t.assert_equals(err, nil)
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, {{id = 22, name = 'Leo Tolstoy', age = 82, bucket_id = 655}})

    -- get
    local result, err = g.router:call('crud.get', {
        'customers', 22, {mode = 'write'},
    })

    t.assert_equals(err, nil)
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, {{id = 22, name = 'Leo Tolstoy', age = 82, bucket_id = 655}})

    -- bad key
    local result, err = g.router:call(
        'crud.update', {'customers', 'bad-key', {{'+', 'age', 10},}})

    t.assert_equals(result, nil)
    t.assert_str_contains(err.err, "Supplied key type of part 0 does not match index part type:")

    -- update by field numbers
    local result, err = g.router:call('crud.update', {'customers', 22, {
            {'-', 4, 10},
            {'=', 3, 'Leo'}
    }})

    t.assert_equals(err, nil)
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, {{id = 22, name = 'Leo', age = 72, bucket_id = 655}})

    -- get
    local result, err = g.router:call('crud.get', {
        'customers', 22, {mode = 'write'},
    })

    t.assert_equals(err, nil)
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, {{id = 22, name = 'Leo', age = 72, bucket_id = 655}})
end

pgroup.test_delete = function(g)
    -- insert tuple
    local result, err = g.router:call(
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
    local result, err = g.router:call('crud.delete', {'customers', 33})

    t.assert_equals(err, nil)
    if g.params.engine == 'memtx' then
        local objects = crud.unflatten_rows(result.rows, result.metadata)
        t.assert_equals(objects, {{id = 33, name = 'Mayakovsky', age = 36, bucket_id = 907}})
    else
        t.assert_equals(#result.rows, 0)
    end

    -- get
    local result, err = g.router:call('crud.get', {
        'customers', 33, {mode = 'write'},
    })

    t.assert_equals(err, nil)
    t.assert_equals(#result.rows, 0)

    -- bad key
    local result, err = g.router:call('crud.delete', {'customers', 'bad-key'})

    t.assert_equals(result, nil)
    t.assert_str_contains(err.err, "Supplied key type of part 0 does not match index part type:")
end

pgroup.test_replace_object = function(g)
    -- get
    local result, err = g.router:call('crud.get', {
        'customers', 44, {mode = 'write'},
    })

    t.assert_equals(err, nil)
    t.assert_equals(result.metadata, {
        {name = 'id', type = 'unsigned'},
        {name = 'bucket_id', type = 'unsigned'},
        {name = 'name', type = 'string'},
        {name = 'age', type = 'number'},
    })
    t.assert_equals(#result.rows, 0)

    -- replace_object
    local result, err = g.router:call(
        'crud.replace_object', {'customers', {id = 44, name = 'John Doe', age = 25}})

    t.assert_equals(err, nil)
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, {{id = 44, name = 'John Doe', age = 25, bucket_id = 2805}})

    -- replace_object
    local result, err = g.router:call(
        'crud.replace_object', {'customers', {id = 44, name = 'Jane Doe', age = 18}})

    t.assert_equals(err, nil)
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, {{id = 44, name = 'Jane Doe', age = 18, bucket_id = 2805}})
end

pgroup.test_replace = function(g)
    local result, err = g.router:call(
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
    local result, err = g.router:call(
        'crud.replace', {'customers', {45, box.NULL, 'John Fedor', 100}})

    t.assert_equals(err, nil)
    t.assert_equals(result.rows, {{45, 392, 'John Fedor', 100}})
end

pgroup.test_upsert_object = function(g)
    -- upsert_object first time
    local result, err = g.router:call(
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
    local result, err = g.router:call('crud.get', {
        'customers', 66, {mode = 'write'},
    })

    t.assert_equals(err, nil)
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, {{id = 66, name = 'Jack Sparrow', age = 25, bucket_id = 486}})

    -- upsert_object the same query second time when tuple exists
    local result, err = g.router:call(
       'crud.upsert_object', {'customers', {id = 66, name = 'Jack Sparrow', age = 25}, {
            {'+', 'age', 25},
            {'=', 'name', 'Leo Tolstoy'},
    }})

    t.assert_equals(#result.rows, 0)
    t.assert_equals(err, nil)

    -- get
    local result, err = g.router:call('crud.get', {
        'customers', 66, {mode = 'write'},
    })

    t.assert_equals(err, nil)
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, {{id = 66, name = 'Leo Tolstoy', age = 50, bucket_id = 486}})
end

pgroup.test_upsert = function(g)
    -- upsert tuple first time
    local result, err = g.router:call(
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
    local result, err = g.router:call('crud.get', {
        'customers', 67, {mode = 'write'},
    })

    t.assert_equals(err, nil)
    t.assert_equals(result.rows, {{67, 1143, 'Saltykov-Shchedrin', 63}})

    -- upsert the same query second time when tuple exists
    local result, err = g.router:call(
       'crud.upsert', {'customers', {67, box.NULL, 'Saltykov-Shchedrin', 63}, {
                          {'=', 'name', 'Mikhail Saltykov-Shchedrin'}}})

    t.assert_equals(#result.rows, 0)
    t.assert_equals(err, nil)

    -- get
    local result, err = g.router:call('crud.get', {
        'customers', 67, {mode = 'write'},
    })

    t.assert_equals(err, nil)
    t.assert_equals(result.rows, {{67, 1143, 'Mikhail Saltykov-Shchedrin', 63}})
end

pgroup.test_intermediate_nullable_fields_update = function(g)
    local result, err = g.router:call(
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
        for i = 1, 12 do
            server.net_box:call('add_extra_field', {'developers', 'extra_' .. tostring(i)})
        end
    end)

    local result, err = g.router:call('crud.update',
        {'developers', 1, {{'=', 'extra_3', { a = { b = {} } } }}, {fetch_latest_metadata = true}})
    t.assert_equals(err, nil)
    objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, {
        {
            id = 1,
            bucket_id = 477,
            extra_1 = box.NULL,
            extra_2 = box.NULL,
            extra_3 = {a = {b = {}}},
        }
    })

    -- Test uses jsonpaths so it should be run for version 2.3+
    -- where jsonpaths are supported (https://github.com/tarantool/tarantool/issues/1261).
    -- However since 2.8 Tarantool could update intermediate nullable fields
    -- (https://github.com/tarantool/tarantool/issues/3378).
    -- So before 2.8 update returns an error but after it update is correct.
    if helpers.tarantool_version_at_least(2, 8) then
        local _, err = g.router:call('crud.update',
            {'developers', 1, {{'=', '[5].a.b[1]', 3}, {'=', 'extra_5', 'extra_value_5'}}})
        t.assert_equals(err, nil)
    elseif helpers.tarantool_version_at_least(2, 3) then
        local _, err = g.router:call('crud.update',
            {'developers', 1, {{'=', '[5].a.b[1]', 3}, {'=', 'extra_5', 'extra_value_5'}}})
        t.assert_equals(err.err, "Failed to update: Field ''extra_5'' was not found in the tuple")
    end

    result, err = g.router:call('crud.update',
        {'developers', 1, {{'=', 5, 'extra_value_3'}, {'=', 7, 'extra_value_5'}}})
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
            extra_5 = 'extra_value_5',
        }
    })

    result, err = g.router:call('crud.update',
        {'developers', 1, {
            {'=', 14, 'extra_value_12'},
            {'=', 'extra_9', 'extra_value_9'},
            {'=', 'extra_3', 'updated_extra_value_3'}
        }
    })

    t.assert_equals(err, nil)
    objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, {
        {
            id = 1,
            bucket_id = 477,
            extra_1 = box.NULL,
            extra_2 = box.NULL,
            extra_3 = 'updated_extra_value_3',
            extra_4 = box.NULL,
            extra_5 = 'extra_value_5',
            extra_6 = box.NULL,
            extra_7 = box.NULL,
            extra_8 = box.NULL,
            extra_9 = 'extra_value_9',
            extra_10 = box.NULL,
            extra_11 = box.NULL,
            extra_12 = 'extra_value_12'
        }
    })
end

local gh_236_metadata_field_name = {
    crud_update = 'extra_gh_236_update',
    crud_insert = 'extra_gh_236_insert',
    crud_insert_object = 'extra_gh_236_insert_object',
    crud_insert_many = 'extra_gh_236_insert_many',
    crud_insert_object_many = 'extra_gh_236_insert_object_many',
    crud_replace = 'extra_gh_236_replace',
    crud_replace_object = 'extra_gh_236_replace_object',
    crud_replace_many = 'extra_gh_236_replace_many',
    crud_replace_object_many = 'extra_gh_236_replace_object_many',
    crud_get = 'extra_gh_236_get',
    crud_delete = 'extra_gh_236_delete',
    crud_upsert = 'extra_gh_236_upsert',
    crud_upsert_object = 'extra_gh_236_upsert_object',
    crud_upsert_many = 'extra_gh_236_upsert_many',
    crud_upsert_object_many = 'extra_gh_236_upsert_object_many',
    crud_max = 'extra_gh_236_max',
    crud_min = 'extra_gh_236_min',
    crud_select = 'extra_gh_236_select',
    crud_pairs = 'extra_gh_236_pairs',
}

local gh_236_cases = {
    crud_update = {
        operation_name = 'crud.update',
        input = {'countries', 3,
            {{'=', gh_236_metadata_field_name.crud_update, 'testing'}},
            {fetch_latest_metadata = true}},
        need_pre_insert_data = true,
    },
    crud_insert = {
        operation_name = 'crud.insert',
        input = {'countries', {3, box.NULL, 'vatican', 825},
            {fetch_latest_metadata = true}},
        need_pre_insert_data = false,
    },
    crud_insert_object = {
        operation_name = 'crud.insert_object',
        input = {'countries',
            {id=3, bucket_id = box.NULL, name = 'vatican', population = 825},
            {fetch_latest_metadata = true}},
        need_pre_insert_data = false,
    },
    crud_insert_many = {
        operation_name = 'crud.insert_many',
        input = {'countries', {{3, box.NULL, 'vatican', 825}},
            {fetch_latest_metadata = true}},
        need_pre_insert_data = false,
    },
    crud_insert_object_many = {
        operation_name = 'crud.insert_object_many',
        input = {'countries',
            {{id=3, bucket_id = box.NULL, name = 'vatican', population = 825}},
            {fetch_latest_metadata = true}},
        need_pre_insert_data = false,
    },
    crud_replace = {
        operation_name = 'crud.replace',
        input = {'countries', {3, box.NULL, 'vatican', 825},
            {fetch_latest_metadata = true}},
        need_pre_insert_data = true,
    },
    crud_replace_object = {
        operation_name = 'crud.replace_object',
        input = {'countries',
            {id=3, bucket_id = box.NULL, name = 'vatican', population = 825},
            {fetch_latest_metadata = true}},
        need_pre_insert_data = true,
    },
    crud_replace_many = {
        operation_name = 'crud.replace_many',
        input = {'countries', {{3, box.NULL, 'vatican', 825}},
            {fetch_latest_metadata = true}},
        need_pre_insert_data = true,
    },
    crud_replace_object_many = {
        operation_name = 'crud.replace_object_many',
        input = {'countries',
            {{id=3, bucket_id = box.NULL, name = 'vatican', population = 825}},
            {fetch_latest_metadata = true}},
        need_pre_insert_data = true,
    },
    crud_get = {
        operation_name = 'crud.get',
        input = {'countries', 3, {fetch_latest_metadata = true, mode = 'write'}},
        need_pre_insert_data = true,
    },
    crud_delete = {
        operation_name = 'crud.delete',
        input = {'countries', 3, {fetch_latest_metadata = true}},
        need_pre_insert_data = true,
    },
    crud_upsert = {
        operation_name = 'crud.upsert',
        input = {'countries',
            {3, box.NULL, 'vatican', 825}, {{'+', 'population', 1}},
            {fetch_latest_metadata = true}},
        need_pre_insert_data = false,
    },
    crud_upsert_object = {
        operation_name = 'crud.upsert_object',
        input = {'countries',
            {id=3, bucket_id = box.NULL, name = 'vatican', population = 825},
            {{'+', 'population', 1}},
            {fetch_latest_metadata = true}},
        need_pre_insert_data = false,
    },
    crud_upsert_many = {
        operation_name = 'crud.upsert_many',
        input = {'countries',
        {
            {{3, box.NULL, 'vatican', 825},
            {{'+', 'population', 1}}}
        },
        {fetch_latest_metadata = true}},
        need_pre_insert_data = false,
    },
    crud_upsert_object_many = {
        operation_name = 'crud.upsert_object_many',
        input = {'countries',
        {
            {
                {id=3, bucket_id = box.NULL, name = 'vatican', population = 825},
                {{'+', 'population', 1}}
            }
        },
        {fetch_latest_metadata = true}},
        need_pre_insert_data = false,
    },
    crud_max = {
        operation_name = 'crud.max',
        input = {'countries', 'id', {fetch_latest_metadata = true, mode = 'write'}},
        need_pre_insert_data = true,
    },
    crud_min = {
        operation_name = 'crud.min',
        input = {'countries', 'id', {fetch_latest_metadata = true, mode = 'write'}},
        need_pre_insert_data = true,
    },
    crud_select = {
        operation_name = 'crud.select',
        input = {'countries', {}, {fetch_latest_metadata = true, mode = 'write'}},
        need_pre_insert_data = true,
    },
    crud_pairs = {
        operation_name = 'crud.pairs',
        input = { --[[ hardcoded inside eval --]] },
        need_pre_insert_data = true,
    },
}

local gh_236_case_prepare = function(g)
    helpers.call_on_servers(g.cluster, {'s1-master', 's2-master'},
            function(server)
                server.net_box:call('create_space_for_gh_326_cases')
    end)
end

local gh_236_case_after = function(g)
    helpers.call_on_servers(g.cluster, {'s1-master', 's2-master'},
            function(server)
                server.net_box:call('drop_space_for_gh_326_cases')
    end)
end

for case_name, case in pairs(gh_236_cases) do
    -- These tests check the relevance of metadata when crud DML operation working.
    -- The reproduction of the bug in the issue [1] was detected when working
    -- with bucket_id 2804 (id = 3 at countries space), which base on another
    -- storage relative to bucket 477 (id = 1 at countries space).
    -- Related to the issue [2].
    -- [1] https://github.com/tarantool/crud/issues/236
    -- [2] https://github.com/tarantool/crud/issues/361

    local test_name = ('test_gh_236_dml_operation_return_actual_metadata_%s'):format(case_name)

    pgroup.before_test(test_name, gh_236_case_prepare)
    pgroup[test_name] = function(g)
        local result, err

        -- Perform request to fetch initial space format.
        result, err = g.router:call(
            'crud.delete', {'countries', 3})
        t.assert_not_equals(result, nil)
        t.assert_equals(err, nil)

        if case.need_pre_insert_data then
            result, err = g.router:call(
                'crud.insert', {'countries', {3, box.NULL, 'vatican', 825, 'extra_test_data'}})
            t.assert_equals(err, nil)
            t.assert_not_equals(result, nil)
        end

        -- Schema migration.
        helpers.call_on_servers(g.cluster, {'s1-master', 's2-master'},
            function(server)
                server.net_box:call('add_extra_field',
                    {'countries', gh_236_metadata_field_name[case_name]})
        end)

        if case.operation_name ~= 'crud.pairs' then
            result, err = g.router:call(case.operation_name, case.input)
            t.assert_equals(err, nil)
            local found_extra_field = false
            for _, v in pairs(result.metadata) do
                if v.name == gh_236_metadata_field_name[case_name] then
                    found_extra_field = true
                end
            end
            t.assert_equals(found_extra_field, true,
                string.format('cannot find the expected metadata for case: %s',
                            case.operation_name))
        else
            local object = g.router:eval([[
                local objects = {}
                for _, object in crud.pairs(
                    'countries',
                    nil,
                    {fetch_latest_metadata = true, use_tomap = true, mode = 'write'}
                ) do
                    table.insert(objects, object)
                end
                return objects[1]
            ]])
            local found_extra_field = false
            for field_name, _ in pairs(object) do
                if field_name == gh_236_metadata_field_name[case_name] then
                    found_extra_field = true
                end
            end
            t.assert_equals(found_extra_field, true,
                string.format('cannot find the expected metadata for case: %s',
                              case.operation_name))
        end
    end
    pgroup.after_test(test_name, gh_236_case_after)
end

pgroup.test_object_with_nullable_fields = function(g)
    -- Insert
    local result, err = g.router:call(
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
    result, err = g.router:call(
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
    result, err = g.router:call(
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
    local _, err = g.router:call(
        'crud.upsert_object', {'tags', {id = 3, is_dirty = true}, {
             {'=', 'is_dirty', true},
    }})
    t.assert_equals(err, nil)

    -- {3, 2804, NULL, NULL, NULL, NULL, NULL, true, NULL, true, true, NULL}
    -- Shouldn't failed because of https://github.com/tarantool/tarantool/issues/3378
    _, err = g.router:call(
        'crud.upsert_object', {'tags', {id = 3, is_dirty = true}, {
             {'=', 'is_useful', true},
    }})
    t.assert_equals(err, nil)

    -- Get
    result, err = g.router:call('crud.get', {
        'tags', 3, {mode = 'write'},
    })
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
end

pgroup.test_get_partial_result = function(g)
    -- insert_object
    local result, err = g.router:call(
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
    local result, err = g.router:call('crud.get', {
        'customers', 1, {fields = {'id', 'name'}, mode = 'write'},
    })

    t.assert_equals(err, nil)
    t.assert_equals(result.metadata, {
        {name = 'id', type = 'unsigned'},
        {name = 'name', type = 'string'},
    })
    t.assert_equals(result.rows, {{1, 'Elizabeth'}})
end

pgroup.test_insert_tuple_partial_result = function(g)
    -- insert
    local result, err = g.router:call( 'crud.insert', {
        'customers', {1, box.NULL, 'Elizabeth', 24}, {fields = {'id', 'name'}}
    })

    t.assert_equals(err, nil)
    t.assert_equals(result.metadata, {
        {name = 'id', type = 'unsigned'},
        {name = 'name', type = 'string'},
    })
    t.assert_equals(result.rows, {{1, 'Elizabeth'}})
end

pgroup.test_insert_object_partial_result = function(g)
    -- insert_object
    local result, err = g.router:call(
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
end

pgroup.test_delete_partial_result = function(g)
    -- insert_object
    local result, err = g.router:call(
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
    local result, err = g.router:call(
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
end

pgroup.test_update_partial_result = function(g)
    -- insert_object
    local result, err = g.router:call(
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
    local result, err = g.router:call('crud.update', {
        'customers', 1, {{'+', 'age', 1},},  {fields = {'id', 'age'}}
    })

    t.assert_equals(err, nil)
    t.assert_equals(result.metadata, {
        {name = 'id', type = 'unsigned'},
        {name = 'age', type = 'number'},
    })
    t.assert_equals(result.rows, {{1, 24}})
end

pgroup.test_replace_tuple_partial_result = function(g)
    local result, err = g.router:call(
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
    local result, err = g.router:call(
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
end

pgroup.test_replace_object_partial_result = function(g)
    -- get
    local result, err = g.router:call('crud.get', {
        'customers', 1, {mode = 'write'},
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
    local result, err = g.router:call(
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
    local result, err = g.router:call(
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
end

pgroup.test_upsert_tuple_partial_result = function(g)
    -- upsert tuple first time
    local result, err = g.router:call('crud.upsert', {
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
    result, err = g.router:call('crud.upsert', {
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
end

pgroup.test_upsert_object_partial_result = function(g)
    -- upsert_object first time
    local result, err = g.router:call('crud.upsert_object', {
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
    result, err = g.router:call('crud.upsert_object', {
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
end

pgroup.test_partial_result_with_nullable_fields = function(g)
    -- Insert
    local result, err = g.router:call(
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

    local result, err = g.router:call('crud.get', {
        'tags', 1, {fields = {'id', 'is_sweet', 'is_green'}, mode = 'write'},
    })

    t.assert_equals(err, nil)
    t.assert_equals(result.metadata, {
        {name = 'id', type = 'unsigned'},
        {name = 'is_sweet', type = 'boolean', is_nullable = true},
        {name = 'is_green', type = 'boolean', is_nullable = true},
    })
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, {{id = 1, is_sweet = box.NULL, is_green = true}})
end

pgroup.test_partial_result_bad_input = function(g)
    -- insert_object
    local result, err = g.router:call(
            'crud.insert_object', {
                'customers',
                {id = 1, name = 'Elizabeth', age = 24},
                {fields = {'id', 'lastname', 'name'}}
            }
    )

    t.assert_equals(result, nil)
    t.assert_str_contains(err.err, 'Space format doesn\'t contain field named "lastname"')

    -- get
    result, err = g.router:call('crud.get', {
        'customers', 1, {fields = {'id', 'lastname', 'name'}, mode = 'write'},
    })

    t.assert_equals(result, nil)
    t.assert_str_contains(err.err, 'Space format doesn\'t contain field named "lastname"')

    -- update
    result, err = g.router:call('crud.update', {
        'customers', 1, {{'+', 'age', 1},},
        {fields = {'id', 'lastname', 'age'}}
    })

    t.assert_equals(result, nil)
    t.assert_str_contains(err.err, 'Space format doesn\'t contain field named "lastname"')

    -- delete
    result, err = g.router:call(
            'crud.delete', {
                'customers', 1,
                {fields = {'id', 'lastname', 'name'}}
            }
    )

    t.assert_equals(result, nil)
    t.assert_str_contains(err.err, 'Space format doesn\'t contain field named "lastname"')

    -- replace
    result, err = g.router:call(
            'crud.replace', {
                'customers',
                {1, box.NULL, 'Elizabeth', 23},
                {fields = {'id', 'lastname', 'age'}}
            }
    )

    t.assert_equals(result, nil)
    t.assert_str_contains(err.err, 'Space format doesn\'t contain field named "lastname"')

    -- replace_object
    local result, err = g.router:call(
            'crud.replace_object', {
                'customers',
                {id = 1, name = 'Elizabeth', age = 24},
                {fields = {'id', 'lastname', 'age'}}
            }
    )

    t.assert_equals(result, nil)
    t.assert_str_contains(err.err, 'Space format doesn\'t contain field named "lastname"')

    -- upsert
    result, err = g.router:call('crud.upsert', {
        'customers',
        {1, box.NULL, 'Elizabeth', 24},
        {{'+', 'age', 1},},
        {fields = {'id', 'lastname', 'age'}}
    })

    t.assert_equals(result, nil)
    t.assert_str_contains(err.err, 'Space format doesn\'t contain field named "lastname"')

    -- upsert_object
    result, err = g.router:call('crud.upsert_object', {
        'customers',
        {id = 1, name = 'Elizabeth', age = 24},
        {{'+', 'age', 1},},
        {fields = {'id', 'lastname', 'age'}}
    })

    t.assert_equals(result, nil)
    t.assert_str_contains(err.err, 'Space format doesn\'t contain field named "lastname"')
end

pgroup.test_tuple_not_damaged = function(g)
    -- insert
    local insert_tuple = {22, box.NULL, 'Elizabeth', 24}
    local new_insert_tuple, err = g.router:eval([[
        local crud = require('crud')

        local insert_tuple = ...

        local _, err = crud.insert('customers', insert_tuple)

        return insert_tuple, err
    ]], {insert_tuple})

    t.assert_equals(err, nil)
    t.assert_equals(new_insert_tuple, insert_tuple)

    -- upsert
    local upsert_tuple = {33, box.NULL, 'Peter', 35}
    local new_upsert_tuple, err = g.router:eval([[
        local crud = require('crud')

        local upsert_tuple = ...

        local _, err = crud.upsert('customers', upsert_tuple, {{'+', 'age', 1}})

        return upsert_tuple, err
    ]], {upsert_tuple})

    t.assert_equals(err, nil)
    t.assert_equals(new_upsert_tuple, upsert_tuple)

    -- replace
    local replace_tuple = {22, box.NULL, 'Elizabeth', 24}
    local new_replace_tuple, err = g.router:eval([[
        local crud = require('crud')

        local replace_tuple = ...

        local _, err = crud.replace('customers', replace_tuple)

        return replace_tuple, err
    ]], {replace_tuple})

    t.assert_equals(err, nil)
    t.assert_equals(new_replace_tuple, replace_tuple)
end

pgroup.test_opts_not_damaged = function(g)
    -- insert
    local insert_opts = {timeout = 1, bucket_id = 655, fields = {'name', 'age'}}
    local new_insert_opts, err = g.router:eval([[
        local crud = require('crud')

        local insert_opts = ...

        local _, err = crud.insert('customers', {22, box.NULL, 'Elizabeth', 24}, insert_opts)

        return insert_opts, err
    ]], {insert_opts})

    t.assert_equals(err, nil)
    t.assert_equals(new_insert_opts, insert_opts)

    -- insert_object
    local insert_opts = {timeout = 1, bucket_id = 477, fields = {'name', 'age'}}
    local new_insert_opts, err = g.router:eval([[
        local crud = require('crud')

        local insert_opts = ...

        local _, err = crud.insert_object('customers', {id = 1, name = 'Fedor', age = 59}, insert_opts)

        return insert_opts, err
    ]], {insert_opts})

    t.assert_equals(err, nil)
    t.assert_equals(new_insert_opts, insert_opts)

    -- upsert
    local upsert_opts = {timeout = 1, bucket_id = 907, fields = {'name', 'age'}}
    local new_upsert_opts, err = g.router:eval([[
        local crud = require('crud')

        local upsert_opts = ...

        local _, err = crud.upsert('customers', {33, box.NULL, 'Peter', 35}, {{'+', 'age', 1}}, upsert_opts)

        return upsert_opts, err
    ]], {upsert_opts})

    t.assert_equals(err, nil)
    t.assert_equals(new_upsert_opts, upsert_opts)

    -- upsert_object
    local upsert_opts = {timeout = 1, bucket_id = 401, fields = {'name', 'age'}}
    local new_upsert_opts, err = g.router:eval([[
        local crud = require('crud')

        local upsert_opts = ...

        local _, err = crud.upsert_object('customers',
            {id = 2, name = 'Alex', age = 30}, {{'+', 'age', 1}},
            upsert_opts)

        return upsert_opts, err
    ]], {upsert_opts})

    t.assert_equals(err, nil)
    t.assert_equals(new_upsert_opts, upsert_opts)

    -- get
    local get_opts = {timeout = 1, bucket_id = 401, fields = {'name', 'age'}}
    local new_get_opts, err = g.router:eval([[
        local crud = require('crud')

        local get_opts = ...

        local _, err = crud.get('customers', 2, get_opts)

        return get_opts, err
    ]], {get_opts})

    t.assert_equals(err, nil)
    t.assert_equals(new_get_opts, get_opts)

    -- update
    local update_opts = {timeout = 1, bucket_id = 401, fields = {'name', 'age'}}
    local new_update_opts, err = g.router:eval([[
        local crud = require('crud')

        local update_opts = ...

        local _, err = crud.update('customers', 2, {{'+', 'age', 10}}, update_opts)

        return update_opts, err
    ]], {update_opts})

    t.assert_equals(err, nil)
    t.assert_equals(new_update_opts, update_opts)

    -- replace
    local replace_opts = {timeout = 1, bucket_id = 655, fields = {'name', 'age'}}
    local new_replace_opts, err = g.router:eval([[
        local crud = require('crud')

        local replace_opts = ...

        local _, err = crud.replace('customers', {22, box.NULL, 'Elizabeth', 25}, replace_opts)

        return replace_opts, err
    ]], {replace_opts})

    t.assert_equals(err, nil)
    t.assert_equals(new_replace_opts, replace_opts)

    -- replace_object
    local replace_opts = {timeout = 1, bucket_id = 477, fields = {'name', 'age'}}
    local new_replace_opts, err = g.router:eval([[
        local crud = require('crud')

        local replace_opts = ...

        local _, err = crud.replace_object('customers', {id = 1, name = 'Fedor', age = 60}, replace_opts)

        return replace_opts, err
    ]], {replace_opts})

    t.assert_equals(err, nil)
    t.assert_equals(new_replace_opts, replace_opts)

    -- delete
    local delete_opts = {timeout = 1, bucket_id = 401, fields = {'name', 'age'}}
    local new_delete_opts, err = g.router:eval([[
        local crud = require('crud')

        local delete_opts = ...

        local _, err = crud.delete('customers', 2, delete_opts)

        return delete_opts, err
    ]], {delete_opts})

    t.assert_equals(err, nil)
    t.assert_equals(new_delete_opts, delete_opts)
end

local gh_328_success_cases = {
    insert = {
        args = {
            'notebook',
            {nil, nil, 'Inserting...'},
        },
        many = false,
        record_id = 3,
    },
    insert_object = {
        args = {
            'notebook',
            {record = 'Inserting...'},
            {skip_nullability_check_on_flatten = true},
        },
        many = false,
        record_id = 'record',
    },
    replace = {
        args = {
            'notebook',
            {nil, nil, 'Replacing...'},
        },
        many = false,
        record_id = 3,
    },
    replace_object = {
        args = {
            'notebook',
            {record = 'Replacing...'},
            {skip_nullability_check_on_flatten = true},
        },
        many = false,
        record_id = 'record',
    },
    insert_many = {
        args = {
            'notebook',
            {{nil, nil, 'Inserting...'}},
        },
        many = true,
        record_id = 3,
    },
    insert_object_many = {
        args = {
            'notebook',
            {{record = 'Inserting...'}},
            {skip_nullability_check_on_flatten = true},
        },
        many = true,
        record_id = 'record',
    },
    replace_many = {
        args = {
            'notebook',
            {{nil, nil, 'Replacing...'}},
        },
        many = true,
        record_id = 3,
    },
    replace_object_many = {
        args = {
            'notebook',
            {{record = 'Replacing...'}},
            {skip_nullability_check_on_flatten = true},
        },
        many = true,
        record_id = 'record',
    },
}

for op, case in pairs(gh_328_success_cases) do
    local test_name = ('test_gh_328_%s_with_sequence'):format(op)

    pgroup[test_name] = function(g)
        local result, err = g.router:call('crud.' .. op, case.args)
        t.assert_equals(err, nil)
        t.assert_equals(#result.rows, 1)

        if case.many then
            t.assert_equals(result.rows, {{1, 1697, case.args[2][1][case.record_id]}})
        else
            t.assert_equals(result.rows, {{1, 1697, case.args[2][case.record_id]}})
        end
    end
end

local gh_328_error_cases = {
    insert_object = {
        args = {
            'notebook',
            {record = 'Inserting...'},
        },
        many = false,
    },
    replace_object = {
        args = {
            'notebook',
            {record = 'Replacing...'},
        },
        many = false,
    },
    insert_object_many = {
        args = {
            'notebook',
            {{record = 'Inserting...'}},
        },
        many = true,
    },
    replace_object_many = {
        args = {
            'notebook',
            {{record = 'Replacing...'}},
        },
        many = true,
    },
}

for op, case in pairs(gh_328_error_cases) do
    local test_name = ('test_gh_328_%s_with_sequence_returns_error_without_option'):format(op)

    pgroup[test_name] = function(g)
        local result, err = g.router:call('crud.' .. op, case.args)
        t.assert_equals(result, nil)

        if case.many then
            t.assert_equals(#err, 1)
            t.assert_str_contains(
                err[1].err,
                'Field "local_id" isn\'t nullable ' ..
                '(set skip_nullability_check_on_flatten option to true to skip check)'
            )
        else
            t.assert_str_contains(
                err.err,
                'Field "local_id" isn\'t nullable ' ..
                '(set skip_nullability_check_on_flatten option to true to skip check)'
            )
        end
    end
end

pgroup.test_noreturn_opt = function(g)
    -- insert with noreturn success
    local result, err = g.router:call('crud.insert', {
        'customers', {1, box.NULL, 'Elizabeth', 24}, {noreturn = true}
    })
    t.assert_equals(err, nil)
    t.assert_equals(result, nil)

    -- insert with noreturn fail
    local result, err = g.router:call('crud.insert', {
        'customers', {1, box.NULL, 'Elizabeth', 24}, {noreturn = true}
    })
    t.assert_not_equals(err, nil)
    t.assert_equals(result, nil)

    -- insert_object with noreturn success
    local result, err = g.router:call('crud.insert_object', {
        'customers', {id = 0, name = 'Fedor', age = 59}, {noreturn = true}
    })
    t.assert_equals(err, nil)
    t.assert_equals(result, nil)

    -- insert_object with noreturn fail
    local result, err = g.router:call('crud.insert_object', {
        'customers', {id = 0, name = 'Fedor', age = 59}, {noreturn = true}
    })
    t.assert_not_equals(err, nil)
    t.assert_equals(result, nil)

    -- replace with noreturn success
    local result, err = g.router:call('crud.replace', {
        'customers', {1, box.NULL, 'Elizabeth', 24}, {noreturn = true}
    })
    t.assert_equals(err, nil)
    t.assert_equals(result, nil)

    -- replace with noreturn fail
    local result, err = g.router:call('crud.replace', {
        'customers', {box.NULL, box.NULL, 'Elizabeth', 24}, {noreturn = true}
    })
    t.assert_not_equals(err, nil)
    t.assert_equals(result, nil)

    -- replace_object with noreturn success
    local result, err = g.router:call('crud.replace_object', {
        'customers', {id = 0, name = 'Fedor', age = 59}, {noreturn = true}
    })
    t.assert_equals(err, nil)
    t.assert_equals(result, nil)

    -- replace_object with noreturn fail
    local result, err = g.router:call('crud.replace_object', {
        'customers', {id = box.NULL, name = 'Fedor', age = 59}, {noreturn = true}
    })
    t.assert_not_equals(err, nil)
    t.assert_equals(result, nil)

    -- upsert with noreturn success
    local result, err = g.router:call('crud.upsert', {
        'customers', {1, box.NULL, 'Alice', 22},
        {{'+', 'age', 1}}, {noreturn = true}
    })
    t.assert_equals(err, nil)
    t.assert_equals(result, nil)

    -- upsert with noreturn fail
    local result, err = g.router:call('crud.upsert', {
        'customers', {1, box.NULL, 'Alice', 22},
        {{'+', 'unknown', 1}}, {noreturn = true}
    })
    t.assert_not_equals(err, nil)
    t.assert_equals(result, nil)

    -- upsert_object with noreturn success
    local result, err = g.router:call('crud.upsert_object', {
        'customers', {id = 0, name = 'Fedor', age = 59},
        {{'+', 'age', 1}}, {noreturn = true}
    })
    t.assert_equals(err, nil)
    t.assert_equals(result, nil)

    -- upsert_object with noreturn fail
    local result, err = g.router:call('crud.upsert_object', {
        'customers', {id = 0, name = 'Fedor', age = 59},
        {{'+', 'unknown', 1}}, {noreturn = true}
    })
    t.assert_not_equals(err, nil)
    t.assert_equals(result, nil)

    -- update with noreturn success
    local result, err = g.router:call('crud.update', {
        'customers', 1, {{'+', 'age', 1},}, {noreturn = true}
    })
    t.assert_equals(err, nil)
    t.assert_equals(result, nil)

    -- update with noreturn fail
    local result, err = g.router:call('crud.update', {
        'customers', {box.NULL, box.NULL, 'Elizabeth', 24}, {noreturn = true}
    })
    t.assert_not_equals(err, nil)
    t.assert_equals(result, nil)

    -- delete with noreturn success
    local result, err = g.router:call('crud.delete', {
        'customers', 1, {noreturn = true}
    })
    t.assert_equals(err, nil)
    t.assert_equals(result, nil)

    -- delete with noreturn fail
    local result, err = g.router:call('crud.delete', {
        'customers', {box.NULL, box.NULL, 'Elizabeth', 24}, {noreturn = true}
    })
    t.assert_not_equals(err, nil)
    t.assert_equals(result, nil)
end
