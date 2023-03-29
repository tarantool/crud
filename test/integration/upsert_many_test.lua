local fio = require('fio')

local t = require('luatest')

local helpers = require('test.helper')

local batching_utils = require('crud.common.batching_utils')

local pgroup = t.group('upsert_many', {
    {engine = 'memtx'},
    {engine = 'vinyl'},
})

pgroup.before_all(function(g)
    g.cluster = helpers.Cluster:new({
        datadir = fio.tempdir(),
        server_command = helpers.entrypoint('srv_batch_operations'),
        use_vshard = true,
        replicasets = helpers.get_test_replicasets(),
        env = {
            ['ENGINE'] = g.params.engine,
        },
    })

    g.cluster:start()
end)

pgroup.after_all(function(g) helpers.stop_cluster(g.cluster) end)

pgroup.before_each(function(g)
    helpers.truncate_space_on_cluster(g.cluster, 'customers')
end)

pgroup.test_non_existent_space = function(g)
    -- upsert_many
    local result, errs = g.cluster.main_server.net_box:call('crud.upsert_many', {
        'non_existent_space',
        {
            {{1, box.NULL, 'Alex', 59}, {{'+', 'age', 1}}},
            {{2, box.NULL, 'Anna', 23}, {{'+', 'age', 1}}},
            {{3, box.NULL, 'Daria', 18}, {{'+', 'age', 1}}}
        },
    })

    t.assert_equals(result, nil)
    t.assert_not_equals(errs, nil)
    t.assert_equals(#errs, 1)
    t.assert_str_contains(errs[1].err, 'Space "non_existent_space" doesn\'t exist')

    -- upsert_object_many
    local result, errs = g.cluster.main_server.net_box:call('crud.upsert_object_many', {
        'non_existent_space',
        {
            {{id = 1, name = 'Fedor', age = 59}, {{'+', 'age', 1}}},
            {{id = 2, name = 'Anna', age = 23}, {{'+', 'age', 1}}},
            {{id = 3, name = 'Daria', age = 18}, {{'+', 'age', 1}}},
        },
    })

    t.assert_equals(result, nil)
    t.assert_not_equals(errs, nil)

    -- we got 3 errors about non existent space, because it caused by flattening objects
    t.assert_equals(#errs, 3)
    t.assert_str_contains(errs[1].err, 'Space "non_existent_space" doesn\'t exist')
    t.assert_str_contains(errs[2].err, 'Space "non_existent_space" doesn\'t exist')
    t.assert_str_contains(errs[3].err, 'Space "non_existent_space" doesn\'t exist')

    -- upsert_object_many
    local result, errs = g.cluster.main_server.net_box:call('crud.upsert_object_many', {
        'non_existent_space',
        {
            {{id = 1, name = 'Fedor', age = 59}, {{'+', 'age', 1}}},
            {{id = 2, name = 'Anna', age = 23}, {{'+', 'age', 1}}},
            {{id = 3, name = 'Daria', age = 18}, {{'+', 'age', 1}}},
        },
        {
            stop_on_error = true,
        }
    })

    t.assert_equals(result, nil)
    t.assert_not_equals(errs, nil)

    -- we got 1 error about non existent space, because stop_on_error == true
    t.assert_equals(#errs, 1)
    t.assert_str_contains(errs[1].err, 'Space "non_existent_space" doesn\'t exist')
end

pgroup.test_object_bad_format = function(g)
    -- bad format
    local result, errs = g.cluster.main_server.net_box:call('crud.upsert_object_many', {
        'customers',
        {
            {{id = 1, name = 'Fedor', age = 59}, {{'+', 'age', 12}}},
            {{id = 2, name = 'Anna'}, {{'+', 'age', 12}}},
        },
    })

    t.assert_not_equals(errs, nil)
    t.assert_equals(#errs, 1)
    t.assert_str_contains(errs[1].err, 'Field \"age\" isn\'t nullable')
    t.assert_equals(errs[1].operation_data, {id = 2, name = 'Anna'})

    t.assert_equals(result.metadata, {
        {name = 'id', type = 'unsigned'},
        {name = 'bucket_id', type = 'unsigned'},
        {name = 'name', type = 'string'},
        {name = 'age', type = 'number'},
    })
    t.assert_equals(result.rows, nil)

    -- get
    -- primary key = 1 -> bucket_id = 477 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:get(1)
    t.assert_equals(result, {1, 477, 'Fedor', 59})

    -- bad format
    local result, errs = g.cluster.main_server.net_box:call('crud.upsert_object_many', {
        'customers',
        {
            {{id = 1, name = 'Fedor', age = 59}, {{'+', 'age', '1'}}},
            {{id = 2, name = 'Anna'}, {{'+', 'age', 12}}}
        },
    })

    t.assert_equals(result, nil)
    t.assert_not_equals(errs, nil)
    t.assert_equals(#errs, 2)

    if helpers.tarantool_version_at_least(2, 3) then
        t.assert_str_contains(errs[1].err,
                'Argument type in operation \'+\' on field \'age\' does not match field type')
    else
        t.assert_str_contains(errs[1].err,
                'Argument type in operation \'+\' on field 4 does not match field type')
    end
    t.assert_equals(errs[1].operation_data, {1, 477, "Fedor", 59})

    t.assert_str_contains(errs[2].err, 'Field \"age\" isn\'t nullable')
    t.assert_equals(errs[2].operation_data, {id = 2, name = 'Anna'})

    -- bad format
    -- two errors, default: stop_on_error == false
    local result, errs = g.cluster.main_server.net_box:call('crud.upsert_object_many', {
        'customers',
        {
            {{id = 2, name = 'Anna'}, {{'+', 'age', 25}, {'=', 'name', 'Leo Tolstoy'},}},
            {{id = 3, name = 'Inga'}, {{'+', 'age', 12}}}
        },
    })

    t.assert_equals(result, nil)
    t.assert_not_equals(errs, nil)
    t.assert_equals(#errs, 2)

    t.assert_str_contains(errs[1].err, 'Field \"age\" isn\'t nullable')
    t.assert_equals(errs[1].operation_data, {id = 2, name = 'Anna'})

    t.assert_str_contains(errs[2].err, 'Field \"age\" isn\'t nullable')
    t.assert_equals(errs[2].operation_data, {id = 3, name = 'Inga'})
end

pgroup.test_all_success = function(g)
    -- upsert_many
    local result, errs = g.cluster.main_server.net_box:call('crud.upsert_many', {
        'customers',
        {
            {{1, box.NULL, 'Fedor', 59}, {{'+', 'age', 25}, {'=', 'name', 'Leo Tolstoy'},}},
            {{2, box.NULL, 'Anna', 23}, {{'+', 'age', 12}}},
            {{3, box.NULL, 'Daria', 18}, {{'=', 'name', 'Jane'}}}
        },
    })

    t.assert_equals(errs, nil)
    t.assert_equals(result.metadata, {
        {name = 'id', type = 'unsigned'},
        {name = 'bucket_id', type = 'unsigned'},
        {name = 'name', type = 'string'},
        {name = 'age', type = 'number'},
    })
    t.assert_equals(result.rows, nil)

    -- get
    -- primary key = 1 -> bucket_id = 477 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:get(1)
    t.assert_equals(result, {1, 477, 'Fedor', 59})

    -- primary key = 2 -> bucket_id = 401 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:get(2)
    t.assert_equals(result, {2, 401, 'Anna', 23})

    -- primary key = 3 -> bucket_id = 2804 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(3)
    t.assert_equals(result, {3, 2804, 'Daria', 18})
end

pgroup.test_object_all_success = function(g)
    -- upsert_object_many
    local result, errs = g.cluster.main_server.net_box:call('crud.upsert_object_many', {
        'customers',
        {
            {{id = 1, name = 'Fedor', age = 59}, {{'+', 'age', 25}, {'=', 'name', 'Leo Tolstoy'},}},
            {{id = 2, name = 'Anna', age = 23}, {{'+', 'age', 12}}},
            {{id = 3, name = 'Daria', age = 18}, {{'=', 'name', 'Jane'}}}
        },
    })

    t.assert_equals(errs, nil)
    t.assert_equals(result.metadata, {
        {name = 'id', type = 'unsigned'},
        {name = 'bucket_id', type = 'unsigned'},
        {name = 'name', type = 'string'},
        {name = 'age', type = 'number'},
    })
    t.assert_equals(result.rows, nil)

    -- get
    -- primary key = 1 -> bucket_id = 477 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:get(1)
    t.assert_equals(result, {1, 477, 'Fedor', 59})

    -- primary key = 2 -> bucket_id = 401 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:get(2)
    t.assert_equals(result, {2, 401, 'Anna', 23})

    -- primary key = 3 -> bucket_id = 2804 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(3)
    t.assert_equals(result, {3, 2804, 'Daria', 18})
end

pgroup.test_one_error = function(g)
    -- primary key = 3 -> bucket_id = 2804 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:insert({3, 2804, 'Daria', 18})
    t.assert_equals(result, {3, 2804, 'Daria', 18})

    -- primary key = 5 -> bucket_id = 1172 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:insert({5, 1172, 'Sergey', 20})
    t.assert_equals(result, {5, 1172, 'Sergey', 20})

    -- upsert_many
    -- failed for s1-master
    local result, errs = g.cluster.main_server.net_box:call('crud.upsert_many', {
        'customers',
        {
            {{22, box.NULL, 'Alex', 34}, {{'=', 'name', 'Peter'},}},
            {{3, box.NULL, 'Anastasia', 22}, {{'=', 'age', 'invalid type'}, {'=', 'name', 'Leo Tolstoy'},}},
            {{5, box.NULL, 'Peter', 27}, {{'+', 'age', 5}}}
        },
    })

    t.assert_not_equals(errs, nil)
    t.assert_equals(#errs, 1)
    if helpers.tarantool_version_at_least(2, 8) then
        t.assert_str_contains(errs[1].err, 'Tuple field 4 (age) type does not match one required by operation')
    else
        t.assert_str_contains(errs[1].err, 'Tuple field 4 type does not match one required by operation')
    end
    t.assert_equals(errs[1].operation_data, {3, 2804, 'Anastasia', 22})
    t.assert_equals(result.metadata, {
        {name = 'id', type = 'unsigned'},
        {name = 'bucket_id', type = 'unsigned'},
        {name = 'name', type = 'string'},
        {name = 'age', type = 'number'},
    })
    t.assert_equals(result.rows, nil)

    -- get
    -- primary key = 22 -> bucket_id = 655 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:get(22)
    t.assert_equals(result, {22, 655, 'Alex', 34})

    -- primary key = 5 -> bucket_id = 1172 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:get(5)
    t.assert_equals(result, {5, 1172, 'Sergey', 25})

    -- primary key = 3 -> bucket_id = 2804 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(3)
    t.assert_equals(result, {3, 2804, 'Daria', 18})
end

pgroup.test_object_one_error = function(g)
    -- primary key = 3 -> bucket_id = 2804 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:insert({3, 2804, 'Daria', 18})
    t.assert_equals(result, {3, 2804, 'Daria', 18})

    -- primary key = 5 -> bucket_id = 1172 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:insert({5, 1172, 'Sergey', 20})
    t.assert_equals(result, {5, 1172, 'Sergey', 20})

    -- upsert_object_many
    -- failed for s1-master
    local result, errs = g.cluster.main_server.net_box:call('crud.upsert_object_many', {
        'customers',
        {
            {{id = 22, name = 'Alex', age = 34}, {{'=', 'name', 'Peter'},}},
            {{id = 3, name = 'Anastasia', age = 22}, {{'=', 'age', 'invalid type'}, {'=', 'name', 'Leo Tolstoy'},}},
            {{id = 5, name = 'Peter', age = 27}, {{'+', 'age', 5}}}
        },
    })

    t.assert_not_equals(errs, nil)
    t.assert_equals(#errs, 1)
    if helpers.tarantool_version_at_least(2, 8) then
        t.assert_str_contains(errs[1].err, 'Tuple field 4 (age) type does not match one required by operation')
    else
        t.assert_str_contains(errs[1].err, 'Tuple field 4 type does not match one required by operation')
    end
    t.assert_equals(errs[1].operation_data, {3, 2804, 'Anastasia', 22})
    t.assert_equals(result.metadata, {
        {name = 'id', type = 'unsigned'},
        {name = 'bucket_id', type = 'unsigned'},
        {name = 'name', type = 'string'},
        {name = 'age', type = 'number'},
    })
    t.assert_equals(result.rows, nil)

    -- get
    -- primary key = 22 -> bucket_id = 655 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:get(22)
    t.assert_equals(result, {22, 655, 'Alex', 34})

    -- primary key = 5 -> bucket_id = 1172 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:get(5)
    t.assert_equals(result, {5, 1172, 'Sergey', 25})

    -- primary key = 3 -> bucket_id = 2804 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(3)
    t.assert_equals(result, {3, 2804, 'Daria', 18})
end

pgroup.test_many_errors = function(g)
    -- primary key = 3 -> bucket_id = 2804 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:insert({3, 2804, 'Daria', 18})
    t.assert_equals(result, {3, 2804, 'Daria', 18})

    -- primary key = 5 -> bucket_id = 1172 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:insert({5, 1172, 'Sergey', 20})
    t.assert_equals(result, {5, 1172, 'Sergey', 20})

    -- upsert_many
    -- failed for both: s1-master s2-master
    local result, errs = g.cluster.main_server.net_box:call('crud.upsert_many', {
        'customers',
        {
            {{22, box.NULL, 'Alex', 34}, {{'=', 'name', 'Peter'},}},
            {{3, box.NULL, 'Anastasia', 22}, {{'=', 'age', 'invalid type'}, {'=', 'name', 'Leo Tolstoy'},}},
            {{5, box.NULL, 'Peter', 27}, {{'+', 'age', '5'}}}
        },
    })

    t.assert_not_equals(errs, nil)
    t.assert_equals(#errs, 2)

    table.sort(errs, function(err1, err2) return err1.operation_data[1] < err2.operation_data[1] end)

    if helpers.tarantool_version_at_least(2, 8) then
        t.assert_str_contains(errs[1].err, 'Tuple field 4 (age) type does not match one required by operation')
    else
        t.assert_str_contains(errs[1].err, 'Tuple field 4 type does not match one required by operation')
    end
    t.assert_equals(errs[1].operation_data, {3, 2804, 'Anastasia', 22})

    if helpers.tarantool_version_at_least(2, 3) then
        t.assert_str_contains(errs[2].err,
                'Argument type in operation \'+\' on field \'age\' does not match field type')
    else
        t.assert_str_contains(errs[2].err,
                'Argument type in operation \'+\' on field 4 does not match field type')
    end
    t.assert_equals(errs[2].operation_data, {5, 1172, 'Peter', 27})

    t.assert_equals(result.metadata, {
        {name = 'id', type = 'unsigned'},
        {name = 'bucket_id', type = 'unsigned'},
        {name = 'name', type = 'string'},
        {name = 'age', type = 'number'},
    })
    t.assert_equals(result.rows, nil)

    -- get
    -- primary key = 22 -> bucket_id = 655 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:get(22)
    t.assert_equals(result, {22, 655, 'Alex', 34})

    -- primary key = 5 -> bucket_id = 1172 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:get(5)
    t.assert_equals(result, {5, 1172, 'Sergey', 20})

    -- primary key = 3 -> bucket_id = 2804 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(3)
    t.assert_equals(result, {3, 2804, 'Daria', 18})
end

pgroup.test_object_many_errors = function(g)
    -- primary key = 3 -> bucket_id = 2804 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:insert({3, 2804, 'Daria', 18})
    t.assert_equals(result, {3, 2804, 'Daria', 18})

    -- primary key = 5 -> bucket_id = 1172 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:insert({5, 1172, 'Sergey', 20})
    t.assert_equals(result, {5, 1172, 'Sergey', 20})

    -- upsert_object_many
    -- failed for both: s1-master s2-master
    local result, errs = g.cluster.main_server.net_box:call('crud.upsert_object_many', {
        'customers',
        {
            {{id = 22, name = 'Alex', age = 34}, {{'=', 'name', 'Peter'},}},
            {{id = 3, name = 'Anastasia', age = 22}, {{'=', 'age', 'invalid type'}, {'=', 'name', 'Leo Tolstoy'},}},
            {{id = 5, name = 'Peter', age = 27}, {{'+', 'age', '5'}}}
        },
    })

    t.assert_not_equals(errs, nil)
    t.assert_equals(#errs, 2)

    table.sort(errs, function(err1, err2) return err1.operation_data[1] < err2.operation_data[1] end)

    if helpers.tarantool_version_at_least(2, 8) then
        t.assert_str_contains(errs[1].err, 'Tuple field 4 (age) type does not match one required by operation')
    else
        t.assert_str_contains(errs[1].err, 'Tuple field 4 type does not match one required by operation')
    end
    t.assert_equals(errs[1].operation_data, {3, 2804, 'Anastasia', 22})

    if helpers.tarantool_version_at_least(2, 3) then
        t.assert_str_contains(errs[2].err,
                'Argument type in operation \'+\' on field \'age\' does not match field type')
    else
        t.assert_str_contains(errs[2].err,
                'Argument type in operation \'+\' on field 4 does not match field type')
    end
    t.assert_equals(errs[2].operation_data, {5, 1172, 'Peter', 27})

    t.assert_equals(result.metadata, {
        {name = 'id', type = 'unsigned'},
        {name = 'bucket_id', type = 'unsigned'},
        {name = 'name', type = 'string'},
        {name = 'age', type = 'number'},
    })
    t.assert_equals(result.rows, nil)

    -- get
    -- primary key = 22 -> bucket_id = 655 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:get(22)
    t.assert_equals(result, {22, 655, 'Alex', 34})

    -- primary key = 5 -> bucket_id = 1172 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:get(5)
    t.assert_equals(result, {5, 1172, 'Sergey', 20})

    -- primary key = 3 -> bucket_id = 2804 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(3)
    t.assert_equals(result, {3, 2804, 'Daria', 18})
end

pgroup.test_no_success = function(g)
    -- primary key = 22 -> bucket_id = 655 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:insert({22, 655, 'Roman', 30})
    t.assert_equals(result, {22, 655, 'Roman', 30})

    -- primary key = 3 -> bucket_id = 2804 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:insert({3, 2804, 'Daria', 18})
    t.assert_equals(result, {3, 2804, 'Daria', 18})

    -- primary key = 5 -> bucket_id = 1172 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:insert({5, 1172, 'Sergey', 20})
    t.assert_equals(result, {5, 1172, 'Sergey', 20})

    -- upsert_many
    -- failed for both: s1-master s2-master
    local result, errs = g.cluster.main_server.net_box:call('crud.upsert_many', {
        'customers',
        {
            {{22, box.NULL, 'Alex', 34}, {{'=', 'name', 5},}},
            {{3, box.NULL, 'Anastasia', 22}, {{'=', 'age', 'invalid type'}, {'=', 'name', 'Leo Tolstoy'},}},
            {{5, box.NULL, 'Peter', 27}, {{'+', 'age', '5'}}}
        },
    })

    t.assert_equals(result, nil)
    t.assert_not_equals(errs, nil)
    t.assert_equals(#errs, 3)

    table.sort(errs, function(err1, err2) return err1.operation_data[1] < err2.operation_data[1] end)

    if helpers.tarantool_version_at_least(2, 8) then
        t.assert_str_contains(errs[1].err, 'Tuple field 4 (age) type does not match one required by operation')
    else
        t.assert_str_contains(errs[1].err, 'Tuple field 4 type does not match one required by operation')
    end
    t.assert_equals(errs[1].operation_data, {3, 2804, 'Anastasia', 22})

    if helpers.tarantool_version_at_least(2, 3) then
        t.assert_str_contains(errs[2].err,
                'Argument type in operation \'+\' on field \'age\' does not match field type')
    else
        t.assert_str_contains(errs[2].err,
                'Argument type in operation \'+\' on field 4 does not match field type')
    end
    t.assert_equals(errs[2].operation_data, {5, 1172, 'Peter', 27})

    if helpers.tarantool_version_at_least(2, 8) then
        t.assert_str_contains(errs[3].err, 'Tuple field 3 (name) type does not match one required by operation')
    else
        t.assert_str_contains(errs[3].err, 'Tuple field 3 type does not match one required by operation')
    end
    t.assert_equals(errs[3].operation_data, {22, 655, 'Alex', 34})

    -- get
    -- primary key = 22 -> bucket_id = 655 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:get(22)
    t.assert_equals(result, {22, 655, 'Roman', 30})

    -- primary key = 3 -> bucket_id = 2804 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(3)
    t.assert_equals(result, {3, 2804, 'Daria', 18})

    -- primary key = 5 -> bucket_id = 1172 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:get(5)
    t.assert_equals(result, {5, 1172, 'Sergey', 20})
end

pgroup.test_object_no_success = function(g)
    -- primary key = 22 -> bucket_id = 655 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:insert({22, 655, 'Roman', 30})
    t.assert_equals(result, {22, 655, 'Roman', 30})

    -- primary key = 3 -> bucket_id = 2804 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:insert({3, 2804, 'Daria', 18})
    t.assert_equals(result, {3, 2804, 'Daria', 18})

    -- primary key = 5 -> bucket_id = 1172 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:insert({5, 1172, 'Sergey', 20})
    t.assert_equals(result, {5, 1172, 'Sergey', 20})

    -- upsert_object_many
    -- failed for both: s1-master s2-master
    local result, errs = g.cluster.main_server.net_box:call('crud.upsert_object_many', {
        'customers',
        {
            {{id = 22, name = 'Alex', age = 34}, {{'=', 'name', 5},}},
            {{id = 3, name = 'Anastasia', age = 22}, {{'=', 'age', 'invalid type'}, {'=', 'name', 'Leo Tolstoy'},}},
            {{id = 5, name = 'Peter', age = 27}, {{'+', 'age', '5'}}}
        },
    })

    t.assert_equals(result, nil)
    t.assert_not_equals(errs, nil)
    t.assert_equals(#errs, 3)

    table.sort(errs, function(err1, err2) return err1.operation_data[1] < err2.operation_data[1] end)

    if helpers.tarantool_version_at_least(2, 8) then
        t.assert_str_contains(errs[1].err, 'Tuple field 4 (age) type does not match one required by operation')
    else
        t.assert_str_contains(errs[1].err, 'Tuple field 4 type does not match one required by operation')
    end
    t.assert_equals(errs[1].operation_data, {3, 2804, 'Anastasia', 22})

    if helpers.tarantool_version_at_least(2, 3) then
        t.assert_str_contains(errs[2].err,
                'Argument type in operation \'+\' on field \'age\' does not match field type')
    else
        t.assert_str_contains(errs[2].err,
                'Argument type in operation \'+\' on field 4 does not match field type')
    end
    t.assert_equals(errs[2].operation_data, {5, 1172, 'Peter', 27})

    if helpers.tarantool_version_at_least(2, 8) then
        t.assert_str_contains(errs[3].err, 'Tuple field 3 (name) type does not match one required by operation')
    else
        t.assert_str_contains(errs[3].err, 'Tuple field 3 type does not match one required by operation')
    end
    t.assert_equals(errs[3].operation_data, {22, 655, 'Alex', 34})

    -- get
    -- primary key = 22 -> bucket_id = 655 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:get(22)
    t.assert_equals(result, {22, 655, 'Roman', 30})

    -- primary key = 3 -> bucket_id = 2804 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(3)
    t.assert_equals(result, {3, 2804, 'Daria', 18})

    -- primary key = 5 -> bucket_id = 1172 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:get(5)
    t.assert_equals(result, {5, 1172, 'Sergey', 20})
end

pgroup.test_object_bad_format_stop_on_error = function(g)
    -- bad format
    -- two errors, stop_on_error == true
    local result, errs = g.cluster.main_server.net_box:call('crud.upsert_object_many', {
        'customers',
        {
            {{id = 1, name = 'Fedor'}, {{'+', 'age', 12}}},
            {{id = 2, name = 'Anna'}, {{'+', 'age', 12}}}
        },
        {
            stop_on_error = true,
        }
    })

    t.assert_equals(result, nil)
    t.assert_not_equals(errs, nil)
    t.assert_equals(#errs, 1)
    t.assert_str_contains(errs[1].err, 'Field \"age\" isn\'t nullable')
    t.assert_equals(errs[1].operation_data, {id = 1, name = 'Fedor'})
end

pgroup.test_all_success_stop_on_error = function(g)
    -- upsert_many
    local result, errs = g.cluster.main_server.net_box:call('crud.upsert_many', {
        'customers',
        {
            {{1, box.NULL, 'Fedor', 59}, {{'+', 'age', 25}, {'=', 'name', 'Leo Tolstoy'},}},
            {{2, box.NULL, 'Anna', 23}, {{'+', 'age', 12}}},
            {{3, box.NULL, 'Daria', 18}, {{'=', 'name', 'Jane'}}}
        },
        {
            stop_on_error = true,
        }
    })

    t.assert_equals(errs, nil)
    t.assert_equals(result.metadata, {
        {name = 'id', type = 'unsigned'},
        {name = 'bucket_id', type = 'unsigned'},
        {name = 'name', type = 'string'},
        {name = 'age', type = 'number'},
    })
    t.assert_equals(result.rows, nil)

    -- get
    -- primary key = 1 -> bucket_id = 477 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:get(1)
    t.assert_equals(result, {1, 477, 'Fedor', 59})

    -- primary key = 2 -> bucket_id = 401 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:get(2)
    t.assert_equals(result, {2, 401, 'Anna', 23})

    -- primary key = 3 -> bucket_id = 2804 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(3)
    t.assert_equals(result, {3, 2804, 'Daria', 18})
end

pgroup.test_object_all_success_stop_on_error = function(g)
    -- upsert_object_many
    local result, errs = g.cluster.main_server.net_box:call('crud.upsert_object_many', {
        'customers',
        {
            {{id = 1, name = 'Fedor', age = 59}, {{'+', 'age', 25}, {'=', 'name', 'Leo Tolstoy'},}},
            {{id = 2, name = 'Anna', age = 23}, {{'+', 'age', 12}}},
            {{id = 3, name = 'Daria', age = 18}, {{'=', 'name', 'Jane'}}}
        },
        {
            stop_on_error = true,
        }
    })

    t.assert_equals(errs, nil)
    t.assert_equals(result.metadata, {
        {name = 'id', type = 'unsigned'},
        {name = 'bucket_id', type = 'unsigned'},
        {name = 'name', type = 'string'},
        {name = 'age', type = 'number'},
    })
    t.assert_equals(result.rows, nil)

    -- get
    -- primary key = 1 -> bucket_id = 477 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:get(1)
    t.assert_equals(result, {1, 477, 'Fedor', 59})

    -- primary key = 2 -> bucket_id = 401 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:get(2)
    t.assert_equals(result, {2, 401, 'Anna', 23})

    -- primary key = 3 -> bucket_id = 2804 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(3)
    t.assert_equals(result, {3, 2804, 'Daria', 18})
end

pgroup.test_object_partial_success_stop_on_error = function(g)
    -- insert
    -- primary key = 3 -> bucket_id = 2804 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:insert({3, 2804, 'Daria', 18})
    t.assert_equals(result, {3, 2804, 'Daria', 18})

    -- primary key = 9 -> bucket_id = 1644 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:insert({9, 1644, 'Nicolo', 35})
    t.assert_equals(result, {9, 1644, 'Nicolo', 35})

    -- upsert_object_many
    -- stop_on_error = true, rollback_on_error = false
    -- one error on one storage without rollback, inserts stop by error on this storage
    -- inserts before error are successful
    local result, errs = g.cluster.main_server.net_box:call('crud.upsert_object_many', {
        'customers',
        {
            {{id = 22, name = 'Alex', age = 34}, {{'+', 'age', 1}}},
            {{id = 92, name = 'Artur', age = 29}, {{'+', 'age', 2}}},
            {{id = 3, name = 'Anastasia', age = 22}, {{'+', 'age', '3'}}},
            {{id = 5, name = 'Sergey', age = 25}, {{'+', 'age', 4}}},
            {{id = 9, name = 'Anna', age = 30}, {{'+', 'age', '5'}}}
        },
        {
            stop_on_error = true,
        }
    })

    t.assert_equals(#errs, 2)

    table.sort(errs, function(err1, err2) return err1.operation_data[1] < err2.operation_data[1] end)

    if helpers.tarantool_version_at_least(2, 3) then
        t.assert_str_contains(errs[1].err,
                'Argument type in operation \'+\' on field \'age\' does not match field type')
    else
        t.assert_str_contains(errs[1].err,
                'Argument type in operation \'+\' on field 4 does not match field type')
    end
    t.assert_equals(errs[1].operation_data, {3, 2804, 'Anastasia', 22})

    t.assert_str_contains(errs[2].err, batching_utils.stop_on_error_msg)
    t.assert_equals(errs[2].operation_data, {9, 1644, "Anna", 30})

    t.assert_equals(result.metadata, {
        {name = 'id', type = 'unsigned'},
        {name = 'bucket_id', type = 'unsigned'},
        {name = 'name', type = 'string'},
        {name = 'age', type = 'number'},
    })
    t.assert_equals(result.rows, nil)

    -- get
    -- primary key = 22 -> bucket_id = 655 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:get(22)
    t.assert_equals(result, {22, 655, 'Alex', 34})

    -- primary key = 92 -> bucket_id = 2040 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(92)
    t.assert_equals(result, {92, 2040, 'Artur', 29})

    -- primary key = 5 -> bucket_id = 1172 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:get(5)
    t.assert_equals(result, {5, 1172, 'Sergey', 25})

    -- primary key = 3 -> bucket_id = 2804 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(3)
    t.assert_equals(result, {3, 2804, 'Daria', 18})

    -- primary key = 9 -> bucket_id = 1644 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(9)
    t.assert_equals(result, {9, 1644, 'Nicolo', 35})
end

pgroup.test_partial_success_stop_on_error = function(g)
    -- insert
    -- primary key = 3 -> bucket_id = 2804 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:insert({3, 2804, 'Daria', 18})
    t.assert_equals(result, {3, 2804, 'Daria', 18})

    -- primary key = 9 -> bucket_id = 1644 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:insert({9, 1644, 'Nicolo', 35})
    t.assert_equals(result, {9, 1644, 'Nicolo', 35})

    -- upsert_many
    -- stop_on_error = true, rollback_on_error = false
    -- one error on one storage without rollback, inserts stop by error on this storage
    -- inserts before error are successful
    local result, errs = g.cluster.main_server.net_box:call('crud.upsert_many', {
        'customers',
        {
            {{22, box.NULL, 'Alex', 34}, {{'+', 'age', 1}}},
            {{92, box.NULL, 'Artur', 29}, {{'+', 'age', 2}}},
            {{3, box.NULL, 'Anastasia', 22}, {{'+', 'age', '3'}}},
            {{5, box.NULL, 'Sergey', 25}, {{'+', 'age', 4}}},
            {{9, box.NULL, 'Anna', 30}, {{'+', 'age', '5'}}}
        },
        {
            stop_on_error = true,
        }
    })

    t.assert_equals(#errs, 2)

    table.sort(errs, function(err1, err2) return err1.operation_data[1] < err2.operation_data[1] end)

    if helpers.tarantool_version_at_least(2, 3) then
        t.assert_str_contains(errs[1].err,
                'Argument type in operation \'+\' on field \'age\' does not match field type')
    else
        t.assert_str_contains(errs[1].err,
                'Argument type in operation \'+\' on field 4 does not match field type')
    end
    t.assert_equals(errs[1].operation_data, {3, 2804, 'Anastasia', 22})

    t.assert_str_contains(errs[2].err, batching_utils.stop_on_error_msg)
    t.assert_equals(errs[2].operation_data, {9, 1644, "Anna", 30})

    t.assert_equals(result.metadata, {
        {name = 'id', type = 'unsigned'},
        {name = 'bucket_id', type = 'unsigned'},
        {name = 'name', type = 'string'},
        {name = 'age', type = 'number'},
    })
    t.assert_equals(result.rows, nil)

    -- get
    -- primary key = 22 -> bucket_id = 655 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:get(22)
    t.assert_equals(result, {22, 655, 'Alex', 34})

    -- primary key = 92 -> bucket_id = 2040 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(92)
    t.assert_equals(result, {92, 2040, 'Artur', 29})

    -- primary key = 5 -> bucket_id = 1172 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:get(5)
    t.assert_equals(result, {5, 1172, 'Sergey', 25})

    -- primary key = 3 -> bucket_id = 2804 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(3)
    t.assert_equals(result, {3, 2804, 'Daria', 18})

    -- primary key = 9 -> bucket_id = 1644 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(9)
    t.assert_equals(result, {9, 1644, 'Nicolo', 35})
end

pgroup.test_no_success_stop_on_error = function(g)
    -- insert
    -- primary key = 2 -> bucket_id = 401 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:insert({2, 401, 'Anna', 23})
    t.assert_equals(result, {2, 401, 'Anna', 23})

    -- primary key = 3 -> bucket_id = 2804 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:insert({3, 2804, 'Daria', 18})
    t.assert_equals(result, {3, 2804, 'Daria', 18})

    -- primary key = 92 -> bucket_id = 2040 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:insert({92, 2040, 'Artur', 29})
    t.assert_equals(result, {92, 2040, 'Artur', 29})

    -- upsert_many
    -- fails for both: s1-master s2-master
    -- one error on each storage, all inserts stop by error
    local result, errs = g.cluster.main_server.net_box:call('crud.upsert_many', {
        'customers',
        {
            {{2, box.NULL, 'Alex', 34}, {{'+', 'age', '1'}}},
            {{3, box.NULL, 'Anastasia', 22}, {{'+', 'age', '2'}}},
            {{10, box.NULL, 'Sergey', 25}, {{'+', 'age', 3}}},
            {{9, box.NULL, 'Anna', 30}, {{'+', 'age', '4'}}},
            {{92, box.NULL, 'Leo', 29}, {{'+', 'age', 5}}}
        },
        {
            stop_on_error = true,
        }
    })

    t.assert_equals(result, nil)
    t.assert_not_equals(errs, nil)
    t.assert_equals(#errs, 5)

    table.sort(errs, function(err1, err2) return err1.operation_data[1] < err2.operation_data[1] end)

    if helpers.tarantool_version_at_least(2, 3) then
        t.assert_str_contains(errs[1].err,
                'Argument type in operation \'+\' on field \'age\' does not match field type')
    else
        t.assert_str_contains(errs[1].err,
                'Argument type in operation \'+\' on field 4 does not match field type')
    end
    t.assert_equals(errs[1].operation_data, {2, 401, 'Alex', 34})

    if helpers.tarantool_version_at_least(2, 3) then
        t.assert_str_contains(errs[2].err,
                'Argument type in operation \'+\' on field \'age\' does not match field type')
    else
        t.assert_str_contains(errs[2].err,
                'Argument type in operation \'+\' on field 4 does not match field type')
    end
    t.assert_equals(errs[2].operation_data, {3, 2804, 'Anastasia', 22})

    t.assert_str_contains(errs[3].err, batching_utils.stop_on_error_msg)
    t.assert_equals(errs[3].operation_data, {9, 1644, "Anna", 30})

    t.assert_str_contains(errs[4].err, batching_utils.stop_on_error_msg)
    t.assert_equals(errs[4].operation_data, {10, 569, "Sergey", 25})

    t.assert_str_contains(errs[5].err, batching_utils.stop_on_error_msg)
    t.assert_equals(errs[5].operation_data, {92, 2040, "Leo", 29})

    -- primary key = 2 -> bucket_id = 401 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:get(2)
    t.assert_equals(result, {2, 401, 'Anna', 23})

    -- primary key = 3 -> bucket_id = 2804 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(3)
    t.assert_equals(result, {3, 2804, 'Daria', 18})

    -- primary key = 10 -> bucket_id = 569 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:get(10)
    t.assert_equals(result, nil)

    -- primary key = 9 -> bucket_id = 1644 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(9)
    t.assert_equals(result, nil)

    -- primary key = 92 -> bucket_id = 2040 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(92)
    t.assert_equals(result, {92, 2040, 'Artur', 29})
end

pgroup.test_object_no_success_stop_on_error = function(g)
    -- insert
    -- primary key = 2 -> bucket_id = 401 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:insert({2, 401, 'Anna', 23})
    t.assert_equals(result, {2, 401, 'Anna', 23})

    -- primary key = 3 -> bucket_id = 2804 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:insert({3, 2804, 'Daria', 18})
    t.assert_equals(result, {3, 2804, 'Daria', 18})

    -- primary key = 92 -> bucket_id = 2040 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:insert({92, 2040, 'Artur', 29})
    t.assert_equals(result, {92, 2040, 'Artur', 29})

    -- upsert_object_many
    -- fails for both: s1-master s2-master
    -- one error on each storage, all inserts stop by error
    local result, errs = g.cluster.main_server.net_box:call('crud.upsert_object_many', {
        'customers',
        {
            {{id = 2, name = 'Alex', age = 34}, {{'+', 'age', '1'}}},
            {{id = 3, name = 'Anastasia', age = 22}, {{'+', 'age', '2'}}},
            {{id = 10, name = 'Sergey', age = 25}, {{'+', 'age', 3}}},
            {{id = 9, name = 'Anna', age = 30}, {{'+', 'age', '4'}}},
            {{id = 92, name = 'Leo', age = 29}, {{'+', 'age', 5}}},
        },
        {
            stop_on_error = true,
        }
    })

    t.assert_equals(result, nil)
    t.assert_not_equals(errs, nil)
    t.assert_equals(#errs, 5)

    table.sort(errs, function(err1, err2) return err1.operation_data[1] < err2.operation_data[1] end)

    if helpers.tarantool_version_at_least(2, 3) then
        t.assert_str_contains(errs[1].err,
                'Argument type in operation \'+\' on field \'age\' does not match field type')
    else
        t.assert_str_contains(errs[1].err,
                'Argument type in operation \'+\' on field 4 does not match field type')
    end
    t.assert_equals(errs[1].operation_data, {2, 401, 'Alex', 34})

    if helpers.tarantool_version_at_least(2, 3) then
        t.assert_str_contains(errs[2].err,
                'Argument type in operation \'+\' on field \'age\' does not match field type')
    else
        t.assert_str_contains(errs[2].err,
                'Argument type in operation \'+\' on field 4 does not match field type')
    end
    t.assert_equals(errs[2].operation_data, {3, 2804, 'Anastasia', 22})

    t.assert_str_contains(errs[3].err, batching_utils.stop_on_error_msg)
    t.assert_equals(errs[3].operation_data, {9, 1644, "Anna", 30})

    t.assert_str_contains(errs[4].err, batching_utils.stop_on_error_msg)
    t.assert_equals(errs[4].operation_data, {10, 569, "Sergey", 25})

    t.assert_str_contains(errs[5].err, batching_utils.stop_on_error_msg)
    t.assert_equals(errs[5].operation_data, {92, 2040, "Leo", 29})

    -- primary key = 2 -> bucket_id = 401 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:get(2)
    t.assert_equals(result, {2, 401, 'Anna', 23})

    -- primary key = 3 -> bucket_id = 2804 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(3)
    t.assert_equals(result, {3, 2804, 'Daria', 18})

    -- primary key = 10 -> bucket_id = 569 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:get(10)
    t.assert_equals(result, nil)

    -- primary key = 9 -> bucket_id = 1644 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(9)
    t.assert_equals(result, nil)

    -- primary key = 92 -> bucket_id = 2040 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(92)
    t.assert_equals(result, {92, 2040, 'Artur', 29})
end

pgroup.test_all_success_rollback_on_error = function(g)
    -- upsert_many
    local result, errs = g.cluster.main_server.net_box:call('crud.upsert_many', {
        'customers',
        {
            {{1, box.NULL, 'Fedor', 59}, {{'+', 'age', 25}, {'=', 'name', 'Leo Tolstoy'},}},
            {{2, box.NULL, 'Anna', 23}, {{'+', 'age', 12}}},
            {{3, box.NULL, 'Daria', 18}, {{'=', 'name', 'Jane'}}}
        },
        {
            rollback_on_error = true,
        }
    })

    t.assert_equals(errs, nil)
    t.assert_equals(result.metadata, {
        {name = 'id', type = 'unsigned'},
        {name = 'bucket_id', type = 'unsigned'},
        {name = 'name', type = 'string'},
        {name = 'age', type = 'number'},
    })
    t.assert_equals(result.rows, nil)

    -- get
    -- primary key = 1 -> bucket_id = 477 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:get(1)
    t.assert_equals(result, {1, 477, 'Fedor', 59})

    -- primary key = 2 -> bucket_id = 401 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:get(2)
    t.assert_equals(result, {2, 401, 'Anna', 23})

    -- primary key = 3 -> bucket_id = 2804 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(3)
    t.assert_equals(result, {3, 2804, 'Daria', 18})
end

pgroup.test_object_all_success_rollback_on_error = function(g)
    -- upsert_object_many
    local result, errs = g.cluster.main_server.net_box:call('crud.upsert_object_many', {
        'customers',
        {
            {{id = 1, name = 'Fedor', age = 59}, {{'+', 'age', 25}, {'=', 'name', 'Leo Tolstoy'},}},
            {{id = 2, name = 'Anna', age = 23}, {{'+', 'age', 12}}},
            {{id = 3, name = 'Daria', age = 18}, {{'=', 'name', 'Jane'}}}
        },
        {
            rollback_on_error = true,
        }
    })

    t.assert_equals(errs, nil)
    t.assert_equals(result.metadata, {
        {name = 'id', type = 'unsigned'},
        {name = 'bucket_id', type = 'unsigned'},
        {name = 'name', type = 'string'},
        {name = 'age', type = 'number'},
    })
    t.assert_equals(result.rows, nil)

    -- get
    -- primary key = 1 -> bucket_id = 477 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:get(1)
    t.assert_equals(result, {1, 477, 'Fedor', 59})

    -- primary key = 2 -> bucket_id = 401 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:get(2)
    t.assert_equals(result, {2, 401, 'Anna', 23})

    -- primary key = 3 -> bucket_id = 2804 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(3)
    t.assert_equals(result, {3, 2804, 'Daria', 18})
end

pgroup.test_object_partial_success_rollback_on_error = function(g)
    -- insert
    -- primary key = 22 -> bucket_id = 655 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:insert({22, 655, 'Alex', 34})
    t.assert_equals(result, {22, 655, 'Alex', 34})

    -- primary key = 3 -> bucket_id = 2804 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:insert({3, 2804, 'Daria', 18})
    t.assert_equals(result, {3, 2804, 'Daria', 18})

    -- primary key = 9 -> bucket_id = 1644 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:insert({9, 1644, 'Nicolo', 35})
    t.assert_equals(result, {9, 1644, 'Nicolo', 35})

    -- upsert_object_many
    -- stop_on_error = false, rollback_on_error = true
    -- two error on one storage with rollback
    local result, errs = g.cluster.main_server.net_box:call('crud.upsert_object_many', {
        'customers',
        {
            {{id = 22, name = 'Alex', age = 34}, {{'+', 'age', 1}}},
            {{id = 92, name = 'Artur', age = 29}, {{'+', 'age', 2}}},
            {{id = 3, name = 'Anastasia', age = 22}, {{'+', 'age', '3'}}},
            {{id = 5, name = 'Sergey', age = 25}, {{'+', 'age', 4}}},
            {{id = 9, name = 'Anna', age = 30}, {{'+', 'age', '5'}}}
        },
        {
            rollback_on_error = true,
        }
    })

    t.assert_not_equals(errs, nil)
    t.assert_equals(#errs, 3)

    table.sort(errs, function(err1, err2) return err1.operation_data[1] < err2.operation_data[1] end)

    if helpers.tarantool_version_at_least(2, 3) then
        t.assert_str_contains(errs[1].err,
                'Argument type in operation \'+\' on field \'age\' does not match field type')
    else
        t.assert_str_contains(errs[1].err,
                'Argument type in operation \'+\' on field 4 does not match field type')
    end
    t.assert_equals(errs[1].operation_data, {3, 2804, 'Anastasia', 22})

    if helpers.tarantool_version_at_least(2, 3) then
        t.assert_str_contains(errs[2].err,
                'Argument type in operation \'+\' on field \'age\' does not match field type')
    else
        t.assert_str_contains(errs[2].err,
                'Argument type in operation \'+\' on field 4 does not match field type')
    end
    t.assert_equals(errs[2].operation_data, {9, 1644, 'Anna', 30})

    t.assert_str_contains(errs[3].err, batching_utils.rollback_on_error_msg)
    t.assert_equals(errs[3].operation_data, {92, 2040, "Artur", 29})

    t.assert_equals(result.metadata, {
        {name = 'id', type = 'unsigned'},
        {name = 'bucket_id', type = 'unsigned'},
        {name = 'name', type = 'string'},
        {name = 'age', type = 'number'},
    })
    t.assert_equals(result.rows, nil)

    -- get
    -- primary key = 22 -> bucket_id = 655 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:get(22)
    t.assert_equals(result, {22, 655, 'Alex', 35})

    -- primary key = 92 -> bucket_id = 2040 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(92)
    t.assert_equals(result, nil)

    -- primary key = 5 -> bucket_id = 1172 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:get(5)
    t.assert_equals(result, {5, 1172, 'Sergey', 25})

    -- primary key = 3 -> bucket_id = 2804 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(3)
    t.assert_equals(result, {3, 2804, 'Daria', 18})

    -- primary key = 9 -> bucket_id = 1644 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(9)
    t.assert_equals(result, {9, 1644, 'Nicolo', 35})
end

pgroup.test_partial_success_rollback_on_error = function(g)
    -- insert
    -- primary key = 22 -> bucket_id = 655 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:insert({22, 655, 'Alex', 34})
    t.assert_equals(result, {22, 655, 'Alex', 34})

    -- primary key = 3 -> bucket_id = 2804 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:insert({3, 2804, 'Daria', 18})
    t.assert_equals(result, {3, 2804, 'Daria', 18})

    -- primary key = 9 -> bucket_id = 1644 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:insert({9, 1644, 'Nicolo', 35})
    t.assert_equals(result, {9, 1644, 'Nicolo', 35})

    -- upsert_many
    -- stop_on_error = false, rollback_on_error = true
    -- two error on one storage with rollback
    local result, errs = g.cluster.main_server.net_box:call('crud.upsert_many', {
        'customers',
        {
            {{22, box.NULL, 'Peter', 24}, {{'+', 'age', 1}}},
            {{92, box.NULL, 'Artur', 29}, {{'+', 'age', 2}}},
            {{3, box.NULL, 'Anastasia', 22}, {{'+', 'age', '3'}}},
            {{5, box.NULL, 'Sergey', 25}, {{'+', 'age', 4}}},
            {{9, box.NULL, 'Anna', 30}, {{'+', 'age', '5'}}}
        },
        {
            rollback_on_error = true,
        }
    })

    t.assert_not_equals(errs, nil)
    t.assert_equals(#errs, 3)

    table.sort(errs, function(err1, err2) return err1.operation_data[1] < err2.operation_data[1] end)

    if helpers.tarantool_version_at_least(2, 3) then
        t.assert_str_contains(errs[1].err,
                'Argument type in operation \'+\' on field \'age\' does not match field type')
    else
        t.assert_str_contains(errs[1].err,
                'Argument type in operation \'+\' on field 4 does not match field type')
    end
    t.assert_equals(errs[1].operation_data, {3, 2804, 'Anastasia', 22})

    if helpers.tarantool_version_at_least(2, 3) then
        t.assert_str_contains(errs[2].err,
                'Argument type in operation \'+\' on field \'age\' does not match field type')
    else
        t.assert_str_contains(errs[2].err,
                'Argument type in operation \'+\' on field 4 does not match field type')
    end
    t.assert_equals(errs[2].operation_data, {9, 1644, 'Anna', 30})

    t.assert_str_contains(errs[3].err, batching_utils.rollback_on_error_msg)
    t.assert_equals(errs[3].operation_data, {92, 2040, "Artur", 29})

    t.assert_equals(result.metadata, {
        {name = 'id', type = 'unsigned'},
        {name = 'bucket_id', type = 'unsigned'},
        {name = 'name', type = 'string'},
        {name = 'age', type = 'number'},
    })
    t.assert_equals(result.rows, nil)

    -- get
    -- primary key = 22 -> bucket_id = 655 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:get(22)
    t.assert_equals(result, {22, 655, 'Alex', 35})

    -- primary key = 92 -> bucket_id = 2040 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(92)
    t.assert_equals(result, nil)

    -- primary key = 5 -> bucket_id = 1172 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:get(5)
    t.assert_equals(result, {5, 1172, 'Sergey', 25})

    -- primary key = 3 -> bucket_id = 2804 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(3)
    t.assert_equals(result, {3, 2804, 'Daria', 18})

    -- primary key = 9 -> bucket_id = 1644 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(9)
    t.assert_equals(result, {9, 1644, 'Nicolo', 35})
end

pgroup.test_no_success_rollback_on_error = function(g)
    -- insert
    -- primary key = 2 -> bucket_id = 401 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:insert({2, 401, 'Anna', 23})
    t.assert_equals(result, {2, 401, 'Anna', 23})

    -- primary key = 3 -> bucket_id = 2804 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:insert({3, 2804, 'Daria', 18})
    t.assert_equals(result, {3, 2804, 'Daria', 18})

    -- primary key = 5 -> bucket_id = 1172 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:insert({5, 1172, 'Sergey', 25})
    t.assert_equals(result, {5, 1172, 'Sergey', 25})

    -- primary key = 71 -> bucket_id = 1802 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:insert({71, 1802, 'Oleg', 32})
    t.assert_equals(result, {71, 1802, 'Oleg', 32})

    -- upsert_many
    -- fails for both: s1-master s2-master
    -- two errors on each storage with rollback
    local result, errs = g.cluster.main_server.net_box:call('crud.upsert_many', {
        'customers',
        {
            {{1, box.NULL, 'Olga', 27}, {{'+', 'age', 1}}},
            {{92, box.NULL, 'Oleg', 32}, {{'+', 'age', 2}}},
            {{71, box.NULL, 'Sergey', 25}, {{'+', 'age', '3'}}},
            {{5, box.NULL, 'Anna', 30}, {{'+', 'age', '4'}}},
            {{2, box.NULL, 'Alex', 34}, {{'+', 'age', '5'}}},
            {{3, box.NULL, 'Anastasia', 22}, {{'+', 'age', '6'}}}
        },
        {
            rollback_on_error = true,
        }
    })

    t.assert_equals(result, nil)
    t.assert_not_equals(errs, nil)
    t.assert_equals(#errs, 6)

    table.sort(errs, function(err1, err2) return err1.operation_data[1] < err2.operation_data[1] end)

    t.assert_str_contains(errs[1].err, batching_utils.rollback_on_error_msg)
    t.assert_equals(errs[1].operation_data, {1, 477, "Olga", 27})

    if helpers.tarantool_version_at_least(2, 3) then
        t.assert_str_contains(errs[2].err,
                'Argument type in operation \'+\' on field \'age\' does not match field type')
    else
        t.assert_str_contains(errs[2].err,
                'Argument type in operation \'+\' on field 4 does not match field type')
    end
    t.assert_equals(errs[2].operation_data, {2, 401, 'Alex', 34})

    if helpers.tarantool_version_at_least(2, 3) then
        t.assert_str_contains(errs[3].err,
                'Argument type in operation \'+\' on field \'age\' does not match field type')
    else
        t.assert_str_contains(errs[3].err,
                'Argument type in operation \'+\' on field 4 does not match field type')
    end
    t.assert_equals(errs[3].operation_data, {3, 2804, 'Anastasia', 22})

    if helpers.tarantool_version_at_least(2, 3) then
        t.assert_str_contains(errs[4].err,
                'Argument type in operation \'+\' on field \'age\' does not match field type')
    else
        t.assert_str_contains(errs[4].err,
                'Argument type in operation \'+\' on field 4 does not match field type')
    end
    t.assert_equals(errs[4].operation_data, {5, 1172, "Anna", 30})

    if helpers.tarantool_version_at_least(2, 3) then
        t.assert_str_contains(errs[5].err,
                'Argument type in operation \'+\' on field \'age\' does not match field type')
    else
        t.assert_str_contains(errs[5].err,
                'Argument type in operation \'+\' on field 4 does not match field type')
    end
    t.assert_equals(errs[5].operation_data, {71, 1802, "Sergey", 25})

    t.assert_str_contains(errs[6].err, batching_utils.rollback_on_error_msg)
    t.assert_equals(errs[6].operation_data, {92, 2040, "Oleg", 32})

    -- primary key = 1 -> bucket_id = 477 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:get(1)
    t.assert_equals(result, nil)

    -- primary key = 92 -> bucket_id = 2040 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(92)
    t.assert_equals(result, nil)

    -- primary key = 2 -> bucket_id = 401 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:get(2)
    t.assert_equals(result, {2, 401, 'Anna', 23})

    -- primary key = 3 -> bucket_id = 2804 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(3)
    t.assert_equals(result, {3, 2804, 'Daria', 18})

    -- primary key = 5 -> bucket_id = 1172 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:get(5)
    t.assert_equals(result, {5, 1172, 'Sergey', 25})

    -- primary key = 71 -> bucket_id = 1802 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(71)
    t.assert_equals(result, {71, 1802, 'Oleg', 32})
end

pgroup.test_object_no_success_rollback_on_error = function(g)
    -- insert
    -- primary key = 2 -> bucket_id = 401 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:insert({2, 401, 'Anna', 23})
    t.assert_equals(result, {2, 401, 'Anna', 23})

    -- primary key = 3 -> bucket_id = 2804 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:insert({3, 2804, 'Daria', 18})
    t.assert_equals(result, {3, 2804, 'Daria', 18})

    -- primary key = 5 -> bucket_id = 1172 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:insert({5, 1172, 'Sergey', 25})
    t.assert_equals(result, {5, 1172, 'Sergey', 25})

    -- primary key = 71 -> bucket_id = 1802 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:insert({71, 1802, 'Oleg', 32})
    t.assert_equals(result, {71, 1802, 'Oleg', 32})

    -- upsert_object_many
    -- fails for both: s1-master s2-master
    -- two errors on each storage with rollback
    local result, errs = g.cluster.main_server.net_box:call('crud.upsert_object_many', {
        'customers',
        {
            {{id = 1, name = 'Olga', age = 27}, {{'+', 'age', 1}}},
            {{id = 92, name = 'Oleg', age = 32}, {{'+', 'age', 2}}},
            {{id = 71, name = 'Sergey', age = 25}, {{'+', 'age', '3'}}},
            {{id = 5, name = 'Anna', age = 30}, {{'+', 'age', '4'}}},
            {{id = 2, name = 'Alex', age = 34}, {{'+', 'age', '5'}}},
            {{id = 3, name = 'Anastasia', age = 22}, {{'+', 'age', '6'}}}
        },
        {
            rollback_on_error = true,
        }
    })

    t.assert_equals(result, nil)
    t.assert_not_equals(errs, nil)
    t.assert_equals(#errs, 6)

    table.sort(errs, function(err1, err2) return err1.operation_data[1] < err2.operation_data[1] end)

    t.assert_str_contains(errs[1].err, batching_utils.rollback_on_error_msg)
    t.assert_equals(errs[1].operation_data, {1, 477, "Olga", 27})

    if helpers.tarantool_version_at_least(2, 3) then
        t.assert_str_contains(errs[2].err,
                'Argument type in operation \'+\' on field \'age\' does not match field type')
    else
        t.assert_str_contains(errs[2].err,
                'Argument type in operation \'+\' on field 4 does not match field type')
    end
    t.assert_equals(errs[2].operation_data, {2, 401, 'Alex', 34})

    if helpers.tarantool_version_at_least(2, 3) then
        t.assert_str_contains(errs[3].err,
                'Argument type in operation \'+\' on field \'age\' does not match field type')
    else
        t.assert_str_contains(errs[3].err,
                'Argument type in operation \'+\' on field 4 does not match field type')
    end
    t.assert_equals(errs[3].operation_data, {3, 2804, 'Anastasia', 22})

    if helpers.tarantool_version_at_least(2, 3) then
        t.assert_str_contains(errs[4].err,
                'Argument type in operation \'+\' on field \'age\' does not match field type')
    else
        t.assert_str_contains(errs[4].err,
                'Argument type in operation \'+\' on field 4 does not match field type')
    end
    t.assert_equals(errs[4].operation_data, {5, 1172, "Anna", 30})

    if helpers.tarantool_version_at_least(2, 3) then
        t.assert_str_contains(errs[5].err,
                'Argument type in operation \'+\' on field \'age\' does not match field type')
    else
        t.assert_str_contains(errs[5].err,
                'Argument type in operation \'+\' on field 4 does not match field type')
    end
    t.assert_equals(errs[5].operation_data, {71, 1802, "Sergey", 25})

    t.assert_str_contains(errs[6].err, batching_utils.rollback_on_error_msg)
    t.assert_equals(errs[6].operation_data, {92, 2040, "Oleg", 32})

    -- primary key = 1 -> bucket_id = 477 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:get(1)
    t.assert_equals(result, nil)

    -- primary key = 92 -> bucket_id = 2040 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(92)
    t.assert_equals(result, nil)

    -- primary key = 2 -> bucket_id = 401 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:get(2)
    t.assert_equals(result, {2, 401, 'Anna', 23})

    -- primary key = 3 -> bucket_id = 2804 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(3)
    t.assert_equals(result, {3, 2804, 'Daria', 18})

    -- primary key = 5 -> bucket_id = 1172 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:get(5)
    t.assert_equals(result, {5, 1172, 'Sergey', 25})

    -- primary key = 71 -> bucket_id = 1802 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(71)
    t.assert_equals(result, {71, 1802, 'Oleg', 32})
end

pgroup.test_all_success_rollback_and_stop_on_error = function(g)
    -- upsert_many
    -- all success
    local result, errs = g.cluster.main_server.net_box:call('crud.upsert_many', {
        'customers',
        {
            {{2, box.NULL, 'Anna', 23}, {{'+', 'age', 1}}},
            {{3, box.NULL, 'Daria', 18}, {{'+', 'age', 1}}},
            {{71, box.NULL, 'Oleg', 32}, {{'+', 'age', 1}}}
        },
        {
            stop_on_error = true,
            rollback_on_error  = true,
        }
    })

    t.assert_equals(errs, nil)
    t.assert_equals(result.metadata, {
        {name = 'id', type = 'unsigned'},
        {name = 'bucket_id', type = 'unsigned'},
        {name = 'name', type = 'string'},
        {name = 'age', type = 'number'},
    })
    t.assert_equals(result.rows, nil)

    -- get
    -- primary key = 2 -> bucket_id = 401 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:get(2)
    t.assert_equals(result, {2, 401, 'Anna', 23})

    -- primary key = 3 -> bucket_id = 2804 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(3)
    t.assert_equals(result, {3, 2804, 'Daria', 18})

    -- primary key = 71 -> bucket_id = 1802 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(71)
    t.assert_equals(result, {71, 1802, 'Oleg', 32})
end

pgroup.test_object_all_success_rollback_and_stop_on_error = function(g)
    -- upsert_object_many
    -- all success
    local result, errs = g.cluster.main_server.net_box:call('crud.upsert_object_many', {
        'customers',
        {
            {{id = 2, name = 'Anna', age = 23}, {{'+', 'age', 1}}},
            {{id = 3, name = 'Daria', age = 18}, {{'+', 'age', 1}}},
            {{id = 71, name = 'Oleg', age = 32}, {{'+', 'age', 1}}}
        },
        {
            stop_on_error = true,
            rollback_on_error  = true,
        }
    })

    t.assert_equals(errs, nil)
    t.assert_equals(result.metadata, {
        {name = 'id', type = 'unsigned'},
        {name = 'bucket_id', type = 'unsigned'},
        {name = 'name', type = 'string'},
        {name = 'age', type = 'number'},
    })
    t.assert_equals(result.rows, nil)

    -- get
    -- primary key = 2 -> bucket_id = 401 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:get(2)
    t.assert_equals(result, {2, 401, 'Anna', 23})

    -- primary key = 3 -> bucket_id = 2804 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(3)
    t.assert_equals(result, {3, 2804, 'Daria', 18})

    -- primary key = 71 -> bucket_id = 1802 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(71)
    t.assert_equals(result, {71, 1802, 'Oleg', 32})
end

pgroup.test_partial_success_rollback_and_stop_on_error = function(g)
    -- insert
    -- primary key = 3 -> bucket_id = 2804 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:insert({3, 2804, 'Daria', 18})
    t.assert_equals(result, {3, 2804, 'Daria', 18})

    -- primary key = 71 -> bucket_id = 1802 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:insert({71, 1802, 'Oleg', 32})
    t.assert_equals(result, {71, 1802, 'Oleg', 32})

    -- upsert_many
    -- stop_on_error = true, rollback_on_error = true
    -- two error on one storage with rollback, inserts stop by error on this storage
    -- inserts before error are rollbacked
    local result, errs = g.cluster.main_server.net_box:call('crud.upsert_many', {
        'customers',
        {
            {{22, box.NULL, 'Alex', 34}, {{'+', 'age', 1}}},
            {{92, box.NULL, 'Artur', 29}, {{'+', 'age', 2}}},
            {{3, box.NULL, 'Anastasia', 22}, {{'+', 'age', '3'}}},
            {{5, box.NULL, 'Sergey', 25}, {{'+', 'age', 4}}},
            {{9, box.NULL, 'Anna', 30}, {{'+', 'age', 5}}},
            {{71, box.NULL, 'Oksana', 29}, {{'+', 'age', '6'}}},
        },
        {
            stop_on_error = true,
            rollback_on_error  = true,
        }
    })

    t.assert_not_equals(errs, nil)
    t.assert_equals(#errs, 4)

    table.sort(errs, function(err1, err2) return err1.operation_data[1] < err2.operation_data[1] end)

    if helpers.tarantool_version_at_least(2, 3) then
        t.assert_str_contains(errs[1].err,
                'Argument type in operation \'+\' on field \'age\' does not match field type')
    else
        t.assert_str_contains(errs[1].err,
                'Argument type in operation \'+\' on field 4 does not match field type')
    end
    t.assert_equals(errs[1].operation_data, {3, 2804, 'Anastasia', 22})

    t.assert_str_contains(errs[2].err, batching_utils.stop_on_error_msg)
    t.assert_equals(errs[2].operation_data, {9, 1644, "Anna", 30})

    t.assert_str_contains(errs[3].err, batching_utils.stop_on_error_msg)
    t.assert_equals(errs[3].operation_data, {71, 1802, "Oksana", 29})

    t.assert_str_contains(errs[4].err, batching_utils.rollback_on_error_msg)
    t.assert_equals(errs[4].operation_data, {92, 2040, "Artur", 29})

    t.assert_equals(result.metadata, {
        {name = 'id', type = 'unsigned'},
        {name = 'bucket_id', type = 'unsigned'},
        {name = 'name', type = 'string'},
        {name = 'age', type = 'number'},
    })
    t.assert_equals(result.rows, nil)

    -- get
    -- primary key = 22 -> bucket_id = 655 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:get(22)
    t.assert_equals(result, {22, 655, 'Alex', 34})

    -- primary key = 92 -> bucket_id = 2040 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(92)
    t.assert_equals(result, nil)

    -- primary key = 5 -> bucket_id = 1172 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:get(5)
    t.assert_equals(result, {5, 1172, 'Sergey', 25})

    -- primary key = 3 -> bucket_id = 2804 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(3)
    t.assert_equals(result, {3, 2804, 'Daria', 18})

    -- primary key = 9 -> bucket_id = 1644 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(9)
    t.assert_equals(result, nil)

    -- primary key = 71 -> bucket_id = 1802 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(71)
    t.assert_equals(result, {71, 1802, 'Oleg', 32})
end

pgroup.test_object_partial_success_rollback_and_stop_on_error = function(g)
    -- insert
    -- primary key = 3 -> bucket_id = 2804 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:insert({3, 2804, 'Daria', 18})
    t.assert_equals(result, {3, 2804, 'Daria', 18})

    -- primary key = 71 -> bucket_id = 1802 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:insert({71, 1802, 'Oleg', 32})
    t.assert_equals(result, {71, 1802, 'Oleg', 32})

    -- upsert_object_many
    -- stop_on_error = true, rollback_on_error = true
    -- two error on one storage with rollback, inserts stop by error on this storage
    -- inserts before error are rollbacked
    local result, errs = g.cluster.main_server.net_box:call('crud.upsert_object_many', {
        'customers',
        {
            {{id = 22, name = 'Alex', age = 34}, {{'+', 'age', 1}}},
            {{id = 92, name = 'Artur', age = 29}, {{'+', 'age', 2}}},
            {{id = 3, name = 'Anastasia', age = 22}, {{'+', 'age', '3'}}},
            {{id = 5, name = 'Sergey', age = 25}, {{'+', 'age', 4}}},
            {{id = 9, name = 'Anna', age = 30}, {{'+', 'age', 5}}},
            {{id = 71, name = 'Oksana', age = 29}, {{'+', 'age', '6'}}},
        },
        {
            stop_on_error = true,
            rollback_on_error  = true,
        }
    })

    t.assert_not_equals(errs, nil)
    t.assert_equals(#errs, 4)

    table.sort(errs, function(err1, err2) return err1.operation_data[1] < err2.operation_data[1] end)

    if helpers.tarantool_version_at_least(2, 3) then
        t.assert_str_contains(errs[1].err,
                'Argument type in operation \'+\' on field \'age\' does not match field type')
    else
        t.assert_str_contains(errs[1].err,
                'Argument type in operation \'+\' on field 4 does not match field type')
    end
    t.assert_equals(errs[1].operation_data, {3, 2804, 'Anastasia', 22})

    t.assert_str_contains(errs[2].err, batching_utils.stop_on_error_msg)
    t.assert_equals(errs[2].operation_data, {9, 1644, "Anna", 30})

    t.assert_str_contains(errs[3].err, batching_utils.stop_on_error_msg)
    t.assert_equals(errs[3].operation_data, {71, 1802, "Oksana", 29})

    t.assert_str_contains(errs[4].err, batching_utils.rollback_on_error_msg)
    t.assert_equals(errs[4].operation_data, {92, 2040, "Artur", 29})

    t.assert_equals(result.metadata, {
        {name = 'id', type = 'unsigned'},
        {name = 'bucket_id', type = 'unsigned'},
        {name = 'name', type = 'string'},
        {name = 'age', type = 'number'},
    })
    t.assert_equals(result.rows, nil)

    -- get
    -- primary key = 22 -> bucket_id = 655 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:get(22)
    t.assert_equals(result, {22, 655, 'Alex', 34})

    -- primary key = 92 -> bucket_id = 2040 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(92)
    t.assert_equals(result, nil)

    -- primary key = 5 -> bucket_id = 1172 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:get(5)
    t.assert_equals(result, {5, 1172, 'Sergey', 25})

    -- primary key = 3 -> bucket_id = 2804 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(3)
    t.assert_equals(result, {3, 2804, 'Daria', 18})

    -- primary key = 9 -> bucket_id = 1644 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(9)
    t.assert_equals(result, nil)

    -- primary key = 71 -> bucket_id = 1802 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(71)
    t.assert_equals(result, {71, 1802, 'Oleg', 32})
end

pgroup.test_partial_result = function(g)
    -- bad fields format
    local result, errs = g.cluster.main_server.net_box:call('crud.upsert_many', {
        'customers',
        {
            {{15, box.NULL, 'Fedor', 59}, {{'+', 'age', 1}}},
            {{25, box.NULL, 'Anna', 23}, {{'+', 'age', 1}}},
        },
        {fields = {'id', 'invalid'}},
    })

    t.assert_equals(result, nil)
    t.assert_not_equals(errs, nil)
    t.assert_equals(#errs, 1)
    t.assert_str_contains(errs[1].err, 'Space format doesn\'t contain field named "invalid"')

    -- upsert_many
    local result, errs = g.cluster.main_server.net_box:call('crud.upsert_many', {
        'customers',
        {
            {{1, box.NULL, 'Fedor', 59}, {{'+', 'age', 1}}},
            {{2, box.NULL, 'Anna', 23}, {{'+', 'age', 1}}},
            {{3, box.NULL, 'Daria', 18}, {{'+', 'age', 1}}},
        },
        {fields = {'id', 'name'}},
    })

    t.assert_equals(errs, nil)
    t.assert_equals(result.metadata, {
        {name = 'id', type = 'unsigned'},
        {name = 'name', type = 'string'},
    })
    t.assert_equals(result.rows, nil)
end

pgroup.test_object_partial_result = function(g)
    -- bad fields format
    local result, errs = g.cluster.main_server.net_box:call('crud.upsert_object_many', {
        'customers',
        {
            {{id = 15, name = 'Fedor', age = 59}, {{'+', 'age', 1}}},
            {{id = 25, name = 'Anna', age = 23}, {{'+', 'age', 1}}},
        },
        {fields = {'id', 'invalid'}},
    })

    t.assert_equals(result, nil)
    t.assert_not_equals(errs, nil)
    t.assert_equals(#errs, 1)
    t.assert_str_contains(errs[1].err, 'Space format doesn\'t contain field named "invalid"')

    -- upsert_object_many
    local result, errs = g.cluster.main_server.net_box:call('crud.upsert_object_many', {
        'customers',
        {
            {{id = 1, name = 'Fedor', age = 59}, {{'+', 'age', 1}}},
            {{id = 2, name = 'Anna', age = 23}, {{'+', 'age', 1}}},
            {{id = 3, name = 'Daria', age = 18}, {{'+', 'age', 1}}},
        },
        {fields = {'id', 'name'}},
    })

    t.assert_equals(errs, nil)
    t.assert_equals(result.metadata, {
        {name = 'id', type = 'unsigned'},
        {name = 'name', type = 'string'},
    })
    t.assert_equals(result.rows, nil)
end

pgroup.test_opts_not_damaged = function(g)
    -- upsert_many
    local batch_upsert_opts = {timeout = 1, fields = {'name', 'age'}}
    local new_batch_upsert_opts, err = g.cluster.main_server:eval([[
        local crud = require('crud')

        local batch_upsert_opts = ...

        local _, err = crud.upsert_many('customers', {
            {{1, box.NULL, 'Alex', 59}, {{'+', 'age', 1}},}
        }, batch_upsert_opts)

        return batch_upsert_opts, err
    ]], {batch_upsert_opts})

    t.assert_equals(err, nil)
    t.assert_equals(new_batch_upsert_opts, batch_upsert_opts)

    -- upsert_object_many
    local batch_upsert_opts = {timeout = 1, fields = {'name', 'age'}}
    local new_batch_upsert_opts, err = g.cluster.main_server:eval([[
        local crud = require('crud')

        local batch_upsert_opts = ...

        local _, err = crud.upsert_object_many('customers', {
            {{id = 2, name = 'Fedor', age = 59}, {{'+', 'age', 1}},}
        }, batch_upsert_opts)

        return batch_upsert_opts, err
    ]], {batch_upsert_opts})

    t.assert_equals(err, nil)
    t.assert_equals(new_batch_upsert_opts, batch_upsert_opts)
end

pgroup.test_noreturn_opt = function(g)
    -- upsert_many with noreturn, all tuples are correct
    local result, errs = g.cluster.main_server.net_box:call('crud.upsert_many', {
        'customers',
        {
            {{1, box.NULL, 'Alex', 59}, {{'+', 'age', 1}}},
            {{2, box.NULL, 'Anna', 23}, {{'+', 'age', 1}}},
            {{3, box.NULL, 'Daria', 18}, {{'+', 'age', 1}}}
        },
        {noreturn = true},
    })

    t.assert_equals(errs, nil)
    t.assert_equals(result, nil)

    -- upsert_many with noreturn, some tuples are correct
    local result, errs = g.cluster.main_server.net_box:call('crud.upsert_many', {
        'customers',
        {
            {{1, box.NULL, 'Alex', 59}, {{'+', 'age', 1}}},
            {{box.NULL, box.NULL, 'Anna', 23}, {{'+', 'age', 1}}},
            {{box.NULL, box.NULL, 'Daria', 18}, {{'+', 'age', 1}}}
        },
        {noreturn = true},
    })

    t.assert_not_equals(errs, nil)
    t.assert_equals(#errs, 2)
    t.assert_equals(result, nil)

    -- upsert_many with noreturn, all tuples are not correct
    local result, errs = g.cluster.main_server.net_box:call('crud.upsert_many', {
        'customers',
        {
            {{box.NULL, box.NULL, 'Alex', 59}, {{'+', 'age', 1}}},
            {{box.NULL, box.NULL, 'Anna', 23}, {{'+', 'age', 1}}},
            {{box.NULL, box.NULL, 'Daria', 18}, {{'+', 'age', 1}}}
        },
        {noreturn = true},
    })

    t.assert_not_equals(errs, nil)
    t.assert_equals(#errs, 3)
    t.assert_equals(result, nil)

    -- upsert_object_many with noreturn, all tuples are correct
    local result, errs = g.cluster.main_server.net_box:call('crud.upsert_object_many', {
        'customers',
        {
            {{id = 1, name = 'Fedor', age = 59}, {{'+', 'age', 1}}},
            {{id = 2, name = 'Anna', age = 23}, {{'+', 'age', 1}}},
            {{id = 3, name = 'Daria', age = 18}, {{'+', 'age', 1}}},
        },
        {noreturn = true},
    })

    t.assert_equals(errs, nil)
    t.assert_equals(result, nil)

    -- upsert_object_many with noreturn, some tuples are correct
    local result, errs = g.cluster.main_server.net_box:call('crud.upsert_object_many', {
        'customers',
        {
            {{id = 1, name = 'Fedor', age = 59}, {{'+', 'age', 1}}},
            {{id = box.NULL, name = 'Anna', age = 23}, {{'+', 'age', 1}}},
            {{id = box.NULL, name = 'Daria', age = 18}, {{'+', 'age', 1}}},
        },
        {noreturn = true},
    })

    t.assert_not_equals(errs, nil)
    t.assert_equals(#errs, 2)
    t.assert_equals(result, nil)

    -- upsert_object_many with noreturn, all tuples are not correct
    local result, errs = g.cluster.main_server.net_box:call('crud.upsert_object_many', {
        'customers',
        {
            {{id = box.NULL, name = 'Fedor', age = 59}, {{'+', 'age', 1}}},
            {{id = box.NULL, name = 'Anna', age = 23}, {{'+', 'age', 1}}},
            {{id = box.NULL, name = 'Daria', age = 18}, {{'+', 'age', 1}}},
        },
        {noreturn = true},
    })

    t.assert_not_equals(errs, nil)
    t.assert_equals(#errs, 3)
    t.assert_equals(result, nil)
end
