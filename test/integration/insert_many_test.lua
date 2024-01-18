local t = require('luatest')
local crud = require('crud')

local helpers = require('test.helper')

local batching_utils = require('crud.common.batching_utils')

local pgroup = t.group('insert_many', helpers.backend_matrix({
    {engine = 'memtx'},
    {engine = 'vinyl'},
}))

pgroup.before_all(function(g)
    helpers.start_default_cluster(g, 'srv_batch_operations')
end)

pgroup.after_all(function(g)
    helpers.stop_cluster(g.cluster, g.params.backend)
end)

pgroup.before_each(function(g)
    helpers.truncate_space_on_cluster(g.cluster, 'customers')
end)

pgroup.test_non_existent_space = function(g)
    -- insert_many
    local result, errs = g.router:call('crud.insert_many', {
        'non_existent_space',
        {
            {1, box.NULL, 'Alex', 59},
            {2, box.NULL, 'Anna', 23},
            {3, box.NULL, 'Daria', 18}
        }
    })

    t.assert_equals(result, nil)
    t.assert_not_equals(errs, nil)
    t.assert_equals(#errs, 1)
    t.assert_str_contains(errs[1].err, 'Space "non_existent_space" doesn\'t exist')

    -- insert_object_many
    -- default: stop_on_error == false
    local result, errs = g.router:call('crud.insert_object_many', {
        'non_existent_space',
        {
            {id = 1, name = 'Fedor', age = 59},
            {id = 2, name = 'Anna', age = 23},
            {id = 3, name = 'Daria', age = 18}
        }
    })

    t.assert_equals(result, nil)
    t.assert_not_equals(errs, nil)

    -- we got 3 errors about non existent space, because it caused by flattening objects
    t.assert_equals(#errs, 3)
    t.assert_str_contains(errs[1].err, 'Space "non_existent_space" doesn\'t exist')
    t.assert_str_contains(errs[2].err, 'Space "non_existent_space" doesn\'t exist')
    t.assert_str_contains(errs[3].err, 'Space "non_existent_space" doesn\'t exist')

    -- insert_object_many
    -- stop_on_error == true
    local result, errs = g.router:call('crud.insert_object_many', {
        'non_existent_space',
        {
            {id = 1, name = 'Fedor', age = 59},
            {id = 2, name = 'Anna', age = 23},
            {id = 3, name = 'Daria', age = 18}
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
    local result, errs = g.router:call('crud.insert_object_many', {
        'customers',
        {
            {id = 1, name = 'Fedor', age = 59},
            {id = 2, name = 'Anna'},
        }
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

    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_items_equals(objects, {
        {id = 1, name = 'Fedor', age = 59, bucket_id = 477},
    })

    -- get
    -- primary key = 1 -> bucket_id = 477 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:get(1)
    t.assert_equals(result, {1, 477, 'Fedor', 59})

    -- bad format
    local result, errs = g.router:call('crud.insert_object_many', {
        'customers',
        {
            {id = 1, name = 'Fedor', age = 59},
            {id = 2, name = 'Anna'},
        }
    })

    t.assert_equals(result, nil)
    t.assert_not_equals(errs, nil)
    t.assert_equals(#errs, 2)

    t.assert_str_contains(errs[1].err, 'Duplicate key exists')
    t.assert_equals(errs[1].operation_data, {1, 477, "Fedor", 59})

    t.assert_str_contains(errs[2].err, 'Field \"age\" isn\'t nullable')
    t.assert_equals(errs[2].operation_data, {id = 2, name = 'Anna'})

    -- get
    -- primary key = 1 -> bucket_id = 477 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:get(1)
    t.assert_equals(result, {1, 477, 'Fedor', 59})

    -- bad format
    -- two errors, default: stop_on_error == false
    local result, errs = g.router:call('crud.insert_object_many', {
        'customers',
        {
            {id = 1, name = 'Fedor'},
            {id = 2, name = 'Anna'},
        }
    })

    t.assert_equals(result, nil)
    t.assert_not_equals(errs, nil)
    t.assert_equals(#errs, 2)

    table.sort(errs, function(err1, err2) return err1.operation_data.id < err2.operation_data.id end)

    t.assert_str_contains(errs[1].err, 'Field \"age\" isn\'t nullable')
    t.assert_equals(errs[1].operation_data, {id = 1, name = 'Fedor'})

    t.assert_str_contains(errs[2].err, 'Field \"age\" isn\'t nullable')
    t.assert_equals(errs[2].operation_data, {id = 2, name = 'Anna'})
end

pgroup.test_all_success = function(g)
    -- insert_many
    -- all success
    local result, errs = g.router:call('crud.insert_many', {
        'customers',
        {
            {1, box.NULL, 'Fedor', 59},
            {2, box.NULL, 'Anna', 23},
            {3, box.NULL, 'Daria', 18}
        }
    })

    t.assert_equals(errs, nil)
    t.assert_equals(result.metadata, {
        {name = 'id', type = 'unsigned'},
        {name = 'bucket_id', type = 'unsigned'},
        {name = 'name', type = 'string'},
        {name = 'age', type = 'number'},
    })

    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_items_equals(objects, {
        {id = 1, name = 'Fedor', age = 59, bucket_id = 477},
        {id = 2, name = 'Anna', age = 23, bucket_id = 401},
        {id = 3, name = 'Daria', age = 18, bucket_id = 2804},
    })

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
    -- batch_insert_object
    -- all success
    local result, errs = g.router:call('crud.insert_object_many', {
        'customers',
        {
            {id = 1, name = 'Fedor', age = 59},
            {id = 2, name = 'Anna', age = 23},
            {id = 3, name = 'Daria', age = 18}
        }
    })

    t.assert_equals(errs, nil)
    t.assert_equals(result.metadata, {
        {name = 'id', type = 'unsigned'},
        {name = 'bucket_id', type = 'unsigned'},
        {name = 'name', type = 'string'},
        {name = 'age', type = 'number'},
    })

    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_items_equals(objects, {
        {id = 1, name = 'Fedor', age = 59, bucket_id = 477},
        {id = 2, name = 'Anna', age = 23, bucket_id = 401},
        {id = 3, name = 'Daria', age = 18, bucket_id = 2804},
    })

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
    -- insert
    -- primary key = 3 -> bucket_id = 2804 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:insert({3, 2804, 'Daria', 18})
    t.assert_equals(result, {3, 2804, 'Daria', 18})

    -- insert_many
    -- default: stop_on_error = false, rollback_on_error = false
    -- one error on one storage without rollback
    local result, errs = g.router:call('crud.insert_many', {
        'customers',
        {
            {22, box.NULL, 'Alex', 34},
            {3, box.NULL, 'Anastasia', 22},
            {5, box.NULL, 'Sergey', 25},
            {9, box.NULL, 'Anna', 30},
        }
    })

    t.assert_not_equals(errs, nil)
    t.assert_equals(#errs, 1)
    t.assert_str_contains(errs[1].err, 'Duplicate key exists')
    t.assert_equals(errs[1].operation_data, {3, 2804, 'Anastasia', 22})
    t.assert_equals(result.metadata, {
        {name = 'id', type = 'unsigned'},
        {name = 'bucket_id', type = 'unsigned'},
        {name = 'name', type = 'string'},
        {name = 'age', type = 'number'},
    })

    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_items_equals(objects, {
        {id = 5, name = 'Sergey', age = 25, bucket_id = 1172},
        {id = 9, name = 'Anna', age = 30, bucket_id = 1644},
        {id = 22, name = 'Alex', age = 34, bucket_id = 655},
    })

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

    -- primary key = 9 -> bucket_id = 1644 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(9)
    t.assert_equals(result, {9, 1644, 'Anna', 30})
end

pgroup.test_object_one_error = function(g)
    -- insert
    -- primary key = 3 -> bucket_id = 2804 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:insert({3, 2804, 'Daria', 18})
    t.assert_equals(result, {3, 2804, 'Daria', 18})

    -- batch_insert_object again
    -- default: stop_on_error = false, rollback_on_error = false
    -- one error on one storage without rollback
    local result, errs = g.router:call('crud.insert_object_many', {
        'customers',
        {
            {id = 22, name = 'Alex', age = 34},
            {id = 3, name = 'Anastasia', age = 22},
            {id = 5, name = 'Sergey', age = 25},
            {id = 9, name = 'Anna', age = 30},
        }
    })

    t.assert_not_equals(errs, nil)
    t.assert_equals(#errs, 1)
    t.assert_str_contains(errs[1].err, 'Duplicate key exists')
    t.assert_equals(errs[1].operation_data, {3, 2804, 'Anastasia', 22})
    t.assert_equals(result.metadata, {
        {name = 'id', type = 'unsigned'},
        {name = 'bucket_id', type = 'unsigned'},
        {name = 'name', type = 'string'},
        {name = 'age', type = 'number'},
    })

    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_items_equals(objects, {
        {id = 5, name = 'Sergey', age = 25, bucket_id = 1172},
        {id = 9, name = 'Anna', age = 30, bucket_id = 1644},
        {id = 22, name = 'Alex', age = 34, bucket_id = 655},
    })

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

    -- primary key = 9 -> bucket_id = 1644 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(9)
    t.assert_equals(result, {9, 1644, 'Anna', 30})
end

pgroup.test_many_errors = function(g)
    -- insert
    -- primary key = 2 -> bucket_id = 401 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:insert({2, 401, 'Anna', 23})
    t.assert_equals(result, {2, 401, 'Anna', 23})

    -- primary key = 3 -> bucket_id = 2804 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:insert({3, 2804, 'Daria', 18})
    t.assert_equals(result, {3, 2804, 'Daria', 18})

    -- insert_many
    -- fails for both: s1-master s2-master
    -- one error on each storage, one success on each storage
    local result, errs = g.router:call('crud.insert_many', {
        'customers',
        {
            {2, box.NULL, 'Alex', 34},
            {3, box.NULL, 'Anastasia', 22},
            {10, box.NULL, 'Sergey', 25},
            {92, box.NULL, 'Artur', 29},
        }
    })

    t.assert_not_equals(errs, nil)
    t.assert_equals(#errs, 2)

    table.sort(errs, function(err1, err2) return err1.operation_data[1] < err2.operation_data[1] end)

    t.assert_str_contains(errs[1].err, 'Duplicate key exists')
    t.assert_equals(errs[1].operation_data, {2, 401, 'Alex', 34})

    t.assert_str_contains(errs[2].err, 'Duplicate key exists')
    t.assert_equals(errs[2].operation_data, {3, 2804, 'Anastasia', 22})

    t.assert_equals(result.metadata, {
        {name = 'id', type = 'unsigned'},
        {name = 'bucket_id', type = 'unsigned'},
        {name = 'name', type = 'string'},
        {name = 'age', type = 'number'},
    })

    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_items_equals(objects, {
        {id = 10, name = 'Sergey', age = 25, bucket_id = 569},
        {id = 92, name = 'Artur', age = 29, bucket_id = 2040},
    })

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
    t.assert_equals(result, {10, 569, 'Sergey', 25})

    -- primary key = 92 -> bucket_id = 2040 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(92)
    t.assert_equals(result, {92, 2040, 'Artur', 29})
end

pgroup.test_object_many_errors = function(g)
    -- insert
    -- primary key = 2 -> bucket_id = 401 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:insert({2, 401, 'Anna', 23})
    t.assert_equals(result, {2, 401, 'Anna', 23})

    -- primary key = 3 -> bucket_id = 2804 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:insert({3, 2804, 'Daria', 18})
    t.assert_equals(result, {3, 2804, 'Daria', 18})

    -- batch_insert_object again
    -- fails for both: s1-master s2-master
    -- one error on each storage, one success on each storage
    local result, errs = g.router:call('crud.insert_object_many', {
        'customers',
        {
            {id = 2, name = 'Alex', age = 34},
            {id = 3, name = 'Anastasia', age = 22},
            {id = 10, name = 'Sergey', age = 25},
            {id = 92, name = 'Artur', age = 29},
        }
    })

    t.assert_not_equals(errs, nil)
    t.assert_equals(#errs, 2)

    table.sort(errs, function(err1, err2) return err1.operation_data[1] < err2.operation_data[1] end)

    t.assert_str_contains(errs[1].err, 'Duplicate key exists')
    t.assert_equals(errs[1].operation_data, {2, 401, 'Alex', 34})

    t.assert_str_contains(errs[2].err, 'Duplicate key exists')
    t.assert_equals(errs[2].operation_data, {3, 2804, 'Anastasia', 22})

    t.assert_equals(result.metadata, {
        {name = 'id', type = 'unsigned'},
        {name = 'bucket_id', type = 'unsigned'},
        {name = 'name', type = 'string'},
        {name = 'age', type = 'number'},
    })

    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_items_equals(objects, {
        {id = 10, name = 'Sergey', age = 25, bucket_id = 569},
        {id = 92, name = 'Artur', age = 29, bucket_id = 2040},
    })

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
    t.assert_equals(result, {10, 569, 'Sergey', 25})

    -- primary key = 92 -> bucket_id = 2040 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(92)
    t.assert_equals(result, {92, 2040, 'Artur', 29})
end

pgroup.test_no_success = function(g)
    -- insert
    -- primary key = 2 -> bucket_id = 401 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:insert({2, 401, 'Anna', 23})
    t.assert_equals(result, {2, 401, 'Anna', 23})

    -- primary key = 3 -> bucket_id = 2804 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:insert({3, 2804, 'Daria', 18})
    t.assert_equals(result, {3, 2804, 'Daria', 18})

    -- primary key = 10 -> bucket_id = 569 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:insert({10, 569, 'Sergey', 25})
    t.assert_equals(result, {10, 569, 'Sergey', 25})

    -- primary key = 92 -> bucket_id = 2040 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:insert({92, 2040, 'Artur', 29})
    t.assert_equals(result, {92, 2040, 'Artur', 29})

    -- insert_many again
    -- fails for both: s1-master s2-master
    -- no success
    local result, errs = g.router:call('crud.insert_many', {
        'customers',
        {
            {2, box.NULL, 'Alex', 34},
            {3, box.NULL, 'Anastasia', 22},
            {10, box.NULL, 'Vlad', 25},
            {92, box.NULL, 'Mark', 29},
        }
    })

    t.assert_equals(result, nil)
    t.assert_not_equals(errs, nil)
    t.assert_equals(#errs, 4)

    table.sort(errs, function(err1, err2) return err1.operation_data[1] < err2.operation_data[1] end)

    t.assert_str_contains(errs[1].err, 'Duplicate key exists')
    t.assert_equals(errs[1].operation_data, {2, 401, 'Alex', 34})

    t.assert_str_contains(errs[2].err, 'Duplicate key exists')
    t.assert_equals(errs[2].operation_data, {3, 2804, 'Anastasia', 22})

    t.assert_str_contains(errs[3].err, 'Duplicate key exists')
    t.assert_equals(errs[3].operation_data, {10, 569, 'Vlad', 25})

    t.assert_str_contains(errs[4].err, 'Duplicate key exists')
    t.assert_equals(errs[4].operation_data, {92, 2040, 'Mark', 29})

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
    t.assert_equals(result, {10, 569, 'Sergey', 25})

    -- primary key = 92 -> bucket_id = 2040 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(92)
    t.assert_equals(result, {92, 2040, 'Artur', 29})
end

pgroup.test_object_no_success = function(g)
    -- insert
    -- primary key = 2 -> bucket_id = 401 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:insert({2, 401, 'Anna', 23})
    t.assert_equals(result, {2, 401, 'Anna', 23})

    -- primary key = 3 -> bucket_id = 2804 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:insert({3, 2804, 'Daria', 18})
    t.assert_equals(result, {3, 2804, 'Daria', 18})

    -- primary key = 10 -> bucket_id = 569 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['customers']:insert({10, 569, 'Sergey', 25})
    t.assert_equals(result, {10, 569, 'Sergey', 25})

    -- primary key = 92 -> bucket_id = 2040 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:insert({92, 2040, 'Artur', 29})
    t.assert_equals(result, {92, 2040, 'Artur', 29})

    -- batch_insert_object again
    -- fails for both: s1-master s2-master
    -- no success
    local result, errs = g.router:call('crud.insert_object_many', {
        'customers',
        {
            {id = 2, name = 'Alex', age = 34},
            {id = 3, name = 'Anastasia', age = 22},
            {id = 10, name = 'Vlad', age = 25},
            {id = 92, name = 'Mark', age = 29},
        }
    })

    t.assert_equals(result, nil)
    t.assert_not_equals(errs, nil)
    t.assert_equals(#errs, 4)

    table.sort(errs, function(err1, err2) return err1.operation_data[1] < err2.operation_data[1] end)

    t.assert_str_contains(errs[1].err, 'Duplicate key exists')
    t.assert_equals(errs[1].operation_data, {2, 401, 'Alex', 34})

    t.assert_str_contains(errs[2].err, 'Duplicate key exists')
    t.assert_equals(errs[2].operation_data, {3, 2804, 'Anastasia', 22})

    t.assert_str_contains(errs[3].err, 'Duplicate key exists')
    t.assert_equals(errs[3].operation_data, {10, 569, 'Vlad', 25})

    t.assert_str_contains(errs[4].err, 'Duplicate key exists')
    t.assert_equals(errs[4].operation_data, {92, 2040, 'Mark', 29})

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
    t.assert_equals(result, {10, 569, 'Sergey', 25})

    -- primary key = 92 -> bucket_id = 2040 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:get(92)
    t.assert_equals(result, {92, 2040, 'Artur', 29})
end

pgroup.test_object_bad_format_stop_on_error = function(g)
    -- bad format
    -- two errors, stop_on_error == true
    local result, errs = g.router:call('crud.insert_object_many', {
        'customers',
        {
            {id = 1, name = 'Fedor'},
            {id = 2, name = 'Anna'},
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
    -- insert_many
    -- all success
    local result, errs = g.router:call('crud.insert_many', {
        'customers',
        {
            {2, box.NULL, 'Anna', 23},
            {3, box.NULL, 'Daria', 18},
            {71, box.NULL, 'Oleg', 32}
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

    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_items_equals(objects, {
        {id = 2, name = 'Anna', age = 23, bucket_id = 401},
        {id = 3, name = 'Daria', age = 18, bucket_id = 2804},
        {id = 71, name = 'Oleg', age = 32, bucket_id = 1802},
    })

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

pgroup.test_object_all_success_stop_on_error = function(g)
    -- batch_insert_object
    -- all success
    local result, errs = g.router:call('crud.insert_object_many', {
        'customers',
        {
            {id = 2, name = 'Anna', age = 23},
            {id = 3, name = 'Daria', age = 18},
            {id = 71, name = 'Oleg', age = 32}
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

    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_items_equals(objects, {
        {id = 2, name = 'Anna', age = 23, bucket_id = 401},
        {id = 3, name = 'Daria', age = 18, bucket_id = 2804},
        {id = 71, name = 'Oleg', age = 32, bucket_id = 1802},
    })

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

    -- insert_many
    -- stop_on_error = true, rollback_on_error = false
    -- one error on one storage without rollback, inserts stop by error on this storage
    -- inserts before error are successful
    local result, errs = g.router:call('crud.insert_many', {
        'customers',
        {
            {22, box.NULL, 'Alex', 34},
            {92, box.NULL, 'Artur', 29},
            {3, box.NULL, 'Anastasia', 22},
            {5, box.NULL, 'Sergey', 25},
            {9, box.NULL, 'Anna', 30},
        },
        {
            stop_on_error = true,
        }
    })

    t.assert_not_equals(errs, nil)
    t.assert_equals(#errs, 2)

    table.sort(errs, function(err1, err2) return err1.operation_data[1] < err2.operation_data[1] end)

    t.assert_str_contains(errs[1].err, 'Duplicate key exists')
    t.assert_equals(errs[1].operation_data, {3, 2804, 'Anastasia', 22})

    t.assert_str_contains(errs[2].err, batching_utils.stop_on_error_msg)
    t.assert_equals(errs[2].operation_data, {9, 1644, "Anna", 30})

    t.assert_equals(result.metadata, {
        {name = 'id', type = 'unsigned'},
        {name = 'bucket_id', type = 'unsigned'},
        {name = 'name', type = 'string'},
        {name = 'age', type = 'number'},
    })

    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_items_equals(objects, {
        {id = 5, name = 'Sergey', age = 25, bucket_id = 1172},
        {id = 22, name = 'Alex', age = 34, bucket_id = 655},
        {id = 92, name = 'Artur', age = 29, bucket_id = 2040},
    })

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

    -- insert_object_many
    -- stop_on_error = true, rollback_on_error = false
    -- one error on one storage without rollback, inserts stop by error on this storage
    -- inserts before error are successful
    local result, errs = g.router:call('crud.insert_object_many', {
        'customers',
        {
            {id = 22, name = 'Alex', age = 34},
            {id = 92, name = 'Artur', age = 29},
            {id = 3, name = 'Anastasia', age = 22},
            {id = 5, name = 'Sergey', age = 25},
            {id = 9, name = 'Anna', age = 30},
        },
        {
            stop_on_error = true,
        }
    })

    t.assert_not_equals(errs, nil)
    t.assert_equals(#errs, 2)

    table.sort(errs, function(err1, err2) return err1.operation_data[1] < err2.operation_data[1] end)

    t.assert_str_contains(errs[1].err, 'Duplicate key exists')
    t.assert_equals(errs[1].operation_data, {3, 2804, 'Anastasia', 22})

    t.assert_str_contains(errs[2].err, batching_utils.stop_on_error_msg)
    t.assert_equals(errs[2].operation_data, {9, 1644, "Anna", 30})

    t.assert_equals(result.metadata, {
        {name = 'id', type = 'unsigned'},
        {name = 'bucket_id', type = 'unsigned'},
        {name = 'name', type = 'string'},
        {name = 'age', type = 'number'},
    })

    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_items_equals(objects, {
        {id = 5, name = 'Sergey', age = 25, bucket_id = 1172},
        {id = 22, name = 'Alex', age = 34, bucket_id = 655},
        {id = 92, name = 'Artur', age = 29, bucket_id = 2040},
    })

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

    -- insert_many
    -- fails for both: s1-master s2-master
    -- one error on each storage, all inserts stop by error
    local result, errs = g.router:call('crud.insert_many', {
        'customers',
        {
            {2, box.NULL, 'Alex', 34},
            {3, box.NULL, 'Anastasia', 22},
            {10, box.NULL, 'Sergey', 25},
            {9, box.NULL, 'Anna', 30},
            {92, box.NULL, 'Leo', 29},
        },
        {
            stop_on_error = true,
        }
    })

    t.assert_equals(result, nil)
    t.assert_not_equals(errs, nil)
    t.assert_equals(#errs, 5)

    table.sort(errs, function(err1, err2) return err1.operation_data[1] < err2.operation_data[1] end)

    t.assert_str_contains(errs[1].err, 'Duplicate key exists')
    t.assert_equals(errs[1].operation_data, {2, 401, 'Alex', 34})

    t.assert_str_contains(errs[2].err, 'Duplicate key exists')
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

    -- insert_object_many
    -- fails for both: s1-master s2-master
    -- one error on each storage, all inserts stop by error
    local result, errs = g.router:call('crud.insert_object_many', {
        'customers',
        {
            {id = 2, name = 'Alex', age = 34},
            {id = 3, name = 'Anastasia', age = 22},
            {id = 10, name = 'Sergey', age = 25},
            {id = 9, name = 'Anna', age = 30},
            {id = 92, name = 'Leo', age = 29},
        },
        {
            stop_on_error = true,
        }
    })

    t.assert_equals(result, nil)
    t.assert_not_equals(errs, nil)
    t.assert_equals(#errs, 5)

    table.sort(errs, function(err1, err2) return err1.operation_data[1] < err2.operation_data[1] end)

    t.assert_str_contains(errs[1].err, 'Duplicate key exists')
    t.assert_equals(errs[1].operation_data, {2, 401, 'Alex', 34})

    t.assert_str_contains(errs[2].err, 'Duplicate key exists')
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
    -- insert_many
    -- all success
    local result, errs = g.router:call('crud.insert_many', {
        'customers',
        {
            {2, box.NULL, 'Anna', 23},
            {3, box.NULL, 'Daria', 18},
            {71, box.NULL, 'Oleg', 32}
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

    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_items_equals(objects, {
        {id = 2, name = 'Anna', age = 23, bucket_id = 401},
        {id = 3, name = 'Daria', age = 18, bucket_id = 2804},
        {id = 71, name = 'Oleg', age = 32, bucket_id = 1802},
    })

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

pgroup.test_object_all_success_rollback_on_error = function(g)
    -- insert_object_many
    -- all success
    local result, errs = g.router:call('crud.insert_object_many', {
        'customers',
        {
            {id = 2, name = 'Anna', age = 23},
            {id = 3, name = 'Daria', age = 18},
            {id = 71, name = 'Oleg', age = 32}
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

    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_items_equals(objects, {
        {id = 2, name = 'Anna', age = 23, bucket_id = 401},
        {id = 3, name = 'Daria', age = 18, bucket_id = 2804},
        {id = 71, name = 'Oleg', age = 32, bucket_id = 1802},
    })

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

pgroup.test_partial_success_rollback_on_error = function(g)
    -- insert
    -- primary key = 3 -> bucket_id = 2804 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:insert({3, 2804, 'Daria', 18})
    t.assert_equals(result, {3, 2804, 'Daria', 18})

    -- primary key = 9 -> bucket_id = 1644 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:insert({9, 1644, 'Nicolo', 35})
    t.assert_equals(result, {9, 1644, 'Nicolo', 35})

    -- insert_many
    -- stop_on_error = false, rollback_on_error = true
    -- two error on one storage with rollback
    local result, errs = g.router:call('crud.insert_many', {
        'customers',
        {
            {22, box.NULL, 'Alex', 34},
            {92, box.NULL, 'Artur', 29},
            {3, box.NULL, 'Anastasia', 22},
            {5, box.NULL, 'Sergey', 25},
            {9, box.NULL, 'Anna', 30},
        },
        {
            rollback_on_error = true,
        }
    })

    t.assert_not_equals(errs, nil)
    t.assert_equals(#errs, 3)

    table.sort(errs, function(err1, err2) return err1.operation_data[1] < err2.operation_data[1] end)

    t.assert_str_contains(errs[1].err, 'Duplicate key exists')
    t.assert_equals(errs[1].operation_data, {3, 2804, 'Anastasia', 22})

    t.assert_str_contains(errs[2].err, 'Duplicate key exists')
    t.assert_equals(errs[2].operation_data, {9, 1644, 'Anna', 30})

    t.assert_str_contains(errs[3].err, batching_utils.rollback_on_error_msg)
    t.assert_equals(errs[3].operation_data, {92, 2040, 'Artur', 29})

    t.assert_equals(result.metadata, {
        {name = 'id', type = 'unsigned'},
        {name = 'bucket_id', type = 'unsigned'},
        {name = 'name', type = 'string'},
        {name = 'age', type = 'number'},
    })

    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_items_equals(objects, {
        {id = 5, name = 'Sergey', age = 25, bucket_id = 1172},
        {id = 22, name = 'Alex', age = 34, bucket_id = 655},
    })

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
    t.assert_equals(result, {9, 1644, 'Nicolo', 35})
end

pgroup.test_object_partial_success_rollback_on_error = function(g)
    -- insert
    -- primary key = 3 -> bucket_id = 2804 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:insert({3, 2804, 'Daria', 18})
    t.assert_equals(result, {3, 2804, 'Daria', 18})

    -- primary key = 9 -> bucket_id = 1644 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['customers']:insert({9, 1644, 'Nicolo', 35})
    t.assert_equals(result, {9, 1644, 'Nicolo', 35})

    -- insert_object_many
    -- stop_on_error = false, rollback_on_error = true
    -- two error on one storage with rollback
    local result, errs = g.router:call('crud.insert_object_many', {
        'customers',
        {
            {id = 22, name = 'Alex', age = 34},
            {id = 92, name = 'Artur', age = 29},
            {id = 3, name = 'Anastasia', age = 22},
            {id = 5, name = 'Sergey', age = 25},
            {id = 9, name = 'Anna', age = 30},
        },
        {
            rollback_on_error = true,
        }
    })

    t.assert_not_equals(errs, nil)
    t.assert_equals(#errs, 3)

    table.sort(errs, function(err1, err2) return err1.operation_data[1] < err2.operation_data[1] end)

    t.assert_str_contains(errs[1].err, 'Duplicate key exists')
    t.assert_equals(errs[1].operation_data, {3, 2804, 'Anastasia', 22})

    t.assert_str_contains(errs[2].err, 'Duplicate key exists')
    t.assert_equals(errs[2].operation_data, {9, 1644, 'Anna', 30})

    t.assert_str_contains(errs[3].err, batching_utils.rollback_on_error_msg)
    t.assert_equals(errs[3].operation_data, {92, 2040, "Artur", 29})

    t.assert_equals(result.metadata, {
        {name = 'id', type = 'unsigned'},
        {name = 'bucket_id', type = 'unsigned'},
        {name = 'name', type = 'string'},
        {name = 'age', type = 'number'},
    })

    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_items_equals(objects, {
        {id = 5, name = 'Sergey', age = 25, bucket_id = 1172},
        {id = 22, name = 'Alex', age = 34, bucket_id = 655},
    })

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

    -- insert_many
    -- fails for both: s1-master s2-master
    -- two errors on each storage with rollback
    local result, errs = g.router:call('crud.insert_many', {
        'customers',
        {
            {1, box.NULL, 'Olga', 27},
            {92, box.NULL, 'Oleg', 32},
            {71, box.NULL, 'Sergey', 25},
            {5, box.NULL, 'Anna', 30},
            {2, box.NULL, 'Alex', 34},
            {3, box.NULL, 'Anastasia', 22},
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

    t.assert_str_contains(errs[2].err, 'Duplicate key exists')
    t.assert_equals(errs[2].operation_data, {2, 401, 'Alex', 34})

    t.assert_str_contains(errs[3].err, 'Duplicate key exists')
    t.assert_equals(errs[3].operation_data, {3, 2804, 'Anastasia', 22})

    t.assert_str_contains(errs[4].err, 'Duplicate key exists')
    t.assert_equals(errs[4].operation_data, {5, 1172, "Anna", 30})

    t.assert_str_contains(errs[5].err, 'Duplicate key exists')
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

    -- insert_object_many
    -- fails for both: s1-master s2-master
    -- two errors on each storage with rollback
    local result, errs = g.router:call('crud.insert_object_many', {
        'customers',
        {
            {id = 1, name = 'Olga', age = 27},
            {id = 92, name = 'Oleg', age = 32},
            {id = 71, name = 'Sergey', age = 25},
            {id = 5, name = 'Anna', age = 30},
            {id = 2, name = 'Alex', age = 34},
            {id = 3, name = 'Anastasia', age = 22},
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

    t.assert_str_contains(errs[2].err, 'Duplicate key exists')
    t.assert_equals(errs[2].operation_data, {2, 401, 'Alex', 34})

    t.assert_str_contains(errs[3].err, 'Duplicate key exists')
    t.assert_equals(errs[3].operation_data, {3, 2804, 'Anastasia', 22})

    t.assert_str_contains(errs[4].err, 'Duplicate key exists')
    t.assert_equals(errs[4].operation_data, {5, 1172, "Anna", 30})

    t.assert_str_contains(errs[5].err, 'Duplicate key exists')
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
    -- insert_many
    -- all success
    local result, errs = g.router:call('crud.insert_many', {
        'customers',
        {
            {2, box.NULL, 'Anna', 23},
            {3, box.NULL, 'Daria', 18},
            {71, box.NULL, 'Oleg', 32}
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

    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_items_equals(objects, {
        {id = 2, name = 'Anna', age = 23, bucket_id = 401},
        {id = 3, name = 'Daria', age = 18, bucket_id = 2804},
        {id = 71, name = "Oleg", age = 32, bucket_id = 1802}
    })

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
    -- insert_object_many
    -- all success
    local result, errs = g.router:call('crud.insert_object_many', {
        'customers',
        {
            {id = 2, name = 'Anna', age = 23},
            {id = 3, name = 'Daria', age = 18},
            {id = 71, name = 'Oleg', age = 32}
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

    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_items_equals(objects, {
        {id = 2, name = 'Anna', age = 23, bucket_id = 401},
        {id = 3, name = 'Daria', age = 18, bucket_id = 2804},
        {id = 71, name = "Oleg", age = 32, bucket_id = 1802}
    })

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

    -- insert_many
    -- stop_on_error = true, rollback_on_error = true
    -- two error on one storage with rollback, inserts stop by error on this storage
    -- inserts before error are rollbacked
    local result, errs = g.router:call('crud.insert_many', {
        'customers',
        {
            {22, box.NULL, 'Alex', 34},
            {92, box.NULL, 'Artur', 29},
            {3, box.NULL, 'Anastasia', 22},
            {5, box.NULL, 'Sergey', 25},
            {9, box.NULL, 'Anna', 30},
            {71, box.NULL, 'Oksana', 29},
        },
        {
            stop_on_error = true,
            rollback_on_error = true,
        }
    })

    t.assert_not_equals(errs, nil)
    t.assert_equals(#errs, 4)

    table.sort(errs, function(err1, err2) return err1.operation_data[1] < err2.operation_data[1] end)

    t.assert_str_contains(errs[1].err, 'Duplicate key exists')
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

    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_items_equals(objects, {
        {id = 5, name = 'Sergey', age = 25, bucket_id = 1172},
        {id = 22, name = 'Alex', age = 34, bucket_id = 655},
    })

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

    -- insert_object_many
    -- stop_on_error = true, rollback_on_error = true
    -- two error on one storage with rollback, inserts stop by error on this storage
    -- inserts before error are rollbacked
    local result, errs = g.router:call('crud.insert_object_many', {
        'customers',
        {
            {id = 22, name = 'Alex', age = 34},
            {id = 92, name = 'Artur', age = 29},
            {id = 3, name = 'Anastasia', age = 22},
            {id = 5, name = 'Sergey', age = 25},
            {id = 9, name = 'Anna', age = 30},
            {id = 71, name = 'Oksana', age = 29},
        },
        {
            stop_on_error = true,
            rollback_on_error  = true,
        }
    })

    t.assert_not_equals(errs, nil)
    t.assert_equals(#errs, 4)

    table.sort(errs, function(err1, err2) return err1.operation_data[1] < err2.operation_data[1] end)

    t.assert_str_contains(errs[1].err, 'Duplicate key exists')
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

    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_items_equals(objects, {
        {id = 5, name = 'Sergey', age = 25, bucket_id = 1172},
        {id = 22, name = 'Alex', age = 34, bucket_id = 655},
    })

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
    local result, errs = g.router:call('crud.insert_many', {
        'customers',
        {
            {15, box.NULL, 'Fedor', 59},
            {25, box.NULL, 'Anna', 23},
        },
        {fields = {'id', 'invalid'}},
    })

    t.assert_equals(result, nil)
    t.assert_not_equals(errs, nil)
    t.assert_equals(#errs, 1)
    t.assert_str_contains(errs[1].err, 'Space format doesn\'t contain field named "invalid"')

    -- insert_many
    local result, errs = g.router:call('crud.insert_many', {
        'customers',
        {
            {1, box.NULL, 'Fedor', 59},
            {2, box.NULL, 'Anna', 23},
            {3, box.NULL, 'Daria', 18}
        },
        {fields = {'id', 'name'}},
    })

    t.assert_equals(errs, nil)
    t.assert_equals(result.metadata, {
        {name = 'id', type = 'unsigned'},
        {name = 'name', type = 'string'},
    })

    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_items_equals(objects, {{id = 1, name = 'Fedor'}, {id = 2, name = 'Anna'}, {id = 3, name = 'Daria'}})
end

pgroup.test_object_partial_result = function(g)
    -- bad fields format
    local result, errs = g.router:call('crud.insert_object_many', {
        'customers',
        {
            {id = 15, name = 'Fedor', age = 59},
            {id = 25, name = 'Anna', age = 23},
        },
        {fields = {'id', 'invalid'}},
    })

    t.assert_equals(result, nil)
    t.assert_not_equals(errs, nil)
    t.assert_equals(#errs, 1)
    t.assert_str_contains(errs[1].err, 'Space format doesn\'t contain field named "invalid"')

    -- insert_object_many
    local result, errs = g.router:call('crud.insert_object_many', {
        'customers',
        {
            {id = 1, name = 'Fedor', age = 59},
            {id = 2, name = 'Anna', age = 23},
            {id = 3, name = 'Daria', age = 18}
        },
        {fields = {'id', 'name'}},
    })

    t.assert_equals(errs, nil)
    t.assert_equals(result.metadata, {
        {name = 'id', type = 'unsigned'},
        {name = 'name', type = 'string'},
    })

    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_items_equals(objects, {{id = 1, name = 'Fedor'}, {id = 2, name = 'Anna'}, {id = 3, name = 'Daria'}})
end

pgroup.test_opts_not_damaged = function(g)
    -- insert_many
    local batch_insert_opts = {timeout = 1, fields = {'name', 'age'}}
    local new_batch_insert_opts, err = g.router:eval([[
        local crud = require('crud')

        local batch_insert_opts = ...

        local _, err = crud.insert_many('customers', {
            {1, box.NULL, 'Alex', 59}
        }, batch_insert_opts)

        return batch_insert_opts, err
    ]], {batch_insert_opts})

    t.assert_equals(err, nil)
    t.assert_equals(new_batch_insert_opts, batch_insert_opts)

    -- insert_object_many
    local batch_insert_opts = {timeout = 1, fields = {'name', 'age'}}
    local new_batch_insert_opts, err = g.router:eval([[
        local crud = require('crud')

        local batch_insert_opts = ...

        local _, err = crud.insert_object_many('customers', {
            {id = 2, name = 'Fedor', age = 59}
        }, batch_insert_opts)

        return batch_insert_opts, err
    ]], {batch_insert_opts})

    t.assert_equals(err, nil)
    t.assert_equals(new_batch_insert_opts, batch_insert_opts)
end

pgroup.test_noreturn_opt = function(g)
    -- insert_many with noreturn, all tuples are correct
    local result, errs = g.router:call('crud.insert_many', {
        'customers',
        {
            {1, box.NULL, 'Fedor', 59},
            {2, box.NULL, 'Anna', 23},
            {3, box.NULL, 'Daria', 18}
        },
        {noreturn = true},
    })

    t.assert_equals(errs, nil)
    t.assert_equals(result, nil)

    -- insert_many with noreturn, some tuples are correct
    local result, errs = g.router:call('crud.insert_many', {
        'customers',
        {
            {1, box.NULL, 'Fedor', 59},
            {4, box.NULL, 'Rom', 23},
            {5, box.NULL, 'Max', 18}
        },
        {noreturn = true},
    })

    t.assert_not_equals(errs, nil)
    t.assert_equals(#errs, 1)
    t.assert_equals(result, nil)

    -- insert_many with noreturn, all tuples are not correct
    local result, errs = g.router:call('crud.insert_many', {
        'customers',
        {
            {1, box.NULL, 'Fedor', 59},
            {2, box.NULL, 'Anna', 23},
            {3, box.NULL, 'Daria', 18}
        },
        {noreturn = true},
    })

    t.assert_not_equals(errs, nil)
    t.assert_equals(#errs, 3)
    t.assert_equals(result, nil)

    -- insert_object_many with noreturn, all tuples are correct
    local result, errs = g.router:call('crud.insert_object_many', {
        'customers',
        {
            {id = 10, name = 'Fedor', age = 59},
            {id = 20, name = 'Anna', age = 23},
            {id = 30, name = 'Daria', age = 18}
        },
        {noreturn = true},
    })

    t.assert_equals(errs, nil)
    t.assert_equals(result, nil)

    -- insert_object_many with noreturn, some tuples are correct
    local result, errs = g.router:call('crud.insert_object_many', {
        'customers',
        {
            {id = 40, name = 'Fedor', age = 59},
            {id = box.NULL, name = 'Anna', age = 23},
            {id = box.NULL, name = 'Daria', age = 18}
        },
        {noreturn = true},
    })

    t.assert_not_equals(errs, nil)
    t.assert_equals(#errs, 2)
    t.assert_equals(result, nil)

    -- insert_object_many with noreturn, all tuples are not correct
    local result, errs = g.router:call('crud.insert_object_many', {
        'customers',
        {
            {id = box.NULL, name = 'Fedor', age = 59},
            {id = box.NULL, name = 'Anna', age = 23},
            {id = box.NULL, name = 'Daria', age = 18}
        },
        {noreturn = true},
    })

    t.assert_not_equals(errs, nil)
    t.assert_equals(#errs, 3)
    t.assert_equals(result, nil)
end

pgroup.test_zero_tuples = function(g)
    local result, errs = g.router:call(
        'crud.insert_many', {'customers', {}})

    t.assert_not_equals(errs, nil)
    t.assert_equals(#errs, 1)
    t.assert_str_contains(errs[1].err, "At least one tuple expected")
    t.assert_equals(result, nil)
end

pgroup.test_zero_objects = function(g)
    local result, errs = g.router:call(
        'crud.insert_object_many', {'customers', {}})

    t.assert_not_equals(errs, nil)
    t.assert_equals(#errs, 1)
    t.assert_str_contains(errs[1].err, "At least one object expected")
    t.assert_equals(result, nil)
end
