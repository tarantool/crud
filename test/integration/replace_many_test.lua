local t = require('luatest')

local crud = require('crud')

local helpers = require('test.helper')

local batching_utils = require('crud.common.batching_utils')

local pgroup = t.group('replace_many', helpers.backend_matrix({
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
    helpers.truncate_space_on_cluster(g.cluster, 'developers')
end)

pgroup.test_non_existent_space = function(g)
    -- replace_many
    local result, errs = g.cluster.main_server.net_box:call('crud.replace_many', {
        'non_existent_space',
        {
            {1, box.NULL, 'Alex', 'alexpushkin'},
            {2, box.NULL, 'Anna', 'AnnaKar'},
            {3, box.NULL, 'Daria', 'mongendor'}
        }
    })

    t.assert_equals(result, nil)
    t.assert_not_equals(errs, nil)
    t.assert_equals(#errs, 1)
    t.assert_str_contains(errs[1].err, 'Space "non_existent_space" doesn\'t exist')

    -- replace_object_many
    -- default: stop_on_error == false
    local result, errs = g.cluster.main_server.net_box:call('crud.replace_object_many', {
        'non_existent_space',
        {
            {id = 1, name = 'Fedor', login = 'FDost'},
            {id = 2, name = 'Anna', login = 'AnnaKar'},
            {id = 3, name = 'Daria', login = 'mongendor'}
        }
    })

    t.assert_equals(result, nil)
    t.assert_not_equals(errs, nil)

    -- we got 3 errors about non existent space, because it caused by flattening objects
    t.assert_equals(#errs, 3)
    t.assert_str_contains(errs[1].err, 'Space "non_existent_space" doesn\'t exist')
    t.assert_str_contains(errs[2].err, 'Space "non_existent_space" doesn\'t exist')
    t.assert_str_contains(errs[3].err, 'Space "non_existent_space" doesn\'t exist')

    -- replace_object_many
    -- stop_on_error == true
    local result, errs = g.cluster.main_server.net_box:call('crud.replace_object_many', {
        'non_existent_space',
        {
            {id = 1, name = 'Fedor', login = 'FDost'},
            {id = 2, name = 'Anna', login = 'AnnaKar'},
            {id = 3, name = 'Daria', login = 'mongendor'}
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
    local result, errs = g.cluster.main_server.net_box:call('crud.replace_object_many', {
        'developers',
        {
            {id = 1, name = 'Fedor', login = 'FDost'},
            {id = 2, name = 'Anna'},
        }
    })

    t.assert_not_equals(errs, nil)
    t.assert_equals(#errs, 1)
    t.assert_str_contains(errs[1].err, 'Field \"login\" isn\'t nullable')
    t.assert_equals(errs[1].operation_data, {id = 2, name = 'Anna'})

    t.assert_equals(result.metadata, {
        {name = 'id', type = 'unsigned'},
        {name = 'bucket_id', type = 'unsigned'},
        {name = 'name', type = 'string'},
        {name = 'login', type = 'string'},
    })

    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_items_equals(objects, {
        {id = 1, name = 'Fedor', login = 'FDost', bucket_id = 477},
    })

    -- get
    -- primary key = 1 -> bucket_id = 477 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['developers']:get(1)
    t.assert_equals(result, {1, 477, 'Fedor', 'FDost'})

    -- bad format
    local result, errs = g.cluster.main_server.net_box:call('crud.replace_object_many', {
        'developers',
        {
            {id = 4, name = 'Fedor', login = 'FDost'},
            {id = 2, name = 'Anna'},
        }
    })

    t.assert_equals(result, nil)
    t.assert_not_equals(errs, nil)
    t.assert_equals(#errs, 2)

    t.assert_str_contains(errs[1].err, 'Duplicate key exists')
    t.assert_equals(errs[1].operation_data, {4, 1161, "Fedor", "FDost"})

    t.assert_str_contains(errs[2].err, 'Field \"login\" isn\'t nullable')
    t.assert_equals(errs[2].operation_data, {id = 2, name = 'Anna'})

    -- get
    -- primary key = 4 -> bucket_id = 1161 -> s1-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['developers']:get(4)
    t.assert_equals(result, nil)

    -- bad format
    -- two errors, default: stop_on_error == false
    local result, errs = g.cluster.main_server.net_box:call('crud.replace_object_many', {
        'developers',
        {
            {id = 1, name = 'Fedor'},
            {id = 2, name = 'Anna'},
        }
    })

    t.assert_equals(result, nil)
    t.assert_not_equals(errs, nil)
    t.assert_equals(#errs, 2)

    table.sort(errs, function(err1, err2) return err1.operation_data.id < err2.operation_data.id end)

    t.assert_str_contains(errs[1].err, 'Field \"login\" isn\'t nullable')
    t.assert_equals(errs[1].operation_data, {id = 1, name = 'Fedor'})

    t.assert_str_contains(errs[2].err, 'Field \"login\" isn\'t nullable')
    t.assert_equals(errs[2].operation_data, {id = 2, name = 'Anna'})
end

pgroup.test_all_success = function(g)
    -- replace_many
    -- all success
    local result, errs = g.cluster.main_server.net_box:call('crud.replace_many', {
        'developers',
        {
            {1, box.NULL, 'Fedor', 'FDost'},
            {2, box.NULL, 'Anna', 'AnnaKar'},
            {3, box.NULL, 'Daria', 'mongend'}
        }
    })

    t.assert_equals(errs, nil)
    t.assert_equals(result.metadata, {
        {name = 'id', type = 'unsigned'},
        {name = 'bucket_id', type = 'unsigned'},
        {name = 'name', type = 'string'},
        {name = 'login', type = 'string'},
    })

    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_items_equals(objects, {
        {id = 1, name = 'Fedor', login = 'FDost', bucket_id = 477},
        {id = 2, name = 'Anna', login = 'AnnaKar', bucket_id = 401},
        {id = 3, name = 'Daria', login = 'mongend', bucket_id = 2804},
    })

    -- get
    -- primary key = 1 -> bucket_id = 477 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['developers']:get(1)
    t.assert_equals(result, {1, 477, 'Fedor', 'FDost'})

    -- primary key = 2 -> bucket_id = 401 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['developers']:get(2)
    t.assert_equals(result, {2, 401, 'Anna', 'AnnaKar'})

    -- primary key = 3 -> bucket_id = 2804 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['developers']:get(3)
    t.assert_equals(result, {3, 2804, 'Daria', 'mongend'})
end

pgroup.test_object_all_success = function(g)
    -- insert
    -- primary key = 1 -> bucket_id = 477 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['developers']:insert({1, 477, 'Alex', 'alexpushkin'})
    t.assert_equals(result, {1, 477, 'Alex', 'alexpushkin'})

    -- replace_object_many
    -- all success
    local result, errs = g.cluster.main_server.net_box:call('crud.replace_object_many', {
        'developers',
        {
            {id = 1, name = 'Fedor', login = 'FDost'},
            {id = 2, name = 'Anna', login = 'AnnaKar'},
            {id = 3, name = 'Daria', login = 'mongend'}
        },
    })

    t.assert_equals(errs, nil)
    t.assert_equals(result.metadata, {
        {name = 'id', type = 'unsigned'},
        {name = 'bucket_id', type = 'unsigned'},
        {name = 'name', type = 'string'},
        {name = 'login', type = 'string'},
    })

    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_items_equals(objects, {
        {id = 1, name = 'Fedor', login = 'FDost', bucket_id = 477},
        {id = 2, name = 'Anna', login = 'AnnaKar', bucket_id = 401},
        {id = 3, name = 'Daria', login = 'mongend', bucket_id = 2804},
    })

    -- get
    -- primary key = 1 -> bucket_id = 477 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['developers']:get(1)
    t.assert_equals(result, {1, 477, 'Fedor', 'FDost'})

    -- primary key = 2 -> bucket_id = 401 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['developers']:get(2)
    t.assert_equals(result, {2, 401, 'Anna', 'AnnaKar'})

    -- primary key = 3 -> bucket_id = 2804 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['developers']:get(3)
    t.assert_equals(result, {3, 2804, 'Daria', 'mongend'})
end

pgroup.test_one_error = function(g)
    -- insert
    -- primary key = 1 -> bucket_id = 477 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['developers']:insert({1, 477, 'Alex', 'alexpushkin'})
    t.assert_equals(result, {1, 477, 'Alex', 'alexpushkin'})

    -- replace_many
    -- failed for s1-master
    local result, errs = g.cluster.main_server.net_box:call('crud.replace_many', {
        'developers',
        {
            {4, box.NULL, 'Fedor', 'alexpushkin'},
            {2, box.NULL, 'Anna', 'AnnaKar'},
            {3, box.NULL, 'Daria', 'mongend'}
        },
    })

    t.assert_not_equals(errs, nil)
    t.assert_equals(#errs, 1)
    t.assert_str_contains(errs[1].err, 'Duplicate key exists')
    t.assert_equals(errs[1].operation_data, {4, 1161, "Fedor", "alexpushkin"})

    t.assert_equals(result.metadata, {
        {name = 'id', type = 'unsigned'},
        {name = 'bucket_id', type = 'unsigned'},
        {name = 'name', type = 'string'},
        {name = 'login', type = 'string'},
    })

    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_items_equals(objects, {
        {id = 2, name = 'Anna', login = 'AnnaKar', bucket_id = 401},
        {id = 3, name = 'Daria', login = 'mongend', bucket_id = 2804},
    })

    -- get
    -- primary key = 1 -> bucket_id = 477 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['developers']:get(4)
    t.assert_equals(result, nil)

    -- primary key = 2 -> bucket_id = 401 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['developers']:get(2)
    t.assert_equals(result, {2, 401, 'Anna', 'AnnaKar'})

    -- primary key = 3 -> bucket_id = 2804 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['developers']:get(3)
    t.assert_equals(result, {3, 2804, 'Daria', 'mongend'})
end

pgroup.test_object_one_error = function(g)
    -- insert
    -- primary key = 1 -> bucket_id = 477 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['developers']:insert({1, 477, 'Alex', 'alexpushkin'})
    t.assert_equals(result, {1, 477, 'Alex', 'alexpushkin'})

    -- replace_object_many
    -- failed for s1-master
    local result, errs = g.cluster.main_server.net_box:call('crud.replace_object_many', {
        'developers',
        {
            {id = 4, name = 'Fedor', login = 'alexpushkin'},
            {id = 2, name = 'Anna', login = 'AnnaKar'},
            {id = 3, name = 'Daria', login = 'mongend'}
        },
    })

    t.assert_not_equals(errs, nil)
    t.assert_equals(#errs, 1)
    t.assert_str_contains(errs[1].err, 'Duplicate key exists')
    t.assert_equals(errs[1].operation_data, {4, 1161, "Fedor", "alexpushkin"})

    t.assert_equals(result.metadata, {
        {name = 'id', type = 'unsigned'},
        {name = 'bucket_id', type = 'unsigned'},
        {name = 'name', type = 'string'},
        {name = 'login', type = 'string'},
    })

    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_items_equals(objects, {
        {id = 2, name = 'Anna', login = 'AnnaKar', bucket_id = 401},
        {id = 3, name = 'Daria', login = 'mongend', bucket_id = 2804},
    })

    -- get
    -- primary key = 1 -> bucket_id = 477 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['developers']:get(4)
    t.assert_equals(result, nil)

    -- primary key = 2 -> bucket_id = 401 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['developers']:get(2)
    t.assert_equals(result, {2, 401, 'Anna', 'AnnaKar'})

    -- primary key = 3 -> bucket_id = 2804 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['developers']:get(3)
    t.assert_equals(result, {3, 2804, 'Daria', 'mongend'})
end

pgroup.test_object_many_errors = function(g)
    -- insert
    -- primary key = 2 -> bucket_id = 401 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['developers']:insert({2, 401, 'Anna', 'a.petrova'})
    t.assert_equals(result, {2, 401, 'Anna', 'a.petrova'})

    -- primary key = 3 -> bucket_id = 2804 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['developers']:insert({3, 2804, 'Daria', 'd.petrenko'})
    t.assert_equals(result, {3, 2804, 'Daria', 'd.petrenko'})

    -- replace_object_many
    -- fails for both: s1-master s2-master
    -- one error on each storage, one success on each storage
    local result, errs = g.cluster.main_server.net_box:call('crud.replace_object_many', {
        'developers',
        {
            {id = 4, name = 'Sergey', login = 's.ivanov'},
            {id = 71, name = 'Artur', login = 'a.orlov'},
            {id = 10, name = 'Anastasia', login = 'a.petrova'},
            {id = 92, name = 'Dmitriy', login = 'd.petrenko'},
        }
    })

    t.assert_not_equals(errs, nil)
    t.assert_equals(#errs, 2)

    table.sort(errs, function(err1, err2) return err1.operation_data[1] < err2.operation_data[1] end)

    t.assert_str_contains(errs[1].err, 'Duplicate key exists')
    t.assert_equals(errs[1].operation_data, {10, 569, "Anastasia", "a.petrova"})

    t.assert_str_contains(errs[2].err, 'Duplicate key exists')
    t.assert_equals(errs[2].operation_data, {92, 2040, "Dmitriy", "d.petrenko"})

    t.assert_equals(result.metadata, {
        {name = 'id', type = 'unsigned'},
        {name = 'bucket_id', type = 'unsigned'},
        {name = 'name', type = 'string'},
        {name = 'login', type = 'string'},
    })

    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_items_equals(objects, {
        {id = 4, login = "s.ivanov", bucket_id = 1161, name = "Sergey"},
        {id = 71, login = "a.orlov", bucket_id = 1802, name = "Artur"},
    })

    -- primary key = 4 -> bucket_id = 1161 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['developers']:get(4)
    t.assert_equals(result, {4, 1161, 'Sergey', 's.ivanov'})

    -- primary key = 71 -> bucket_id = 1802 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['developers']:get(71)
    t.assert_equals(result, {71, 1802, 'Artur', 'a.orlov'})

    -- primary key = 10 -> bucket_id = 569 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['developers']:get(10)
    t.assert_equals(result, nil)

    -- primary key = 92 -> bucket_id = 2040 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['developers']:get(92)
    t.assert_equals(result, nil)
end

pgroup.test_many_errors = function(g)
    -- insert
    -- primary key = 2 -> bucket_id = 401 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['developers']:insert({2, 401, 'Anna', 'a.petrova'})
    t.assert_equals(result, {2, 401, 'Anna', 'a.petrova'})

    -- primary key = 3 -> bucket_id = 2804 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['developers']:insert({3, 2804, 'Daria', 'd.petrenko'})
    t.assert_equals(result, {3, 2804, 'Daria', 'd.petrenko'})

    -- replace_many
    -- fails for both: s1-master s2-master
    -- one error on each storage, one success on each storage
    local result, errs = g.cluster.main_server.net_box:call('crud.replace_many', {
        'developers',
        {
            {4, box.NULL, 'Sergey', 's.ivanov'},
            {71, box.NULL, 'Artur', 'a.orlov'},
            {10, box.NULL, 'Anastasia', 'a.petrova'},
            {92, box.NULL, 'Dmitriy', 'd.petrenko'},
        }
    })

    t.assert_not_equals(errs, nil)
    t.assert_equals(#errs, 2)

    table.sort(errs, function(err1, err2) return err1.operation_data[1] < err2.operation_data[1] end)

    t.assert_str_contains(errs[1].err, 'Duplicate key exists')
    t.assert_equals(errs[1].operation_data, {10, 569, "Anastasia", "a.petrova"})

    t.assert_str_contains(errs[2].err, 'Duplicate key exists')
    t.assert_equals(errs[2].operation_data, {92, 2040, "Dmitriy", "d.petrenko"})

    t.assert_equals(result.metadata, {
        {name = 'id', type = 'unsigned'},
        {name = 'bucket_id', type = 'unsigned'},
        {name = 'name', type = 'string'},
        {name = 'login', type = 'string'},
    })

    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_items_equals(objects, {
        {id = 4, login = "s.ivanov", bucket_id = 1161, name = "Sergey"},
        {id = 71, login = "a.orlov", bucket_id = 1802, name = "Artur"},
    })

    -- primary key = 4 -> bucket_id = 1161 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['developers']:get(4)
    t.assert_equals(result, {4, 1161, 'Sergey', 's.ivanov'})

    -- primary key = 71 -> bucket_id = 1802 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['developers']:get(71)
    t.assert_equals(result, {71, 1802, 'Artur', 'a.orlov'})

    -- primary key = 10 -> bucket_id = 569 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['developers']:get(10)
    t.assert_equals(result, nil)

    -- primary key = 92 -> bucket_id = 2040 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['developers']:get(92)
    t.assert_equals(result, nil)
end

pgroup.test_no_success = function(g)
    -- insert
    -- primary key = 2 -> bucket_id = 401 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['developers']:insert({2, 401, 'Anna', 'a.smith'})
    t.assert_equals(result, {2, 401, 'Anna', 'a.smith'})

    -- primary key = 3 -> bucket_id = 2804 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['developers']:insert({3, 2804, 'Daria', 'd.petrenko'})
    t.assert_equals(result, {3, 2804, 'Daria', 'd.petrenko'})

    -- primary key = 10 -> bucket_id = 569 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['developers']:insert({10, 569, 'Sergey', 's.serenko'})
    t.assert_equals(result, {10, 569, 'Sergey', 's.serenko'})

    -- primary key = 92 -> bucket_id = 2040 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['developers']:insert({92, 2040, 'Artur', 'a.smirnov'})
    t.assert_equals(result, {92, 2040, 'Artur', 'a.smirnov'})

    -- replace_many
    -- fails for both: s1-master s2-master
    -- no success
    local result, errs = g.cluster.main_server.net_box:call('crud.replace_many', {
        'developers',
        {
            {4, box.NULL, 'Alex', 'a.smith'},
            {71, box.NULL, 'Dmitriy', 'd.petrenko'},
            {6, box.NULL, 'Semen', 's.serenko'},
            {9, box.NULL, 'Anton', 'a.smirnov'},
        }
    })

    t.assert_equals(result, nil)
    t.assert_not_equals(errs, nil)
    t.assert_equals(#errs, 4)

    table.sort(errs, function(err1, err2) return err1.operation_data[1] < err2.operation_data[1] end)

    t.assert_str_contains(errs[1].err, 'Duplicate key exists')
    t.assert_equals(errs[1].operation_data, {4, 1161, "Alex", "a.smith"})

    t.assert_str_contains(errs[2].err, 'Duplicate key exists')
    t.assert_equals(errs[2].operation_data, {6, 1064, "Semen", "s.serenko"})

    t.assert_str_contains(errs[3].err, 'Duplicate key exists')
    t.assert_equals(errs[3].operation_data, {9, 1644, "Anton", "a.smirnov"})

    t.assert_str_contains(errs[4].err, 'Duplicate key exists')
    t.assert_equals(errs[4].operation_data, {71, 1802, "Dmitriy", "d.petrenko"})

    -- primary key = 4 -> bucket_id = 1161 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['developers']:get(4)
    t.assert_equals(result, nil)

    -- primary key = 9 -> bucket_id = 1644 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['developers']:get(9)
    t.assert_equals(result, nil)

    -- primary key = 6 -> bucket_id = 1064 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['developers']:get(6)
    t.assert_equals(result, nil)

    -- primary key = 71 -> bucket_id = 1802 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['developers']:get(71)
    t.assert_equals(result, nil)
end

pgroup.test_object_no_success = function(g)
    -- insert
    -- primary key = 2 -> bucket_id = 401 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['developers']:insert({2, 401, 'Anna', 'a.smith'})
    t.assert_equals(result, {2, 401, 'Anna', 'a.smith'})

    -- primary key = 3 -> bucket_id = 2804 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['developers']:insert({3, 2804, 'Daria', 'd.petrenko'})
    t.assert_equals(result, {3, 2804, 'Daria', 'd.petrenko'})

    -- primary key = 10 -> bucket_id = 569 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['developers']:insert({10, 569, 'Sergey', 's.serenko'})
    t.assert_equals(result, {10, 569, 'Sergey', 's.serenko'})

    -- primary key = 92 -> bucket_id = 2040 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['developers']:insert({92, 2040, 'Artur', 'a.smirnov'})
    t.assert_equals(result, {92, 2040, 'Artur', 'a.smirnov'})

    -- replace_object_many
    -- fails for both: s1-master s2-master
    -- no success
    local result, errs = g.cluster.main_server.net_box:call('crud.replace_object_many', {
        'developers',
        {
            {id = 4, name = 'Alex', login = 'a.smith'},
            {id = 71, name = 'Dmitriy', login = 'd.petrenko'},
            {id = 6, name = 'Semen', login = 's.serenko'},
            {id = 9, name = 'Anton', login = 'a.smirnov'},
        }
    })

    t.assert_equals(result, nil)
    t.assert_not_equals(errs, nil)
    t.assert_equals(#errs, 4)

    table.sort(errs, function(err1, err2) return err1.operation_data[1] < err2.operation_data[1] end)

    t.assert_str_contains(errs[1].err, 'Duplicate key exists')
    t.assert_equals(errs[1].operation_data, {4, 1161, "Alex", "a.smith"})

    t.assert_str_contains(errs[2].err, 'Duplicate key exists')
    t.assert_equals(errs[2].operation_data, {6, 1064, "Semen", "s.serenko"})

    t.assert_str_contains(errs[3].err, 'Duplicate key exists')
    t.assert_equals(errs[3].operation_data, {9, 1644, "Anton", "a.smirnov"})

    t.assert_str_contains(errs[4].err, 'Duplicate key exists')
    t.assert_equals(errs[4].operation_data, {71, 1802, "Dmitriy", "d.petrenko"})

    -- primary key = 4 -> bucket_id = 1161 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['developers']:get(4)
    t.assert_equals(result, nil)

    -- primary key = 9 -> bucket_id = 1644 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['developers']:get(9)
    t.assert_equals(result, nil)

    -- primary key = 6 -> bucket_id = 1064 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['developers']:get(6)
    t.assert_equals(result, nil)

    -- primary key = 71 -> bucket_id = 1802 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['developers']:get(71)
    t.assert_equals(result, nil)
end

pgroup.test_object_bad_format_stop_on_error = function(g)
    -- bad format
    -- two errors, stop_on_error == true
    local result, errs = g.cluster.main_server.net_box:call('crud.replace_object_many', {
        'developers',
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

    t.assert_str_contains(errs[1].err, 'Field \"login\" isn\'t nullable')
    t.assert_equals(errs[1].operation_data, {id = 1, name = 'Fedor'})
end

pgroup.test_all_success_stop_on_error = function(g)
    -- replace_many
    -- all success
    local result, errs = g.cluster.main_server.net_box:call('crud.replace_many', {
        'developers',
        {
            {1, box.NULL, 'Fedor', 'FDost'},
            {2, box.NULL, 'Anna', 'AnnaKar'},
            {3, box.NULL, 'Daria', 'mongend'}
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
        {name = 'login', type = 'string'},
    })

    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_items_equals(objects, {
        {id = 1, name = 'Fedor', login = 'FDost', bucket_id = 477},
        {id = 2, name = 'Anna', login = 'AnnaKar', bucket_id = 401},
        {id = 3, name = 'Daria', login = 'mongend', bucket_id = 2804},
    })

    -- get
    -- primary key = 1 -> bucket_id = 477 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['developers']:get(1)
    t.assert_equals(result, {1, 477, 'Fedor', 'FDost'})

    -- primary key = 2 -> bucket_id = 401 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['developers']:get(2)
    t.assert_equals(result, {2, 401, 'Anna', 'AnnaKar'})

    -- primary key = 3 -> bucket_id = 2804 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['developers']:get(3)
    t.assert_equals(result, {3, 2804, 'Daria', 'mongend'})
end

pgroup.test_object_all_success_stop_on_error = function(g)
    -- insert
    -- primary key = 1 -> bucket_id = 477 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['developers']:insert({1, 477, 'Alex', 'alexpushkin'})
    t.assert_equals(result, {1, 477, 'Alex', 'alexpushkin'})

    -- replace_object_many
    -- all success
    local result, errs = g.cluster.main_server.net_box:call('crud.replace_object_many', {
        'developers',
        {
            {id = 1, name = 'Fedor', login = 'FDost'},
            {id = 2, name = 'Anna', login = 'AnnaKar'},
            {id = 3, name = 'Daria', login = 'mongend'}
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
        {name = 'login', type = 'string'},
    })

    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_items_equals(objects, {
        {id = 1, name = 'Fedor', login = 'FDost', bucket_id = 477},
        {id = 2, name = 'Anna', login = 'AnnaKar', bucket_id = 401},
        {id = 3, name = 'Daria', login = 'mongend', bucket_id = 2804},
    })

    -- get
    -- primary key = 1 -> bucket_id = 477 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['developers']:get(1)
    t.assert_equals(result, {1, 477, 'Fedor', 'FDost'})

    -- primary key = 2 -> bucket_id = 401 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['developers']:get(2)
    t.assert_equals(result, {2, 401, 'Anna', 'AnnaKar'})

    -- primary key = 3 -> bucket_id = 2804 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['developers']:get(3)
    t.assert_equals(result, {3, 2804, 'Daria', 'mongend'})
end

pgroup.test_partial_success_stop_on_error = function(g)
    -- insert
    -- primary key = 3 -> bucket_id = 2804 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['developers']:insert({3, 2804, 'Daria', 'mongend'})
    t.assert_equals(result, {3, 2804, 'Daria', 'mongend'})

    -- primary key = 9 -> bucket_id = 1644 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['developers']:insert({9, 1644, 'Eren', 'e.smith'})
    t.assert_equals(result, {9, 1644, 'Eren', 'e.smith'})

    -- replace_many
    -- stop_on_error = true, rollback_on_error = false
    -- one error on one storage without rollback, inserts stop by error on this storage
    -- inserts before error are successful
    local result, errs = g.cluster.main_server.net_box:call('crud.replace_many', {
        'developers',
        {
            {22, box.NULL, 'Alex', 'alexpushkin'},
            {92, box.NULL, 'Artur', 'AGolden'},
            {71, box.NULL, 'Erwin', 'e.smith'},
            {5, box.NULL, 'Sergey', 's.petrenko'},
            {11, box.NULL, 'Anna', 'mongend'},
        },
        {
            stop_on_error = true,
        }
    })

    t.assert_not_equals(errs, nil)
    t.assert_equals(#errs, 2)

    table.sort(errs, function(err1, err2) return err1.operation_data[1] < err2.operation_data[1] end)

    t.assert_str_contains(errs[1].err, batching_utils.stop_on_error_msg)
    t.assert_equals(errs[1].operation_data, {11, 2652, "Anna", "mongend"})

    t.assert_str_contains(errs[2].err, 'Duplicate key exists')
    t.assert_equals(errs[2].operation_data, {71, 1802, "Erwin", "e.smith"})

    t.assert_equals(result.metadata, {
        {name = 'id', type = 'unsigned'},
        {name = 'bucket_id', type = 'unsigned'},
        {name = 'name', type = 'string'},
        {name = 'login', type = 'string'},
    })

    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_items_equals(objects, {
        {id = 22, bucket_id = 655, login = "alexpushkin", name = "Alex"},
        {id = 5, bucket_id = 1172, login = "s.petrenko", name = "Sergey"},
        {id = 92, bucket_id = 2040, login = "AGolden", name = "Artur"},
    })

    -- get
    -- primary key = 22 -> bucket_id = 655 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['developers']:get(22)
    t.assert_equals(result, {22, 655, 'Alex', 'alexpushkin'})

    -- primary key = 92 -> bucket_id = 2040 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['developers']:get(92)
    t.assert_equals(result, {92, 2040, 'Artur', 'AGolden'})

    -- primary key = 5 -> bucket_id = 1172 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['developers']:get(5)
    t.assert_equals(result, {5, 1172, 'Sergey', 's.petrenko'})

    -- primary key = 71 -> bucket_id = 1802 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['developers']:get(71)
    t.assert_equals(result, nil)

    -- primary key = 11 -> bucket_id = 2652 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['developers']:get(11)
    t.assert_equals(result, nil)
end

pgroup.test_object_partial_success_stop_on_error = function(g)
    -- insert
    -- primary key = 3 -> bucket_id = 2804 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['developers']:insert({3, 2804, 'Daria', 'mongend'})
    t.assert_equals(result, {3, 2804, 'Daria', 'mongend'})

    -- primary key = 9 -> bucket_id = 1644 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['developers']:insert({9, 1644, 'Eren', 'e.smith'})
    t.assert_equals(result, {9, 1644, 'Eren', 'e.smith'})

    -- replace_object_many
    -- stop_on_error = true, rollback_on_error = false
    -- one error on one storage without rollback, inserts stop by error on this storage
    -- inserts before error are successful
    local result, errs = g.cluster.main_server.net_box:call('crud.replace_object_many', {
        'developers',
        {
            {id = 22, name = 'Alex', login = 'alexpushkin'},
            {id = 92, name = 'Artur', login = 'AGolden'},
            {id = 71, name = 'Erwin', login = 'e.smith'},
            {id = 5, name = 'Sergey', login = 's.petrenko'},
            {id = 11, name = 'Anna', login = 'mongend'},
        },
        {
            stop_on_error = true,
        }
    })

    t.assert_not_equals(errs, nil)
    t.assert_equals(#errs, 2)

    table.sort(errs, function(err1, err2) return err1.operation_data[1] < err2.operation_data[1] end)

    t.assert_str_contains(errs[1].err, batching_utils.stop_on_error_msg)
    t.assert_equals(errs[1].operation_data, {11, 2652, "Anna", "mongend"})

    t.assert_str_contains(errs[2].err, 'Duplicate key exists')
    t.assert_equals(errs[2].operation_data, {71, 1802, "Erwin", "e.smith"})

    t.assert_equals(result.metadata, {
        {name = 'id', type = 'unsigned'},
        {name = 'bucket_id', type = 'unsigned'},
        {name = 'name', type = 'string'},
        {name = 'login', type = 'string'},
    })

    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_items_equals(objects, {
        {id = 22, bucket_id = 655, login = "alexpushkin", name = "Alex"},
        {id = 5, bucket_id = 1172, login = "s.petrenko", name = "Sergey"},
        {id = 92, bucket_id = 2040, login = "AGolden", name = "Artur"},
    })

    -- get
    -- primary key = 22 -> bucket_id = 655 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['developers']:get(22)
    t.assert_equals(result, {22, 655, 'Alex', 'alexpushkin'})

    -- primary key = 92 -> bucket_id = 2040 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['developers']:get(92)
    t.assert_equals(result, {92, 2040, 'Artur', 'AGolden'})

    -- primary key = 5 -> bucket_id = 1172 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['developers']:get(5)
    t.assert_equals(result, {5, 1172, 'Sergey', 's.petrenko'})

    -- primary key = 71 -> bucket_id = 1802 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['developers']:get(71)
    t.assert_equals(result, nil)

    -- primary key = 11 -> bucket_id = 2652 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['developers']:get(11)
    t.assert_equals(result, nil)
end

pgroup.test_no_success_stop_on_error = function(g)
    -- insert
    -- primary key = 2 -> bucket_id = 401 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['developers']:insert({2, 401, 'Anna', 'AnnaKar'})
    t.assert_equals(result, {2, 401, 'Anna', 'AnnaKar'})

    -- primary key = 3 -> bucket_id = 2804 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['developers']:insert({3, 2804, 'Daria', 'mongend'})
    t.assert_equals(result, {3, 2804, 'Daria', 'mongend'})

    -- primary key = 92 -> bucket_id = 2040 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['developers']:insert({92, 2040, 'Artur', 'AGolden'})
    t.assert_equals(result, {92, 2040, 'Artur', 'AGolden'})

    -- replace_many
    -- fails for both: s1-master s2-master
    -- one error on each storage, all inserts stop by error
    local result, errs = g.cluster.main_server.net_box:call('crud.replace_many', {
        'developers',
        {
            {71, box.NULL, 'Alex', 'AGolden'},
            {4, box.NULL, 'Anastasia', 'AnnaKar'},
            {10, box.NULL, 'Sergey', 's.petrenko'},
            {9, box.NULL, 'Anna', 'a.smirnova'},
            {92, box.NULL, 'Leo', 'tolstoy_leo'},
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
    t.assert_equals(errs[1].operation_data, {4, 1161, "Anastasia", "AnnaKar"})

    t.assert_str_contains(errs[2].err, batching_utils.stop_on_error_msg)
    t.assert_equals(errs[2].operation_data, {9, 1644, "Anna", "a.smirnova"})

    t.assert_str_contains(errs[3].err, batching_utils.stop_on_error_msg)
    t.assert_equals(errs[3].operation_data, {10, 569, "Sergey", "s.petrenko"})

    t.assert_str_contains(errs[4].err, 'Duplicate key exists')
    t.assert_equals(errs[4].operation_data, {71, 1802, "Alex", "AGolden"})

    t.assert_str_contains(errs[5].err, batching_utils.stop_on_error_msg)
    t.assert_equals(errs[5].operation_data, {92, 2040, "Leo", "tolstoy_leo"})

    -- primary key = 4 -> bucket_id = 1161 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['developers']:get(4)
    t.assert_equals(result, nil)

    -- primary key = 71 -> bucket_id = 1802 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['developers']:get(71)
    t.assert_equals(result, nil)

    -- primary key = 10 -> bucket_id = 569 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['developers']:get(10)
    t.assert_equals(result, nil)

    -- primary key = 9 -> bucket_id = 1644 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['developers']:get(9)
    t.assert_equals(result, nil)

    -- primary key = 92 -> bucket_id = 2040 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['developers']:get(92)
    t.assert_equals(result, {92, 2040, 'Artur', 'AGolden'})
end

pgroup.test_object_no_success_stop_on_error = function(g)
    -- insert
    -- primary key = 2 -> bucket_id = 401 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['developers']:insert({2, 401, 'Anna', 'AnnaKar'})
    t.assert_equals(result, {2, 401, 'Anna', 'AnnaKar'})

    -- primary key = 3 -> bucket_id = 2804 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['developers']:insert({3, 2804, 'Daria', 'mongend'})
    t.assert_equals(result, {3, 2804, 'Daria', 'mongend'})

    -- primary key = 92 -> bucket_id = 2040 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['developers']:insert({92, 2040, 'Artur', 'AGolden'})
    t.assert_equals(result, {92, 2040, 'Artur', 'AGolden'})

    -- replace_object_many
    -- fails for both: s1-master s2-master
    -- one error on each storage, all inserts stop by error
    local result, errs = g.cluster.main_server.net_box:call('crud.replace_object_many', {
        'developers',
        {
            {id = 71, name = 'Alex', login = 'AGolden'},
            {id = 4, name = 'Anastasia', login = 'AnnaKar'},
            {id = 10, name = 'Sergey', login = 's.petrenko'},
            {id = 9, name = 'Anna', login = 'a.smirnova'},
            {id = 92, name = 'Leo', login = 'tolstoy_leo'},
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
    t.assert_equals(errs[1].operation_data, {4, 1161, "Anastasia", "AnnaKar"})

    t.assert_str_contains(errs[2].err, batching_utils.stop_on_error_msg)
    t.assert_equals(errs[2].operation_data, {9, 1644, "Anna", "a.smirnova"})

    t.assert_str_contains(errs[3].err, batching_utils.stop_on_error_msg)
    t.assert_equals(errs[3].operation_data, {10, 569, "Sergey", "s.petrenko"})

    t.assert_str_contains(errs[4].err, 'Duplicate key exists')
    t.assert_equals(errs[4].operation_data, {71, 1802, "Alex", "AGolden"})

    t.assert_str_contains(errs[5].err, batching_utils.stop_on_error_msg)
    t.assert_equals(errs[5].operation_data, {92, 2040, "Leo", "tolstoy_leo"})

    -- primary key = 4 -> bucket_id = 1161 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['developers']:get(4)
    t.assert_equals(result, nil)

    -- primary key = 71 -> bucket_id = 1802 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['developers']:get(71)
    t.assert_equals(result, nil)

    -- primary key = 10 -> bucket_id = 569 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['developers']:get(10)
    t.assert_equals(result, nil)

    -- primary key = 9 -> bucket_id = 1644 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['developers']:get(9)
    t.assert_equals(result, nil)

    -- primary key = 92 -> bucket_id = 2040 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['developers']:get(92)
    t.assert_equals(result, {92, 2040, 'Artur', 'AGolden'})
end

pgroup.test_all_success_rollback_on_error = function(g)
    -- replace_many
    -- all success
    local result, errs = g.cluster.main_server.net_box:call('crud.replace_many', {
        'developers',
        {
            {1, box.NULL, 'Fedor', 'FDost'},
            {2, box.NULL, 'Anna', 'AnnaKar'},
            {3, box.NULL, 'Daria', 'mongend'}
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
        {name = 'login', type = 'string'},
    })

    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_items_equals(objects, {
        {id = 1, name = 'Fedor', login = 'FDost', bucket_id = 477},
        {id = 2, name = 'Anna', login = 'AnnaKar', bucket_id = 401},
        {id = 3, name = 'Daria', login = 'mongend', bucket_id = 2804},
    })

    -- get
    -- primary key = 1 -> bucket_id = 477 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['developers']:get(1)
    t.assert_equals(result, {1, 477, 'Fedor', 'FDost'})

    -- primary key = 2 -> bucket_id = 401 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['developers']:get(2)
    t.assert_equals(result, {2, 401, 'Anna', 'AnnaKar'})

    -- primary key = 3 -> bucket_id = 2804 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['developers']:get(3)
    t.assert_equals(result, {3, 2804, 'Daria', 'mongend'})
end

pgroup.test_object_all_success_rollback_on_error = function(g)
    -- insert
    -- primary key = 1 -> bucket_id = 477 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['developers']:insert({1, 477, 'Alex', 'alexpushkin'})
    t.assert_equals(result, {1, 477, 'Alex', 'alexpushkin'})

    -- replace_object_many
    -- all success
    local result, errs = g.cluster.main_server.net_box:call('crud.replace_object_many', {
        'developers',
        {
            {id = 1, name = 'Fedor', login = 'FDost'},
            {id = 2, name = 'Anna', login = 'AnnaKar'},
            {id = 3, name = 'Daria', login = 'mongend'}
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
        {name = 'login', type = 'string'},
    })

    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_items_equals(objects, {
        {id = 1, name = 'Fedor', login = 'FDost', bucket_id = 477},
        {id = 2, name = 'Anna', login = 'AnnaKar', bucket_id = 401},
        {id = 3, name = 'Daria', login = 'mongend', bucket_id = 2804},
    })

    -- get
    -- primary key = 1 -> bucket_id = 477 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['developers']:get(1)
    t.assert_equals(result, {1, 477, 'Fedor', 'FDost'})

    -- primary key = 2 -> bucket_id = 401 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['developers']:get(2)
    t.assert_equals(result, {2, 401, 'Anna', 'AnnaKar'})

    -- primary key = 3 -> bucket_id = 2804 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['developers']:get(3)
    t.assert_equals(result, {3, 2804, 'Daria', 'mongend'})
end

pgroup.test_partial_success_rollback_on_error = function(g)
    -- insert
    -- primary key = 3 -> bucket_id = 2804 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['developers']:insert({3, 2804, 'Daria', 'mongend'})
    t.assert_equals(result, {3, 2804, 'Daria', 'mongend'})

    -- primary key = 9 -> bucket_id = 1644 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['developers']:insert({9, 1644, 'Nicolo', 'n.black'})
    t.assert_equals(result, {9, 1644, 'Nicolo', 'n.black'})

    -- replace_many
    -- stop_on_error = false, rollback_on_error = true
    -- two error on one storage with rollback
    local result, errs = g.cluster.main_server.net_box:call('crud.replace_many', {
        'developers',
        {
            {22, box.NULL, 'Alex', 'alexpushkin'},
            {92, box.NULL, 'Artur', 'AGolden'},
            {71, box.NULL, 'Anastasia', 'n.black'},
            {5, box.NULL, 'Sergey', 's.petrenko'},
            {11, box.NULL, 'Anna', 'mongend'},
        },
        {
            rollback_on_error = true,
        }
    })

    t.assert_not_equals(errs, nil)
    t.assert_equals(#errs, 3)

    table.sort(errs, function(err1, err2) return err1.operation_data[1] < err2.operation_data[1] end)

    t.assert_str_contains(errs[1].err, 'Duplicate key exists')
    t.assert_equals(errs[1].operation_data, {11, 2652, "Anna", "mongend"})

    t.assert_str_contains(errs[2].err, 'Duplicate key exists')
    t.assert_equals(errs[2].operation_data, {71, 1802, "Anastasia", "n.black"})

    t.assert_str_contains(errs[3].err, batching_utils.rollback_on_error_msg)
    t.assert_equals(errs[3].operation_data, {92, 2040, "Artur", "AGolden"})

    t.assert_equals(result.metadata, {
        {name = 'id', type = 'unsigned'},
        {name = 'bucket_id', type = 'unsigned'},
        {name = 'name', type = 'string'},
        {name = 'login', type = 'string'},
    })

    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_items_equals(objects, {
        {id = 5, name = 'Sergey', login = "s.petrenko", bucket_id = 1172},
        {id = 22, name = 'Alex', login = "alexpushkin", bucket_id = 655},
    })

    -- get
    -- primary key = 22 -> bucket_id = 655 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['developers']:get(22)
    t.assert_equals(result, {22, 655, 'Alex', 'alexpushkin'})

    -- primary key = 92 -> bucket_id = 2040 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['developers']:get(92)
    t.assert_equals(result, nil)

    -- primary key = 5 -> bucket_id = 1172 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['developers']:get(5)
    t.assert_equals(result, {5, 1172, 'Sergey', 's.petrenko'})

    -- primary key = 11 -> bucket_id = 2652 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['developers']:get(11)
    t.assert_equals(result, nil)

    -- primary key = 71 -> bucket_id = 1802 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['developers']:get(71)
    t.assert_equals(result, nil)
end

pgroup.test_object_partial_success_rollback_on_error = function(g)
    -- insert
    -- primary key = 3 -> bucket_id = 2804 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['developers']:insert({3, 2804, 'Daria', 'mongend'})
    t.assert_equals(result, {3, 2804, 'Daria', 'mongend'})

    -- primary key = 9 -> bucket_id = 1644 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['developers']:insert({9, 1644, 'Nicolo', 'n.black'})
    t.assert_equals(result, {9, 1644, 'Nicolo', 'n.black'})

    -- replace_object_many
    -- stop_on_error = false, rollback_on_error = true
    -- two error on one storage with rollback
    local result, errs = g.cluster.main_server.net_box:call('crud.replace_object_many', {
        'developers',
        {
            {id = 22, name = 'Alex', login = 'alexpushkin'},
            {id = 92, name = 'Artur', login = 'AGolden'},
            {id = 71, name = 'Anastasia', login = 'n.black'},
            {id = 5, name = 'Sergey', login = 's.petrenko'},
            {id = 11, name = 'Anna', login = 'mongend'},
        },
        {
            rollback_on_error = true,
        }
    })

    t.assert_not_equals(errs, nil)
    t.assert_equals(#errs, 3)

    table.sort(errs, function(err1, err2) return err1.operation_data[1] < err2.operation_data[1] end)

    t.assert_str_contains(errs[1].err, 'Duplicate key exists')
    t.assert_equals(errs[1].operation_data, {11, 2652, "Anna", "mongend"})

    t.assert_str_contains(errs[2].err, 'Duplicate key exists')
    t.assert_equals(errs[2].operation_data, {71, 1802, "Anastasia", "n.black"})

    t.assert_str_contains(errs[3].err, batching_utils.rollback_on_error_msg)
    t.assert_equals(errs[3].operation_data, {92, 2040, "Artur", "AGolden"})

    t.assert_equals(result.metadata, {
        {name = 'id', type = 'unsigned'},
        {name = 'bucket_id', type = 'unsigned'},
        {name = 'name', type = 'string'},
        {name = 'login', type = 'string'},
    })

    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_items_equals(objects, {
        {id = 5, name = 'Sergey', login = "s.petrenko", bucket_id = 1172},
        {id = 22, name = 'Alex', login = "alexpushkin", bucket_id = 655},
    })

    -- get
    -- primary key = 22 -> bucket_id = 655 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['developers']:get(22)
    t.assert_equals(result, {22, 655, 'Alex', 'alexpushkin'})

    -- primary key = 92 -> bucket_id = 2040 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['developers']:get(92)
    t.assert_equals(result, nil)

    -- primary key = 5 -> bucket_id = 1172 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['developers']:get(5)
    t.assert_equals(result, {5, 1172, 'Sergey', 's.petrenko'})

    -- primary key = 11 -> bucket_id = 2652 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['developers']:get(11)
    t.assert_equals(result, nil)

    -- primary key = 71 -> bucket_id = 1802 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['developers']:get(71)
    t.assert_equals(result, nil)
end

pgroup.test_no_success_rollback_on_error = function(g)
    -- insert
    -- primary key = 2 -> bucket_id = 401 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['developers']:insert({2, 401, 'Anna', 'a.leonhart'})
    t.assert_equals(result, {2, 401, 'Anna', 'a.leonhart'})

    -- primary key = 3 -> bucket_id = 2804 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['developers']:insert({3, 2804, 'Daria', 'mongend'})
    t.assert_equals(result, {3, 2804, 'Daria', 'mongend'})

    -- primary key = 5 -> bucket_id = 1172 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['developers']:insert({5, 1172, 'Sergey', 's.petrenko'})
    t.assert_equals(result, {5, 1172, 'Sergey', 's.petrenko'})

    -- primary key = 71 -> bucket_id = 1802 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['developers']:insert({71, 1802, 'Oleg', 'OKonov'})
    t.assert_equals(result, {71, 1802, 'Oleg', 'OKonov'})

    -- replace_many
    -- fails for both: s1-master s2-master
    -- two errors on each storage with rollback
    local result, errs = g.cluster.main_server.net_box:call('crud.replace_many', {
        'developers',
        {
            {1, box.NULL, 'Eren', 'e.eger'},
            {92, box.NULL, 'Alexey', 'black_alex'},
            {11, box.NULL, 'Olga', 'OKonov'},
            {6, box.NULL, 'Anastasia', 'a.leonhart'},
            {4, box.NULL, 'Semen', 's.petrenko'},
            {9, box.NULL, 'Leo', 'mongend'},
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
    t.assert_equals(errs[1].operation_data, {1, 477, "Eren", "e.eger"})

    t.assert_str_contains(errs[2].err, 'Duplicate key exists')
    t.assert_equals(errs[2].operation_data, {4, 1161, "Semen", "s.petrenko"})

    t.assert_str_contains(errs[3].err, 'Duplicate key exists')
    t.assert_equals(errs[3].operation_data, {6, 1064, "Anastasia", "a.leonhart"})

    t.assert_str_contains(errs[4].err, 'Duplicate key exists')
    t.assert_equals(errs[4].operation_data, {9, 1644, "Leo", "mongend"})

    t.assert_str_contains(errs[5].err, 'Duplicate key exists')
    t.assert_equals(errs[5].operation_data, {11, 2652, "Olga", "OKonov"})

    t.assert_str_contains(errs[6].err, batching_utils.rollback_on_error_msg)
    t.assert_equals(errs[6].operation_data, {92, 2040, "Alexey", "black_alex"})

    -- primary key = 1 -> bucket_id = 477 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['developers']:get(1)
    t.assert_equals(result, nil)

    -- primary key = 92 -> bucket_id = 2040 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['developers']:get(92)
    t.assert_equals(result, nil)

    -- primary key = 4 -> bucket_id = 1161 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['developers']:get(4)
    t.assert_equals(result, nil)

    -- primary key = 9 -> bucket_id = 1644 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['developers']:get(9)
    t.assert_equals(result, nil)

    -- primary key = 6 -> bucket_id = 1064 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['developers']:get(6)
    t.assert_equals(result, nil)

    -- primary key = 11 -> bucket_id = 2652 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['developers']:get(11)
    t.assert_equals(result, nil)
end

pgroup.test_object_no_success_rollback_on_error = function(g)
    -- insert
    -- primary key = 2 -> bucket_id = 401 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['developers']:insert({2, 401, 'Anna', 'a.leonhart'})
    t.assert_equals(result, {2, 401, 'Anna', 'a.leonhart'})

    -- primary key = 3 -> bucket_id = 2804 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['developers']:insert({3, 2804, 'Daria', 'mongend'})
    t.assert_equals(result, {3, 2804, 'Daria', 'mongend'})

    -- primary key = 5 -> bucket_id = 1172 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['developers']:insert({5, 1172, 'Sergey', 's.petrenko'})
    t.assert_equals(result, {5, 1172, 'Sergey', 's.petrenko'})

    -- primary key = 71 -> bucket_id = 1802 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['developers']:insert({71, 1802, 'Oleg', 'OKonov'})
    t.assert_equals(result, {71, 1802, 'Oleg', 'OKonov'})

    -- replace_object_many
    -- fails for both: s1-master s2-master
    -- two errors on each storage with rollback
    local result, errs = g.cluster.main_server.net_box:call('crud.replace_object_many', {
        'developers',
        {
            {id = 1, name = 'Eren', login = 'e.eger'},
            {id = 92, name = 'Alexey', login = 'black_alex'},
            {id = 11, name = 'Olga', login = 'OKonov'},
            {id = 6, name = 'Anastasia', login = 'a.leonhart'},
            {id = 4, name = 'Semen', login = 's.petrenko'},
            {id = 9, name = 'Leo', login = 'mongend'},
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
    t.assert_equals(errs[1].operation_data, {1, 477, "Eren", "e.eger"})

    t.assert_str_contains(errs[2].err, 'Duplicate key exists')
    t.assert_equals(errs[2].operation_data, {4, 1161, "Semen", "s.petrenko"})

    t.assert_str_contains(errs[3].err, 'Duplicate key exists')
    t.assert_equals(errs[3].operation_data, {6, 1064, "Anastasia", "a.leonhart"})

    t.assert_str_contains(errs[4].err, 'Duplicate key exists')
    t.assert_equals(errs[4].operation_data, {9, 1644, "Leo", "mongend"})

    t.assert_str_contains(errs[5].err, 'Duplicate key exists')
    t.assert_equals(errs[5].operation_data, {11, 2652, "Olga", "OKonov"})

    t.assert_str_contains(errs[6].err, batching_utils.rollback_on_error_msg)
    t.assert_equals(errs[6].operation_data, {92, 2040, "Alexey", "black_alex"})

    -- primary key = 1 -> bucket_id = 477 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['developers']:get(1)
    t.assert_equals(result, nil)

    -- primary key = 92 -> bucket_id = 2040 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['developers']:get(92)
    t.assert_equals(result, nil)

    -- primary key = 4 -> bucket_id = 1161 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['developers']:get(4)
    t.assert_equals(result, nil)

    -- primary key = 9 -> bucket_id = 1644 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['developers']:get(9)
    t.assert_equals(result, nil)

    -- primary key = 6 -> bucket_id = 1064 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['developers']:get(6)
    t.assert_equals(result, nil)

    -- primary key = 11 -> bucket_id = 2652 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['developers']:get(11)
    t.assert_equals(result, nil)
end

pgroup.test_all_success_rollback_and_stop_on_error = function(g)
    -- replace_many
    -- all success
    local result, errs = g.cluster.main_server.net_box:call('crud.replace_many', {
        'developers',
        {
            {1, box.NULL, 'Fedor', 'FDost'},
            {2, box.NULL, 'Anna', 'AnnaKar'},
            {3, box.NULL, 'Daria', 'mongend'}
        },
        {
            rollback_on_error = true,
            stop_on_error = true,
        }
    })

    t.assert_equals(errs, nil)
    t.assert_equals(result.metadata, {
        {name = 'id', type = 'unsigned'},
        {name = 'bucket_id', type = 'unsigned'},
        {name = 'name', type = 'string'},
        {name = 'login', type = 'string'},
    })

    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_items_equals(objects, {
        {id = 1, name = 'Fedor', login = 'FDost', bucket_id = 477},
        {id = 2, name = 'Anna', login = 'AnnaKar', bucket_id = 401},
        {id = 3, name = 'Daria', login = 'mongend', bucket_id = 2804},
    })

    -- get
    -- primary key = 1 -> bucket_id = 477 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['developers']:get(1)
    t.assert_equals(result, {1, 477, 'Fedor', 'FDost'})

    -- primary key = 2 -> bucket_id = 401 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['developers']:get(2)
    t.assert_equals(result, {2, 401, 'Anna', 'AnnaKar'})

    -- primary key = 3 -> bucket_id = 2804 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['developers']:get(3)
    t.assert_equals(result, {3, 2804, 'Daria', 'mongend'})
end

pgroup.test_object_all_success_rollback_and_stop_on_error = function(g)
    -- insert
    -- primary key = 1 -> bucket_id = 477 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['developers']:insert({1, 477, 'Alex', 'alexpushkin'})
    t.assert_equals(result, {1, 477, 'Alex', 'alexpushkin'})

    -- replace_object_many
    -- all success
    local result, errs = g.cluster.main_server.net_box:call('crud.replace_object_many', {
        'developers',
        {
            {id = 1, name = 'Fedor', login = 'FDost'},
            {id = 2, name = 'Anna', login = 'AnnaKar'},
            {id = 3, name = 'Daria', login = 'mongend'}
        },
        {
            rollback_on_error = true,
            stop_on_error = true,
        }
    })

    t.assert_equals(errs, nil)
    t.assert_equals(result.metadata, {
        {name = 'id', type = 'unsigned'},
        {name = 'bucket_id', type = 'unsigned'},
        {name = 'name', type = 'string'},
        {name = 'login', type = 'string'},
    })

    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_items_equals(objects, {
        {id = 1, name = 'Fedor', login = 'FDost', bucket_id = 477},
        {id = 2, name = 'Anna', login = 'AnnaKar', bucket_id = 401},
        {id = 3, name = 'Daria', login = 'mongend', bucket_id = 2804},
    })

    -- get
    -- primary key = 1 -> bucket_id = 477 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['developers']:get(1)
    t.assert_equals(result, {1, 477, 'Fedor', 'FDost'})

    -- primary key = 2 -> bucket_id = 401 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['developers']:get(2)
    t.assert_equals(result, {2, 401, 'Anna', 'AnnaKar'})

    -- primary key = 3 -> bucket_id = 2804 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['developers']:get(3)
    t.assert_equals(result, {3, 2804, 'Daria', 'mongend'})
end

pgroup.test_partial_success_rollback_and_stop_on_error = function(g)
    -- insert
    -- primary key = 3 -> bucket_id = 2804 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['developers']:insert({3, 2804, 'Anna', 'a.leonhart'})
    t.assert_equals(result, {3, 2804, 'Anna', 'a.leonhart'})

    -- primary key = 71 -> bucket_id = 1802 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['developers']:insert({71, 1802, 'Oleg', 'OKonov'})
    t.assert_equals(result, {71, 1802, 'Oleg', 'OKonov'})

    -- replace_many
    -- stop_on_error = true, rollback_on_error = true
    -- two error on one storage with rollback, inserts stop by error on this storage
    -- inserts before error are rollbacked
    local result, errs = g.cluster.main_server.net_box:call('crud.replace_many', {
        'developers',
        {
            {22, box.NULL, 'Alex', 'alexpushkin'},
            {92, box.NULL, 'Artur', 'AGolden'},
            {11, box.NULL, 'Anastasia', 'a.leonhart'},
            {5, box.NULL, 'Sergey', 's.petrenko'},
            {9, box.NULL, 'Anna', 'AnnaBlack'},
            {17, box.NULL, 'Oksana', 'OKonov'},
        },
        {
            stop_on_error = true,
            rollback_on_error  = true,
        }
    })

    t.assert_not_equals(errs, nil)
    t.assert_equals(#errs, 4)

    table.sort(errs, function(err1, err2) return err1.operation_data[1] < err2.operation_data[1] end)

    t.assert_str_contains(errs[1].err, batching_utils.stop_on_error_msg)
    t.assert_equals(errs[1].operation_data, {9, 1644, "Anna", "AnnaBlack"})

    t.assert_str_contains(errs[2].err, 'Duplicate key exists')
    t.assert_equals(errs[2].operation_data, {11, 2652, "Anastasia", "a.leonhart"})

    t.assert_str_contains(errs[3].err, batching_utils.stop_on_error_msg)
    t.assert_equals(errs[3].operation_data, {17, 2900, "Oksana", "OKonov"})

    t.assert_str_contains(errs[4].err, batching_utils.rollback_on_error_msg)
    t.assert_equals(errs[4].operation_data, {92, 2040, "Artur", "AGolden"})

    t.assert_equals(result.metadata, {
        {name = 'id', type = 'unsigned'},
        {name = 'bucket_id', type = 'unsigned'},
        {name = 'name', type = 'string'},
        {name = 'login', type = 'string'},
    })

    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_items_equals(objects, {
        {id = 5, name = 'Sergey', login = "s.petrenko", bucket_id = 1172},
        {id = 22, name = 'Alex', login = "alexpushkin", bucket_id = 655},
    })

    -- get
    -- primary key = 22 -> bucket_id = 655 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['developers']:get(22)
    t.assert_equals(result, {22, 655, 'Alex', 'alexpushkin'})

    -- primary key = 92 -> bucket_id = 2040 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['developers']:get(92)
    t.assert_equals(result, nil)

    -- primary key = 5 -> bucket_id = 1172 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['developers']:get(5)
    t.assert_equals(result, {5, 1172, 'Sergey', 's.petrenko'})

    -- primary key = 11 -> bucket_id = 2652 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['developers']:get(11)
    t.assert_equals(result, nil)

    -- primary key = 9 -> bucket_id = 1644 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['developers']:get(9)
    t.assert_equals(result, nil)

    -- primary key = 17 -> bucket_id = 2900 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['developers']:get(17)
    t.assert_equals(result, nil)
end

pgroup.test_object_partial_success_rollback_and_stop_on_error = function(g)
    -- insert
    -- primary key = 3 -> bucket_id = 2804 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['developers']:insert({3, 2804, 'Anna', 'a.leonhart'})
    t.assert_equals(result, {3, 2804, 'Anna', 'a.leonhart'})

    -- primary key = 71 -> bucket_id = 1802 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['developers']:insert({71, 1802, 'Oleg', 'OKonov'})
    t.assert_equals(result, {71, 1802, 'Oleg', 'OKonov'})

    -- replace_object_many
    -- stop_on_error = true, rollback_on_error = true
    -- two error on one storage with rollback, inserts stop by error on this storage
    -- inserts before error are rollbacked
    local result, errs = g.cluster.main_server.net_box:call('crud.replace_object_many', {
        'developers',
        {
            {id = 22, name = 'Alex', login = 'alexpushkin'},
            {id = 92, name = 'Artur', login = 'AGolden'},
            {id = 11, name = 'Anastasia', login = 'a.leonhart'},
            {id = 5, name = 'Sergey', login = 's.petrenko'},
            {id = 9, name = 'Anna', login = 'AnnaBlack'},
            {id = 17, name = 'Oksana', login = 'OKonov'},
        },
        {
            stop_on_error = true,
            rollback_on_error  = true,
        }
    })

    t.assert_not_equals(errs, nil)
    t.assert_equals(#errs, 4)

    table.sort(errs, function(err1, err2) return err1.operation_data[1] < err2.operation_data[1] end)

    t.assert_str_contains(errs[1].err, batching_utils.stop_on_error_msg)
    t.assert_equals(errs[1].operation_data, {9, 1644, "Anna", "AnnaBlack"})

    t.assert_str_contains(errs[2].err, 'Duplicate key exists')
    t.assert_equals(errs[2].operation_data, {11, 2652, "Anastasia", "a.leonhart"})

    t.assert_str_contains(errs[3].err, batching_utils.stop_on_error_msg)
    t.assert_equals(errs[3].operation_data, {17, 2900, "Oksana", "OKonov"})

    t.assert_str_contains(errs[4].err, batching_utils.rollback_on_error_msg)
    t.assert_equals(errs[4].operation_data, {92, 2040, "Artur", "AGolden"})

    t.assert_equals(result.metadata, {
        {name = 'id', type = 'unsigned'},
        {name = 'bucket_id', type = 'unsigned'},
        {name = 'name', type = 'string'},
        {name = 'login', type = 'string'},
    })

    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_items_equals(objects, {
        {id = 5, name = 'Sergey', login = "s.petrenko", bucket_id = 1172},
        {id = 22, name = 'Alex', login = "alexpushkin", bucket_id = 655},
    })

    -- get
    -- primary key = 22 -> bucket_id = 655 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['developers']:get(22)
    t.assert_equals(result, {22, 655, 'Alex', 'alexpushkin'})

    -- primary key = 92 -> bucket_id = 2040 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['developers']:get(92)
    t.assert_equals(result, nil)

    -- primary key = 5 -> bucket_id = 1172 -> s2-master
    local conn_s2 = g.cluster:server('s2-master').net_box
    local result = conn_s2.space['developers']:get(5)
    t.assert_equals(result, {5, 1172, 'Sergey', 's.petrenko'})

    -- primary key = 11 -> bucket_id = 2652 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['developers']:get(11)
    t.assert_equals(result, nil)

    -- primary key = 9 -> bucket_id = 1644 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['developers']:get(9)
    t.assert_equals(result, nil)

    -- primary key = 17 -> bucket_id = 2900 -> s1-master
    local conn_s1 = g.cluster:server('s1-master').net_box
    local result = conn_s1.space['developers']:get(17)
    t.assert_equals(result, nil)
end

pgroup.test_partial_result = function(g)
    -- bad fields format
    local result, errs = g.cluster.main_server.net_box:call('crud.replace_many', {
        'developers',
        {
            {1, box.NULL, 'Fedor', 'FDost'},
            {2, box.NULL, 'Anna', 'a.leonhart'},
        },
        {fields = {'id', 'invalid'}},
    })

    t.assert_equals(result, nil)
    t.assert_not_equals(errs, nil)
    t.assert_equals(#errs, 1)
    t.assert_str_contains(errs[1].err, 'Space format doesn\'t contain field named "invalid"')

    -- replace_many
    local result, errs = g.cluster.main_server.net_box:call('crud.replace_many', {
        'developers',
        {
            {1, box.NULL, 'Fedor', 'FDost'},
            {2, box.NULL, 'Anna', 'a.leonhart'},
            {3, box.NULL, 'Daria', 'mongen'}
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
    local result, errs = g.cluster.main_server.net_box:call('crud.replace_object_many', {
        'developers',
        {
            {id = 1, name = 'Fedor', login = 'FDost'},
            {id = 2, name = 'Anna', login = 'a.leonhart'},
        },
        {fields = {'id', 'invalid'}},
    })

    t.assert_equals(result, nil)
    t.assert_not_equals(errs, nil)
    t.assert_equals(#errs, 1)
    t.assert_str_contains(errs[1].err, 'Space format doesn\'t contain field named "invalid"')

    -- replace_object_many
    local result, errs = g.cluster.main_server.net_box:call('crud.replace_object_many', {
        'developers',
        {
            {id = 1, name = 'Fedor', login = 'FDost'},
            {id = 2, name = 'Anna', login = 'a.leonhart'},
            {id = 3, name = 'Daria', login = 'mongen'}
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
    -- replace_many
    local batch_replace_opts = {timeout = 1, fields = {'name', 'login'}}
    local new_batch_replace_opts, err = g.cluster.main_server:eval([[
        local crud = require('crud')

        local batch_replace_opts = ...

        local _, err = crud.replace_many('developers', {
            {1, box.NULL, 'Alex', "alexpushkin"}
        }, batch_replace_opts)

        return batch_replace_opts, err
    ]], {batch_replace_opts})

    t.assert_equals(err, nil)
    t.assert_equals(new_batch_replace_opts, batch_replace_opts)

    -- replace_object_many
    local batch_replace_opts = {timeout = 1, fields = {'name', 'login'}}
    local new_batch_replace_opts, err = g.cluster.main_server:eval([[
        local crud = require('crud')

        local batch_replace_opts = ...

        local _, err = crud.replace_object_many('developers', {
            {id = 2, name = 'Fedor', login = 'FDost'}
        }, batch_replace_opts)

        return batch_replace_opts, err
    ]], {batch_replace_opts})

    t.assert_equals(err, nil)
    t.assert_equals(new_batch_replace_opts, batch_replace_opts)
end

pgroup.test_noreturn_opt = function(g)
    -- replace_many with noreturn, all tuples are correct
    local result, errs = g.cluster.main_server.net_box:call('crud.replace_many', {
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

    -- replace_many with noreturn, some tuples are correct
    local result, errs = g.cluster.main_server.net_box:call('crud.replace_many', {
        'customers',
        {
            {1, box.NULL, 'Fedor', 59},
            {box.NULL, box.NULL, 'Anna', 23},
            {box.NULL, box.NULL, 'Daria', 18}
        },
        {noreturn = true},
    })

    t.assert_not_equals(errs, nil)
    t.assert_equals(#errs, 2)
    t.assert_equals(result, nil)

    -- replace_many with noreturn, all tuples are not correct
    local result, errs = g.cluster.main_server.net_box:call('crud.replace_many', {
        'customers',
        {
            {box.NULL, box.NULL, 'Fedor', 59},
            {box.NULL, box.NULL, 'Anna', 23},
            {box.NULL, box.NULL, 'Daria', 18}
        },
        {noreturn = true},
    })

    t.assert_not_equals(errs, nil)
    t.assert_equals(#errs, 3)
    t.assert_equals(result, nil)

    -- replace_object_many with noreturn, all tuples are correct
    local result, errs = g.cluster.main_server.net_box:call('crud.replace_object_many', {
        'customers',
        {
            {id = 1, name = 'Fedor', age = 100},
            {id = 2, name = 'Anna', age = 100},
            {id = 3, name = 'Daria', age = 100}
        },
        {noreturn = true},
    })

    t.assert_equals(errs, nil)
    t.assert_equals(result, nil)

    -- replace_object_many with noreturn, some tuples are correct
    local result, errs = g.cluster.main_server.net_box:call('crud.replace_object_many', {
        'customers',
        {
            {id = 1, name = 'Fedor', age = 100},
            {id = box.NULL, name = 'Anna', age = 100},
            {id = box.NULL, name = 'Daria', age = 100}
        },
        {noreturn = true},
    })

    t.assert_not_equals(errs, nil)
    t.assert_equals(#errs, 2)
    t.assert_equals(result, nil)

    -- replace_object_many with noreturn, all tuples are not correct
    local result, errs = g.cluster.main_server.net_box:call('crud.replace_object_many', {
        'customers',
        {
            {id = box.NULL, name = 'Fedor', age = 100},
            {id = box.NULL, name = 'Anna', age = 100},
            {id = box.NULL, name = 'Daria', age = 100}
        },
        {noreturn = true},
    })

    t.assert_not_equals(errs, nil)
    t.assert_equals(#errs, 3)
    t.assert_equals(result, nil)
end
