local fio = require('fio')

local t = require('luatest')

local crud = require('crud')

local helpers = require('test.helper')

local pgroup = t.group('borders', {
    {engine = 'memtx'},
    {engine = 'vinyl'},
})

pgroup.before_all(function(g)
    g.cluster = helpers.Cluster:new({
        datadir = fio.tempdir(),
        server_command = helpers.entrypoint('srv_select'),
        use_vshard = true,
        replicasets = helpers.get_test_replicasets(),
        env = {
            ['ENGINE'] = g.params.engine,
        },
    })

    g.cluster:start()

    g.space_format = g.cluster.servers[2].net_box.space.customers:format()
end)

pgroup.after_all(function(g) helpers.stop_cluster(g.cluster) end)

pgroup.before_each(function(g)
    helpers.truncate_space_on_cluster(g.cluster, 'customers')
end)


pgroup.test_non_existent_space = function(g)
    -- min
    local result, err = g.cluster.main_server.net_box:call(
       'crud.min', {'non_existent_space'}
    )

    t.assert_equals(result, nil)
    t.assert_str_contains(err.err, "Space \"non_existent_space\" doesn't exist")

    -- max
    local result, err = g.cluster.main_server.net_box:call(
       'crud.max', {'non_existent_space'}
    )

    t.assert_equals(result, nil)
    t.assert_str_contains(err.err, "Space \"non_existent_space\" doesn't exist")
end

pgroup.test_non_existent_index = function(g)
    -- min
    local result, err = g.cluster.main_server.net_box:call(
       'crud.min', {'customers', 'non_existent_index'}
    )

    t.assert_equals(result, nil)
    t.assert_str_contains(err.err, "Index \"non_existent_index\" of space \"customers\" doesn't exist")

    local result, err = g.cluster.main_server.net_box:call(
       'crud.min', {'customers', 13}
    )

    t.assert_equals(result, nil)
    t.assert_str_contains(err.err, "Index \"13\" of space \"customers\" doesn't exist")

    -- max
    local result, err = g.cluster.main_server.net_box:call(
       'crud.max', {'customers', 'non_existent_index'}
    )

    t.assert_equals(result, nil)
    t.assert_str_contains(err.err, "Index \"non_existent_index\" of space \"customers\" doesn't exist")

    local result, err = g.cluster.main_server.net_box:call(
       'crud.max', {'customers', 13}
    )

    t.assert_equals(result, nil)
    t.assert_str_contains(err.err, "Index \"13\" of space \"customers\" doesn't exist")
end

pgroup.test_empty_space = function(g)
    -- min
    local result, err = g.cluster.main_server.net_box:call(
       'crud.min', {'customers'}
    )

    t.assert_equals(err, nil)
    t.assert_equals(#result.rows, 0)

    -- min by age index with fields
    local result, err = g.cluster.main_server.net_box:call(
       'crud.min', {'customers', 'age_index', {fields = {'name'}}}
    )

    t.assert_equals(err, nil)
    t.assert_equals(#result.rows, 0)

    -- max
    local result, err = g.cluster.main_server.net_box:call(
       'crud.max', {'customers'}
    )

    t.assert_equals(err, nil)
    t.assert_equals(#result.rows, 0)

    -- max by age index with fields
    local result, err = g.cluster.main_server.net_box:call(
       'crud.max', {'customers', 'age_index', {fields = {'name'}}}
    )

    t.assert_equals(err, nil)
    t.assert_equals(#result.rows, 0)
end

pgroup.test_min = function(g)
    local customers = helpers.insert_objects(g, 'customers', {
        {
            id = 1, name = "Elizabeth", last_name = "Jackson",
            age = 21, city = "New York",
        }, {
            id = 2, name = "Mary", last_name = "Brown",
            age = 33, city = "Los Angeles",
        }, {
            id = 3, name = "David", last_name = "Smith",
            age = 12, city = "Los Angeles",
        }, {
            id = 4, name = "William", last_name = "White",
            age = 8, city = "Chicago",
        },
    })

    table.sort(customers, function(obj1, obj2) return obj1.id < obj2.id end)

    -- by primary
    local result, err = g.cluster.main_server.net_box:call('crud.min', {'customers'})
    t.assert_equals(err, nil)
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, helpers.get_objects_by_idxs(customers, {1}))

    -- by primary, index ID is specified
    local result, err = g.cluster.main_server.net_box:call('crud.min', {'customers', 0})
    t.assert_equals(err, nil)
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, helpers.get_objects_by_idxs(customers, {1}))

    -- by primary with fields
    local result, err = g.cluster.main_server.net_box:call('crud.min',
        {'customers', nil, {fields = {'name', 'last_name'}}}
    )
    t.assert_equals(err, nil)
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, {{name = "Elizabeth", last_name = "Jackson"}})

    -- by age index
    local result, err = g.cluster.main_server.net_box:call('crud.min', {'customers', 'age_index'})
    t.assert_equals(err, nil)
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, helpers.get_objects_by_idxs(customers, {4}))

    -- by age index, index ID is specified
    local result, err = g.cluster.main_server.net_box:call('crud.min', {'customers', 2})
    t.assert_equals(err, nil)
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, helpers.get_objects_by_idxs(customers, {4}))

    -- by age index with fields
    local result, err = g.cluster.main_server.net_box:call('crud.min',
        {'customers', 'age_index', {fields = {'name', 'last_name'}}}
    )
    t.assert_equals(err, nil)
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, {{name = "William", last_name = "White"}})

    -- by composite index full_name
    local result, err = g.cluster.main_server.net_box:call('crud.min', {'customers', 'full_name'})
    t.assert_equals(err, nil)
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, helpers.get_objects_by_idxs(customers, {3}))

    -- by composite index full_name, index ID is specified
    local result, err = g.cluster.main_server.net_box:call('crud.min', {'customers', 5})
    t.assert_equals(err, nil)
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, helpers.get_objects_by_idxs(customers, {3}))

    -- by composite index full_name with fields
    local result, err = g.cluster.main_server.net_box:call('crud.min',
        {'customers', 'full_name', {fields = {'name', 'last_name'}}}
    )
    t.assert_equals(err, nil)
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, {{name = "David", last_name = "Smith"}})
end

pgroup.test_max = function(g)
    local customers = helpers.insert_objects(g, 'customers', {
        {
            id = 1, name = "Elizabeth", last_name = "Jackson",
            age = 21, city = "New York",
        }, {
            id = 2, name = "Mary", last_name = "Brown",
            age = 33, city = "Los Angeles",
        }, {
            id = 3, name = "David", last_name = "Smith",
            age = 12, city = "Los Angeles",
        }, {
            id = 4, name = "William", last_name = "White",
            age = 8, city = "Chicago",
        },
    })

    table.sort(customers, function(obj1, obj2) return obj1.id < obj2.id end)

    -- by primary
    local result, err = g.cluster.main_server.net_box:call('crud.max', {'customers'})
    t.assert_equals(err, nil)
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, helpers.get_objects_by_idxs(customers, {4}))

    -- by primary, index ID is specified
    local result, err = g.cluster.main_server.net_box:call('crud.max', {'customers', 0})
    t.assert_equals(err, nil)
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, helpers.get_objects_by_idxs(customers, {4}))

    -- by primary with fields
    local result, err = g.cluster.main_server.net_box:call('crud.max',
        {'customers', nil, {fields = {'name', 'last_name'}}}
    )
    t.assert_equals(err, nil)
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, {{name = "William", last_name = "White"}})

    -- by age index
    local result, err = g.cluster.main_server.net_box:call('crud.max', {'customers', 'age_index'})
    t.assert_equals(err, nil)
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, helpers.get_objects_by_idxs(customers, {2}))

    -- by age index, index ID is specified
    local result, err = g.cluster.main_server.net_box:call('crud.max', {'customers', 2})
    t.assert_equals(err, nil)
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, helpers.get_objects_by_idxs(customers, {2}))

    -- by age index with fields
    local result, err = g.cluster.main_server.net_box:call('crud.max',
        {'customers', 'age_index', {fields = {'name', 'last_name'}}}
    )
    t.assert_equals(err, nil)
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, {{name = "Mary", last_name = "Brown"}})

    -- by composite index full_name
    local result, err = g.cluster.main_server.net_box:call('crud.max', {'customers', 'full_name'})
    t.assert_equals(err, nil)
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, helpers.get_objects_by_idxs(customers, {4}))

    -- by composite index full_name, index ID is specified
    local result, err = g.cluster.main_server.net_box:call('crud.max', {'customers', 5})
    t.assert_equals(err, nil)
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, helpers.get_objects_by_idxs(customers, {4}))

    -- by composite index full_name with fields
    local result, err = g.cluster.main_server.net_box:call('crud.max',
        {'customers', 'full_name', {fields = {'name', 'last_name'}}}
    )
    t.assert_equals(err, nil)
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, {{name = "William", last_name = "White"}})
end

pgroup.test_equal_secondary_keys = function(g)
    local bucket_id = 1
    local other_bucket_id, err = helpers.get_other_storage_bucket_id(g.cluster, bucket_id)
    t.assert_not_equals(other_bucket_id, nil, err)

    -- let's insert two tuples on different replicasets to check that
    -- they will be compared by index + primary fields on router
    local customers = helpers.insert_objects(g, 'customers', {
        {
            id = 1, name = "Elizabeth", last_name = "Jackson",
            age = 33, city = "New York",
            bucket_id = bucket_id,
        }, {
            id = 2, name = "Mary", last_name = "Brown",
            age = 33, city = "Los Angeles",
            bucket_id = other_bucket_id,
        },
    })

    table.sort(customers, function(obj1, obj2) return obj1.id < obj2.id end)

    -- min
    local result, err = g.cluster.main_server.net_box:call('crud.min', {'customers', 'age_index'})
    t.assert_equals(err, nil)
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, helpers.get_objects_by_idxs(customers, {1}))

    -- max
    local result, err = g.cluster.main_server.net_box:call('crud.max', {'customers', 'age_index'})
    t.assert_equals(err, nil)
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, helpers.get_objects_by_idxs(customers, {2}))
end

pgroup.test_opts_not_damaged = function(g)
    helpers.insert_objects(g, 'customers', {
        {
            id = 1, name = "Elizabeth", last_name = "Jackson",
            age = 21, city = "New York",
        }, {
            id = 2, name = "Mary", last_name = "Brown",
            age = 33, city = "Los Angeles",
        }, {
            id = 3, name = "David", last_name = "Smith",
            age = 12, city = "Los Angeles",
        }, {
            id = 4, name = "William", last_name = "White",
            age = 8, city = "Chicago",
        },
    })

    -- min
    local min_opts = {timeout = 1, fields = {'name', 'age'}}
    local new_min_opts, err = g.cluster.main_server:eval([[
        local crud = require('crud')

        local min_opts = ...

        local _, err = crud.min('customers', nil, min_opts)

        return min_opts, err
    ]], {min_opts})

    t.assert_equals(err, nil)
    t.assert_equals(new_min_opts, min_opts)

    -- max
    local max_opts = {timeout = 1, fields = {'name', 'age'}}
    local new_max_opts, err = g.cluster.main_server:eval([[
        local crud = require('crud')

        local max_opts = ...

        local _, err = crud.max('customers', nil, max_opts)

        return max_opts, err
    ]], {min_opts})

    t.assert_equals(err, nil)
    t.assert_equals(new_max_opts, max_opts)
end
