local t = require('luatest')
local crud = require('crud')

--- Integration checks for spaces where the sharding index (bucket_id + id)
--- is the primary index and bucket_id can be passed as box.NULL.
local helpers = require('test.helper')

local pgroup = t.group('bucket_id_pk', helpers.backend_matrix({
    {engine = 'memtx'},
    {engine = 'vinyl'},
}))

pgroup.before_all(function(g)
    helpers.start_default_cluster(g, 'srv_bucket_id_pk')
end)

pgroup.after_all(function(g)
    helpers.stop_cluster(g.cluster, g.params.backend)
end)

pgroup.before_each(function(g)
    helpers.truncate_space_on_cluster(g.cluster, 'customers')
end)

local function insert_customer(g, id)
    local result, err = g.router:call(
        'crud.insert_object', {'customers', {id = id, name = 'Fedor', age = 59}})

    t.assert_equals(err, nil)
    t.assert_equals(result.metadata, {
        {name = 'id', type = 'unsigned'},
        {name = 'bucket_id', type = 'unsigned'},
        {name = 'name', type = 'string'},
        {name = 'age', type = 'number'},
    })
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects[1].id, id)
end

--- test where box.NULL passed in primary key
pgroup.test_get = function(g)
    insert_customer(g, 1)

    local result, err = g.router:call('crud.get', {
        'customers', {box.NULL, 1}, {mode = 'write'},
    })
    t.assert_equals(err, nil)
    t.assert_not_equals(result, nil)
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, {{id = 1, name = 'Fedor', age = 59, bucket_id = 477}})
end

pgroup.test_delete = function(g)
    insert_customer(g, 1)

    local result, err = g.router:call('crud.delete', {
        'customers', {box.NULL, 1}
    })
    t.assert_equals(err, nil)
    t.assert_not_equals(result, nil)

    local result, err = g.router:call('crud.get', {
        'customers', {box.NULL, 1}, {mode = 'write'},
    })

    t.assert_equals(err, nil)
    t.assert_equals(#result.rows, 0)
end

pgroup.test_update = function(g)
    insert_customer(g, 1)

    local result, err = g.router:call('crud.update', {'customers', {box.NULL, 1}, {
        {'+', 'age', 10},
        {'=', 'name', 'Leo Tolstoy'},
    }})
    t.assert_equals(err, nil)
    t.assert_not_equals(result, nil)

    local result, err = g.router:call('crud.get', {
        'customers', {box.NULL, 1}, {mode = 'write'},
    })
    t.assert_equals(err, nil)
    t.assert_not_equals(result, nil)
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, {{id = 1, name = 'Leo Tolstoy', age = 69, bucket_id = 477}})
end

--- test where box.NULL passed in tuple
pgroup.test_insert_object = function(g)
    local result, err = g.router:call('crud.insert_object', {
        'customers', {id = 1, name = 'Fedor', age = 59},
    })

    t.assert_equals(err, nil)
    t.assert_not_equals(result, nil)
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, {{id = 1, name = 'Fedor', age = 59, bucket_id = 477}})

    local result, err = g.router:call('crud.get', {
        'customers', {box.NULL, 1}, {mode = 'write'},
    })
    t.assert_equals(err, nil)
    t.assert_not_equals(result, nil)
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, {{id = 1, name = 'Fedor', age = 59, bucket_id = 477}})
end

pgroup.test_insert = function(g)
    local tuple = {1, box.NULL, 'Fedor', 59}

    local result, err = g.router:call('crud.insert', {'customers', tuple})
    t.assert_equals(err, nil)
    t.assert_not_equals(result, nil)
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, {{id = 1, name = 'Fedor', age = 59, bucket_id = 477}})

    local result, err = g.router:call('crud.get', {
        'customers', {box.NULL, 1}, {mode = 'write'},
    })
    t.assert_equals(err, nil)
    t.assert_not_equals(result, nil)
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, {{id = 1, name = 'Fedor', age = 59, bucket_id = 477}})
end

pgroup.test_replace_object = function(g)
    insert_customer(g, 1)

    local result, err = g.router:call('crud.replace_object', {
        'customers', {id = 1, name = 'Leo Tolstoy', age = 69},
    })
    t.assert_equals(err, nil)
    t.assert_not_equals(result, nil)
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, {{id = 1, name = 'Leo Tolstoy', age = 69, bucket_id = 477}})

    local result, err = g.router:call('crud.get', {
        'customers', {box.NULL, 1}, {mode = 'write'},
    })
    t.assert_equals(err, nil)
    t.assert_not_equals(result, nil)
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, {{id = 1, name = 'Leo Tolstoy', age = 69, bucket_id = 477}})
end

pgroup.test_replace = function(g)
    insert_customer(g, 1)

    local tuple = {1, box.NULL, 'Leo Tolstoy', 69}

    local result, err = g.router:call('crud.replace', {'customers', tuple})
    t.assert_equals(err, nil)
    t.assert_not_equals(result, nil)
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, {{id = 1, name = 'Leo Tolstoy', age = 69, bucket_id = 477}})

    local result, err = g.router:call('crud.get', {
        'customers', {box.NULL, 1}, {mode = 'write'},
    })
    t.assert_equals(err, nil)
    t.assert_not_equals(result, nil)
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, {{id = 1, name = 'Leo Tolstoy', age = 69, bucket_id = 477}})
end

pgroup.test_upsert_object = function(g)
    local operations = {
        {'=', 'name', 'Leo Tolstoy'},
        {'+', 'age', 10},
    }

    --- replace part
    local result, err = g.router:call('crud.upsert_object', {
        'customers', {id = 1, name = 'Fedor', age = 59}, operations,
    })
    t.assert_equals(err, nil)
    t.assert_not_equals(result, nil)
    t.assert_equals(#result.rows, 0)

    local result, err = g.router:call('crud.get', {
        'customers', {box.NULL, 1}, {mode = 'write'},
    })
    t.assert_equals(err, nil)
    t.assert_not_equals(result, nil)
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, {{id = 1, name = 'Fedor', age = 59, bucket_id = 477}})

    --- update part
    local result, err = g.router:call('crud.upsert_object', {
        'customers', {id = 1, name = 'Fedor', age = 59}, operations,
    })
    t.assert_equals(err, nil)
    t.assert_not_equals(result, nil)
    t.assert_equals(#result.rows, 0)

    local result, err = g.router:call('crud.get', {
        'customers', {box.NULL, 1}, {mode = 'write'},
    })
    t.assert_equals(err, nil)
    t.assert_not_equals(result, nil)
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, {{id = 1, name = 'Leo Tolstoy', age = 69, bucket_id = 477}})
end

pgroup.test_upsert = function(g)
    local tuple = {1, box.NULL, 'Fedor', 59}
    local operations = {
        {'=', 'name', 'Leo Tolstoy'},
        {'+', 'age', 10},
    }

    --- replace part
    local result, err = g.router:call('crud.upsert', {
        'customers', tuple, operations,
    })
    t.assert_equals(err, nil)
    t.assert_not_equals(result, nil)
    t.assert_equals(#result.rows, 0)

    local result, err = g.router:call('crud.get', {
        'customers', {box.NULL, 1}, {mode = 'write'},
    })
    t.assert_equals(err, nil)
    t.assert_not_equals(result, nil)
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, {{id = 1, name = 'Fedor', age = 59, bucket_id = 477}})

    --- update part
    local result, err = g.router:call('crud.upsert', {
        'customers', tuple, operations,
    })
    t.assert_equals(err, nil)
    t.assert_not_equals(result, nil)
    t.assert_equals(#result.rows, 0)

    local result, err = g.router:call('crud.get', {
        'customers', {box.NULL, 1}, {mode = 'write'},
    })
    t.assert_equals(err, nil)
    t.assert_not_equals(result, nil)
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, {{id = 1, name = 'Leo Tolstoy', age = 69, bucket_id = 477}})
end
