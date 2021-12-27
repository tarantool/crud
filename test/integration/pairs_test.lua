local fio = require('fio')

local t = require('luatest')

local crud_utils = require('crud.common.utils')

local helpers = require('test.helper')
local storage_stat = require('test.helpers.storage_stat')

local pgroup = t.group('pairs', {
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

    helpers.call_on_storages(g.cluster, function(server)
        server.net_box:eval([[
            local storage_stat = require('test.helpers.storage_stat')
            storage_stat.init_on_storage_for_select()
        ]])
    end)
end)

pgroup.after_all(function(g) helpers.stop_cluster(g.cluster) end)

pgroup.before_each(function(g)
    helpers.truncate_space_on_cluster(g.cluster, 'customers')
end)


pgroup.test_pairs_no_conditions = function(g)
    local customers = helpers.insert_objects(g, 'customers', {
        {
            id = 1, name = "Elizabeth", last_name = "Jackson",
            age = 12, city = "New York",
        }, {
            id = 2, name = "Mary", last_name = "Brown",
            age = 46, city = "Los Angeles",
        }, {
            id = 3, name = "David", last_name = "Smith",
            age = 33, city = "Los Angeles",
        }, {
            id = 4, name = "William", last_name = "White",
            age = 81, city = "Chicago",
        },
    })

    table.sort(customers, function(obj1, obj2) return obj1.id < obj2.id end)

    local raw_rows = {
        {1, 477, 'Elizabeth', 'Jackson', 12, 'New York'},
        {2, 401, 'Mary', 'Brown', 46, 'Los Angeles'},
        {3, 2804, 'David', 'Smith', 33, 'Los Angeles'},
        {4, 1161, 'William', 'White', 81, 'Chicago'},
    }

    -- without conditions and options
    local objects = g.cluster.main_server.net_box:eval([[
        local crud = require('crud')

        local objects = {}
        for _, object in crud.pairs('customers') do
            table.insert(objects, object)
        end

        return objects
    ]])
    t.assert_equals(objects, raw_rows)

    -- with use_tomap=false (the raw tuples returned)
    local objects = g.cluster.main_server.net_box:eval([[
        local crud = require('crud')

        local objects = {}
        for _, object in crud.pairs('customers', nil, {use_tomap = false}) do
            table.insert(objects, object)
        end

        return objects
    ]])
    t.assert_equals(objects, raw_rows)

    -- no after
    local objects = g.cluster.main_server.net_box:eval([[
        local crud = require('crud')

        local objects = {}
        for _, object in crud.pairs('customers', nil, {use_tomap = true}) do
            table.insert(objects, object)
        end

        return objects
    ]])

    t.assert_equals(objects, customers)

    -- after obj 2
    local after = crud_utils.flatten(customers[2], g.space_format)
    local objects, err = g.cluster.main_server.net_box:eval([[
        local crud = require('crud')

        local after = ...

        local objects = {}
        for _, object in crud.pairs('customers', nil, {after = after, use_tomap = true}) do
            table.insert(objects, object)
        end

        return objects
    ]], {after})

    t.assert_equals(err, nil)
    t.assert_equals(objects, helpers.get_objects_by_idxs(customers, {3, 4}))

    -- after obj 4 (last)
    local after = crud_utils.flatten(customers[4], g.space_format)
    local objects, err = g.cluster.main_server.net_box:eval([[
        local crud = require('crud')

        local after = ...

        local objects = {}
        for _, object in crud.pairs('customers', nil, {after = after, use_tomap = true}) do
            table.insert(objects, object)
        end

        return objects
    ]], {after})

    t.assert_equals(err, nil)
    t.assert_equals(#objects, 0)
end

pgroup.test_ge_condition_with_index = function(g)
    local customers = helpers.insert_objects(g, 'customers', {
        {
            id = 1, name = "Elizabeth", last_name = "Jackson",
            age = 12, city = "New York",
        }, {
            id = 2, name = "Mary", last_name = "Brown",
            age = 46, city = "Los Angeles",
        }, {
            id = 3, name = "David", last_name = "Smith",
            age = 33, city = "Los Angeles",
        }, {
            id = 4, name = "William", last_name = "White",
            age = 81, city = "Chicago",
        },
    })

    table.sort(customers, function(obj1, obj2) return obj1.id < obj2.id end)

    local conditions = {
        {'>=', 'age', 33},
    }

    -- no after
    local objects, err = g.cluster.main_server.net_box:eval([[
        local crud = require('crud')

        local conditions = ...

        local objects = {}
        for _, object in crud.pairs('customers', conditions, {use_tomap = true}) do
            table.insert(objects, object)
        end

        return objects
    ]], {conditions})

    t.assert_equals(err, nil)
    t.assert_equals(objects, helpers.get_objects_by_idxs(customers, {3, 2, 4})) -- in age order

    -- after obj 3
    local after = crud_utils.flatten(customers[3], g.space_format)
    local objects, err = g.cluster.main_server.net_box:eval([[
        local crud = require('crud')

        local conditions, after = ...

        local objects = {}
        for _, object in crud.pairs('customers', conditions, {after = after, use_tomap = true}) do
            table.insert(objects, object)
        end

        return objects
    ]], {conditions, after})

    t.assert_equals(err, nil)
    t.assert_equals(objects, helpers.get_objects_by_idxs(customers, {2, 4})) -- in age order
end

pgroup.test_le_condition_with_index = function(g)
    local customers = helpers.insert_objects(g, 'customers', {
        {
            id = 1, name = "Elizabeth", last_name = "Jackson",
            age = 12, city = "New York",
        }, {
            id = 2, name = "Mary", last_name = "Brown",
            age = 46, city = "Los Angeles",
        }, {
            id = 3, name = "David", last_name = "Smith",
            age = 33, city = "Los Angeles",
        }, {
            id = 4, name = "William", last_name = "White",
            age = 81, city = "Chicago",
        },
    })

    table.sort(customers, function(obj1, obj2) return obj1.id < obj2.id end)

    local conditions = {
        {'<=', 'age', 33},
    }

    -- no after
    local objects, err = g.cluster.main_server.net_box:eval([[
        local crud = require('crud')

        local conditions = ...

        local objects = {}
        for _, object in crud.pairs('customers', conditions, {use_tomap = true}) do
            table.insert(objects, object)
        end

        return objects
    ]], {conditions})

    t.assert_equals(err, nil)
    t.assert_equals(objects, helpers.get_objects_by_idxs(customers, {3, 1})) -- in age order

    -- after obj 3
    local after = crud_utils.flatten(customers[3], g.space_format)
    local objects, err = g.cluster.main_server.net_box:eval([[
        local crud = require('crud')

        local conditions, after = ...

        local objects = {}
        for _, object in crud.pairs('customers', conditions, {after = after, use_tomap = true}) do
            table.insert(objects, object)
        end

        return objects
    ]], {conditions, after})

    t.assert_equals(err, nil)
    t.assert_equals(objects, helpers.get_objects_by_idxs(customers, {1})) -- in age order
end

pgroup.test_first = function(g)
    local customers = helpers.insert_objects(g, 'customers', {
        {
            id = 1, name = "Elizabeth", last_name = "Jackson",
            age = 12, city = "New York",
        }, {
            id = 2, name = "Mary", last_name = "Brown",
            age = 46, city = "Los Angeles",
        }, {
            id = 3, name = "David", last_name = "Smith",
            age = 33, city = "Los Angeles",
        },
    })

    table.sort(customers, function(obj1, obj2) return obj1.id < obj2.id end)

    -- w/ tomap
    local objects, err = g.cluster.main_server.net_box:eval([[
        local crud = require('crud')
        local objects = {}
        for _, object in crud.pairs('customers', nil, {first = 2, use_tomap = true}) do
            table.insert(objects, object)
        end
        return objects
    ]])
    t.assert_equals(err, nil)
    t.assert_equals(objects, helpers.get_objects_by_idxs(customers, {1, 2}))

    local tuples, err = g.cluster.main_server.net_box:eval([[
        local crud = require('crud')
        local tuples = {}
        for _, tuple in crud.pairs('customers', nil, {first = 2}) do
            table.insert(tuples, tuple)
        end
        return tuples
    ]])
    t.assert_equals(err, nil)
    t.assert_equals(tuples, {
        {1, 477, 'Elizabeth', 'Jackson', 12, 'New York'},
        {2, 401, 'Mary', 'Brown', 46, 'Los Angeles'},
    })
end

pgroup.test_negative_first = function(g)
    local customers = helpers.insert_objects(g, 'customers',{
        {
            id = 1, name = "Elizabeth", last_name = "Jackson",
            age = 12, city = "New York",
        }, {
            id = 2, name = "Mary", last_name = "Brown",
            age = 46, city = "Los Angeles",
        }, {
            id = 3, name = "David", last_name = "Smith",
            age = 33, city = "Los Angeles",
        },
    })

    table.sort(customers, function(obj1, obj2) return obj1.id < obj2.id end)

    -- negative first
    t.assert_error_msg_contains("Negative first isn't allowed for pairs", function()
        g.cluster.main_server.net_box:eval([[
            local crud = require('crud')
            crud.pairs('customers', nil, {first = -10})
        ]])
    end)
end

pgroup.test_empty_space = function(g)
    local count = g.cluster.main_server.net_box:eval([[
        local crud = require('crud')
        local count = 0
        for _, object in crud.pairs('customers') do
            count = count + 1
        end
        return count
    ]])
    t.assert_equals(count, 0)
end

pgroup.test_luafun_compatibility = function(g)
    helpers.insert_objects(g, 'customers', {
        {
            id = 1, name = "Elizabeth", last_name = "Jackson",
            age = 12, city = "New York",
        }, {
            id = 2, name = "Mary", last_name = "Brown",
            age = 46, city = "Los Angeles",
        }, {
            id = 3, name = "David", last_name = "Smith",
            age = 33, city = "Los Angeles",
        },
    })
    local count = g.cluster.main_server.net_box:eval([[
        local crud = require('crud')
        local count = crud.pairs('customers'):map(function() return 1 end):sum()
        return count
    ]])
    t.assert_equals(count, 3)

    count = g.cluster.main_server.net_box:eval([[
        local crud = require('crud')
        local count = crud.pairs('customers',
            {use_tomap = true}):map(function() return 1 end):sum()
        return count
    ]])
    t.assert_equals(count, 3)
end

pgroup.test_pairs_partial_result = function(g)
    helpers.insert_objects(g, 'customers', {
        {
            id = 1, name = "Elizabeth", last_name = "Jackson",
            age = 12, city = "Los Angeles",
        }, {
            id = 2, name = "Mary", last_name = "Brown",
            age = 46, city = "London",
        }, {
            id = 3, name = "David", last_name = "Smith",
            age = 33, city = "Los Angeles",
        }, {
            id = 4, name = "William", last_name = "White",
            age = 46, city = "Chicago",
        },
    })

    -- condition by indexed non-unique non-primary field (age):
    local conditions = {{'>=', 'age', 33}}

    -- condition field is not in opts.fields
    local fields = {'name', 'city'}

    -- result doesn't contain primary key, result tuples are sorted by field+primary
    -- in age + id order
    local expected_customers = {
        {id = 3, age = 33, name = "David", city = "Los Angeles"},
        {id = 2, age = 46, name = "Mary", city = "London"},
        {id = 4, age = 46, name = "William", city = "Chicago"},
    }

    local objects = g.cluster.main_server.net_box:eval([[
        local crud = require('crud')

        local conditions, fields = ...

        local objects = {}
        for _, object in crud.pairs('customers', conditions, {use_tomap = true, fields = fields}) do
            table.insert(objects, object)
        end

        return objects
    ]], {conditions, fields})
    t.assert_equals(objects, expected_customers)

    -- same case with after option
    expected_customers = {
        {id = 2, age = 46, name = "Mary", city = "London"},
        {id = 4, age = 46, name = "William", city = "Chicago"},
    }

    objects = g.cluster.main_server.net_box:eval([[
        local crud = require('crud')

        local conditions, fields = ...

        local tuples = {}
        for _, tuple in crud.pairs('customers', conditions, {fields = fields}) do
            table.insert(tuples, tuple)
        end

        local objects = {}
        for _, object in crud.pairs('customers', conditions, {after = tuples[1], use_tomap = true, fields = fields}) do
            table.insert(objects, object)
        end

        return objects
    ]], {conditions, fields})
    t.assert_equals(objects, expected_customers)

    -- condition field is in opts.fields
    fields = {'name', 'age'}

    -- result doesn't contain primary key, result tuples are sorted by field+primary
    -- in age + id order
    expected_customers = {
        {id = 3, age = 33, name = "David"},
        {id = 2, age = 46, name = "Mary"},
        {id = 4, age = 46, name = "William"},
    }

    objects = g.cluster.main_server.net_box:eval([[
        local crud = require('crud')

        local conditions, fields = ...

        local objects = {}
        for _, object in crud.pairs('customers', conditions, {use_tomap = true, fields = fields}) do
            table.insert(objects, object)
        end

        return objects
    ]], {conditions, fields})
    t.assert_equals(objects, expected_customers)

    -- same case with after option
    expected_customers = {
        {id = 2, age = 46, name = "Mary"},
        {id = 4, age = 46, name = "William"},
    }

    objects = g.cluster.main_server.net_box:eval([[
        local crud = require('crud')

        local conditions, fields = ...

        local tuples = {}
        for _, tuple in crud.pairs('customers', conditions, {fields = fields}) do
            table.insert(tuples, tuple)
        end

        local objects = {}
        for _, object in crud.pairs('customers', conditions, {after = tuples[1], use_tomap = true, fields = fields}) do
            table.insert(objects, object)
        end

        return objects
    ]], {conditions, fields})
    t.assert_equals(objects, expected_customers)

    -- condition by non-indexed non-unique non-primary field (city):
    conditions = {{'>=', 'city', 'Lo'}}

    -- condition field is not in opts.fields
    fields = {'name', 'age'}

    -- result doesn't contain primary key, result tuples are sorted by primary
    -- in id order
    expected_customers = {
        {id = 1, name = "Elizabeth", age = 12},
        {id = 2, name = "Mary", age = 46},
        {id = 3, name = "David", age = 33},
    }

    objects = g.cluster.main_server.net_box:eval([[
        local crud = require('crud')

        local conditions, fields = ...

        local objects = {}
        for _, object in crud.pairs('customers', conditions, {use_tomap = true, fields = fields}) do
            table.insert(objects, object)
        end

        return objects
    ]], {conditions, fields})
    t.assert_equals(objects, expected_customers)

    -- same case with after option
    expected_customers = {
        {id = 2, name = "Mary", age = 46},
        {id = 3, name = "David", age = 33},
    }

    objects = g.cluster.main_server.net_box:eval([[
        local crud = require('crud')

        local conditions, fields = ...

        local tuples = {}
        for _, tuple in crud.pairs('customers', conditions, {fields = fields}) do
            table.insert(tuples, tuple)
        end

        local objects = {}
        for _, object in crud.pairs('customers', conditions, {after = tuples[1], use_tomap = true, fields = fields}) do
            table.insert(objects, object)
        end

        return objects
    ]], {conditions, fields})
    t.assert_equals(objects, expected_customers)

    -- condition field is in opts.fields
    fields = {'name', 'city'}

    -- result doesn't contain primary key, result tuples are sorted by primary
    -- in id order
    expected_customers = {
        {id = 1, name = "Elizabeth", city = "Los Angeles"},
        {id = 2, name = "Mary", city = "London"},
        {id = 3, name = "David", city = "Los Angeles"},
    }

    objects = g.cluster.main_server.net_box:eval([[
        local crud = require('crud')

        local conditions, fields = ...

        local objects = {}
        for _, object in crud.pairs('customers', conditions, {use_tomap = true, fields = fields}) do
            table.insert(objects, object)
        end

        return objects
    ]], {conditions, fields})
    t.assert_equals(objects, expected_customers)

    -- same case with after option
    expected_customers = {
        {id = 2, name = "Mary", city = "London"},
        {id = 3, name = "David", city = "Los Angeles"},
    }

    objects = g.cluster.main_server.net_box:eval([[
        local crud = require('crud')

        local conditions, fields = ...

        local tuples = {}
        for _, tuple in crud.pairs('customers', conditions, {fields = fields}) do
            table.insert(tuples, tuple)
        end

        local objects = {}
        for _, object in crud.pairs('customers', conditions, {after = tuples[1], use_tomap = true, fields = fields}) do
            table.insert(objects, object)
        end

        return objects
    ]], {conditions, fields})
    t.assert_equals(objects, expected_customers)
end

pgroup.test_pairs_cut_result = function(g)
    helpers.insert_objects(g, 'customers', {
        {
            id = 1, name = "Elizabeth", last_name = "Jackson",
            age = 12, city = "Los Angeles",
        }, {
            id = 2, name = "Mary", last_name = "Brown",
            age = 46, city = "London",
        }, {
            id = 3, name = "David", last_name = "Smith",
            age = 33, city = "Los Angeles",
        }, {
            id = 4, name = "William", last_name = "White",
            age = 46, city = "Chicago",
        },
    })

    -- condition by indexed non-unique non-primary field (age):
    local conditions = {{'>=', 'age', 33}}

    -- condition field is not in opts.fields
    local fields = {'name', 'city'}

    -- result doesn't contain primary key, result tuples are sorted by field+primary
    -- in age + id order
    local expected_customers = {
        {name = "David", city = "Los Angeles"},
        {name = "Mary", city = "London"},
        {name = "William", city = "Chicago"},
    }

    local objects = g.cluster.main_server.net_box:eval([[
        local crud = require('crud')

        local conditions, fields = ...

        local objects = {}
        for _, object in crud.pairs('customers', conditions, {use_tomap = true, fields = fields}) do
            table.insert(objects, object)
        end

        return crud.cut_objects(objects, fields)
    ]], {conditions, fields})
    t.assert_equals(objects, expected_customers)

    -- without use_tomap
    expected_customers = {
        {"David", "Los Angeles"},
        {"Mary", "London"},
        {"William", "Chicago"},
    }

    local tuples = g.cluster.main_server.net_box:eval([[
        local crud = require('crud')

        local conditions, fields = ...

        local tuples = {}
        for _, tuple in crud.pairs('customers', conditions, {fields = fields}) do
            table.insert(tuples, tuple)
        end

        return crud.cut_rows(tuples, nil, fields)
    ]], {conditions, fields})
     t.assert_equals(tuples.metadata, nil)
    t.assert_equals(tuples.rows, expected_customers)
end

pgroup.test_pairs_force_map_call = function(g)
    local key = 1

    local first_bucket_id = g.cluster.main_server.net_box:eval([[
        local vshard = require('vshard')

        local key = ...
        return vshard.router.bucket_id_strcrc32(key)
    ]], {key})

    local second_bucket_id, err = helpers.get_other_storage_bucket_id(g.cluster, first_bucket_id)

    t.assert_equals(err, nil)

    local customers = helpers.insert_objects(g, 'customers', {
        {
            id = key, bucket_id = first_bucket_id, name = "Elizabeth", last_name = "Jackson",
            age = 12, city = "New York",
        }, {
            id = key, bucket_id = second_bucket_id, name = "Mary", last_name = "Brown",
            age = 46, city = "Los Angeles",
        },
    })

    table.sort(customers, function(obj1, obj2) return obj1.bucket_id < obj2.bucket_id end)

    local conditions = {{'==', 'id', key}}

    local objects = g.cluster.main_server.net_box:eval([[
        local crud = require('crud')

        local conditions = ...

        local objects = {}
        for _, object in crud.pairs('customers', conditions, {use_tomap = true}) do
            table.insert(objects, object)
        end

        return objects
    ]], {conditions})

    t.assert_equals(err, nil)
    t.assert_equals(#objects, 1)

    objects = g.cluster.main_server.net_box:eval([[
        local crud = require('crud')

        local conditions = ...

        local objects = {}
        for _, object in crud.pairs('customers', conditions, {use_tomap = true, force_map_call = true}) do
            table.insert(objects, object)
        end

        return objects
    ]], {conditions})
    table.sort(objects, function(obj1, obj2) return obj1.bucket_id < obj2.bucket_id end)
    t.assert_equals(objects, customers)
end

pgroup.test_pairs_timeout = function(g)
    local customers = helpers.insert_objects(g, 'customers', {
        {
            id = 1, name = "Elizabeth", last_name = "Jackson",
            age = 12, city = "New York",
        }, {
            id = 2, name = "Mary", last_name = "Brown",
            age = 46, city = "Los Angeles",
        }, {
            id = 3, name = "David", last_name = "Smith",
            age = 33, city = "Los Angeles",
        }, {
            id = 4, name = "William", last_name = "White",
            age = 81, city = "Chicago",
        },
    })

    table.sort(customers, function(obj1, obj2) return obj1.id < obj2.id end)

    local raw_rows = {
        {1, 477, 'Elizabeth', 'Jackson', 12, 'New York'},
        {2, 401, 'Mary', 'Brown', 46, 'Los Angeles'},
        {3, 2804, 'David', 'Smith', 33, 'Los Angeles'},
        {4, 1161, 'William', 'White', 81, 'Chicago'},
    }

    local objects = g.cluster.main_server.net_box:eval([[
        local crud = require('crud')

        local objects = {}
        for _, object in crud.pairs('customers', nil, {timeout = 1}) do
            table.insert(objects, object)
        end

        return objects
    ]])
    t.assert_equals(objects, raw_rows)
end

pgroup.test_opts_not_damaged = function(g)
    local customers = helpers.insert_objects(g, 'customers', {
        {
            -- bucket_id is 477, storage is s-2
            id = 1, name = "Elizabeth", last_name = "Jackson",
            age = 12, city = "Los Angeles",
        }, {
            -- bucket_id is 401, storage is s-2
            id = 2, name = "Mary", last_name = "Brown",
            age = 46, city = "London",
        }, {
            -- bucket_id is 2804, storage is s-1
            id = 3, name = "David", last_name = "Smith",
            age = 33, city = "Los Angeles",
        }, {
            -- bucket_id is 1161, storage is s-2
            id = 4, name = "William", last_name = "White",
            age = 46, city = "Chicago",
        },
    })

    table.sort(customers, function(obj1, obj2) return obj1.id < obj2.id end)

    local expected_customers = {
        {id = 4, name = "William", age = 46},
    }

    -- after tuple should be in `fields` format + primary key
    local fields = {'name', 'age'}
    local after = {"Mary", 46, 2}

    local pairs_opts = {
        timeout = 1, bucket_id = 1161,
        batch_size = 105, first = 2, after = after,
        fields = fields, mode = 'read', prefer_replica = false,
        balance = false, force_map_call = false, use_tomap = true,
    }
    local new_pairs_opts, objects = g.cluster.main_server:eval([[
         local crud = require('crud')

         local pairs_opts = ...

         local objects = {}
         for _, object in crud.pairs('customers', nil, pairs_opts) do
             table.insert(objects, object)
         end

         return pairs_opts, objects
     ]], {pairs_opts})

    t.assert_equals(objects, expected_customers)
    t.assert_equals(new_pairs_opts, pairs_opts)
end

-- gh-220: bucket_id argument is ignored when it cannot be deduced
-- from provided select/pairs conditions.
pgroup.test_pairs_no_map_reduce = function(g)
    local customers = helpers.insert_objects(g, 'customers', {
        {
            -- bucket_id is 477, storage is s-2
            id = 1, name = 'Elizabeth', last_name = 'Jackson',
            age = 12, city = 'New York',
        }, {
            -- bucket_id is 401, storage is s-2
            id = 2, name = 'Mary', last_name = 'Brown',
            age = 46, city = 'Los Angeles',
        }, {
            -- bucket_id is 2804, storage is s-1
            id = 3, name = 'David', last_name = 'Smith',
            age = 33, city = 'Los Angeles',
        }, {
            -- bucket_id is 1161, storage is s-2
            id = 4, name = 'William', last_name = 'White',
            age = 81, city = 'Chicago',
        },
    })

    table.sort(customers, function(obj1, obj2) return obj1.id < obj2.id end)

    local stat_a = storage_stat.collect(g.cluster)

    -- Case: no conditions, just bucket id.
    local rows = g.cluster.main_server.net_box:eval([[
        local crud = require('crud')

        return crud.pairs(...):totable()
    ]], {
        'customers',
        nil,
        {bucket_id = 2804, timeout = 1},
    })
    t.assert_equals(rows, {
        {3, 2804, 'David', 'Smith', 33, 'Los Angeles'},
    })

    local stat_b = storage_stat.collect(g.cluster)
    t.assert_equals(storage_stat.diff(stat_b, stat_a), {
        ['s-1'] = {
            requests = 1,
        },
        ['s-2'] = {
            requests = 0,
        },
    })

    -- Case: EQ on secondary index, which is not in the sharding
    -- index (primary index in the case).
    local rows = g.cluster.main_server.net_box:eval([[
        local crud = require('crud')

        return crud.pairs(...):totable()
    ]], {
        'customers',
        {{'==', 'age', 81}},
        {bucket_id = 1161, timeout = 1},
    })
    t.assert_equals(rows, {
        {4, 1161, 'William', 'White', 81, 'Chicago'},
    })

    local stat_c = storage_stat.collect(g.cluster)
    t.assert_equals(storage_stat.diff(stat_c, stat_b), {
        ['s-1'] = {
            requests = 0,
        },
        ['s-2'] = {
            requests = 1,
        },
    })
end
