local fiber = require('fiber')

local t = require('luatest')

local crud = require('crud')
local crud_utils = require('crud.common.utils')


local helpers = require('test.helper')
local read_scenario = require('test.integration.read_scenario')

local pgroup = t.group('select_readview', helpers.backend_matrix({
    {engine = 'memtx'},
}))

local function init_cluster(g)
    helpers.start_default_cluster(g, 'srv_select')

    g.space_format = g.cluster:server('s1-master').net_box.space.customers:format()

    g.router.net_box:eval([[
        require('crud').cfg{ stats = true }
    ]])
    g.router.net_box:eval([[
        require('crud.ratelimit').disable()
    ]])
end

pgroup.before_all(function(g)
    if (not helpers.tarantool_version_at_least(2, 11, 0))
    or (not require('luatest.tarantool').is_enterprise_package()) then
        t.skip('Readview is supported only for Tarantool Enterprise starting from v2.11.0')
    end
    init_cluster(g)
end)

pgroup.after_all(function(g)
    helpers.stop_cluster(g.cluster, g.params.backend)
end)

pgroup.before_each(function(g)
    helpers.truncate_space_on_cluster(g.cluster, 'customers')
    helpers.truncate_space_on_cluster(g.cluster, 'developers')
    helpers.truncate_space_on_cluster(g.cluster, 'cars')
end)

local function set_master(cluster, uuid, master_uuid)
    cluster.main_server:graphql({
        query = [[
            mutation(
                $uuid: String!
                $master_uuid: [String!]!
            ) {
                edit_replicaset(
                    uuid: $uuid
                    master: $master_uuid
                )
            }
        ]],
        variables = {uuid = uuid, master_uuid = {master_uuid}}
    })
end

pgroup.test_non_existent_space = function(g)
    local obj, err = g.router:eval([[
        local crud = require('crud')
        local foo, err = crud.readview({name = 'foo'})
        if err ~= nil then
            return nil, err
        end

        local result, err = foo:select('non_existent_space', nil, {fullscan=true})

        foo:close()
        return result, err
    ]])

    t.assert_equals(obj, nil)
    t.assert_str_contains(err.err, "Space \"non_existent_space\" doesn't exist")
end

pgroup.test_select_no_index = function(g)
    local obj, err = g.router:eval([[
        local crud = require('crud')
        local foo, err = crud.readview({name = 'foo'})
        if err ~= nil then
            return nil, err
        end
        local result, err = foo:select('no_index_space', nil, {fullscan=true})

        foo:close()
        return result, err
    ]])

    t.assert_equals(obj, nil)
    t.assert_str_contains(err.err, "Space \"no_index_space\" has no indexes, space should have primary index")
end

pgroup.test_invalid_value_type = function(g)
    local conditions = {
        {'=', 'id', 'not_number'}
    }

    local obj, err = g.router:eval([[
        local crud = require('crud')
        local conditions = ...

        local foo, err = crud.readview({name = 'foo'})
        if err ~= nil then
            return nil, err
        end
        local result, err = foo:select('customers', conditions)

        foo:close()

        return result, err
    ]], {conditions})

    t.assert_equals(obj, nil)
    t.assert_str_contains(err.err, "Supplied key type of part 0 does not match index part type: expected unsigned")
end

pgroup.test_gc_on_storage = function(g)
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
        }, {
            id = 4, name = "William", last_name = "White",
            age = 81, city = "Chicago",
        },
    })


    local _, err = g.router:eval([[
        local crud = require('crud')
        local foo, err = crud.readview({name = 'foo'})
        if err ~= nil then
            return nil, err
        end
        rawset(_G, 'foo', foo)
    ]])
    t.assert_equals(err, nil)

    helpers.call_on_storages(g.cluster, function(server)
        server.net_box:eval([[
        collectgarbage("collect")
        collectgarbage("collect")]])
    end)

    local obj, err = g.router:eval([[
        local crud = require('crud')
        local foo = rawget(_G, 'foo')
        local result, err = foo:select('customers', nil, {fullscan = true})

        foo:close()
        return result, err
    ]])

    t.assert_equals(err, nil)
    t.assert_equals(obj.rows, {
        {1, 477, "Elizabeth", "Jackson", 12, "New York"},
        {2, 401, "Mary", "Brown", 46, "Los Angeles"},
        {3, 2804, "David", "Smith", 33, "Los Angeles"},
        {4, 1161, "William", "White", 81, "Chicago"},
    })
end

pgroup.test_gc_rv_not_referenced_on_router = function(g)
    local _, err = g.router:eval([[
        local crud = require('crud')
        local foo, err = crud.readview({name = 'foo'})
        if err ~= nil then
            return nil, err
        end
        foo = nil
        collectgarbage("collect")
        collectgarbage("collect")
    ]])
    fiber.sleep(1)
    t.assert_equals(err, nil)
    local res = {}
    helpers.call_on_storages(g.cluster, function(server)
        local instance_res = server.net_box:eval([[
        return box.read_view.list()]])
        table.insert(res, instance_res)
    end, res)
    t.assert_equals(res, {{}, {}, {}, {}})

end

pgroup.test_gc_rv_referenced_on_router = function(g)
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
        }, {
            id = 4, name = "William", last_name = "White",
            age = 81, city = "Chicago",
        },
    })

    local _, err = g.router:eval([[
        local crud = require('crud')
        local foo, err = crud.readview({name = 'foo'})
        if err ~= nil then
            return nil, err
        end
        collectgarbage("collect")
        collectgarbage("collect")
        rawset(_G, 'foo', foo)
    ]])
    fiber.sleep(1)
    t.assert_equals(err, nil)
    local obj, err = g.router:eval([[
        local crud = require('crud')
        local foo = rawget(_G, 'foo')
        local result, err = foo:select('customers', nil, {fullscan = true})

        foo:close()
        return result, err
    ]])

    t.assert_equals(err, nil)
    t.assert_equals(obj.rows, {
        {1, 477, "Elizabeth", "Jackson", 12, "New York"},
        {2, 401, "Mary", "Brown", 46, "Los Angeles"},
        {3, 2804, "David", "Smith", 33, "Los Angeles"},
        {4, 1161, "William", "White", 81, "Chicago"},
    })
end

pgroup.test_close = function(g)
    local _, err = g.router:eval([[
        local crud = require('crud')
        local foo, err = crud.readview({name = 'foo'})
        if err ~= nil then
            return nil, err
        end
        foo:close()
    ]])
    t.assert_equals(err, nil)
    local res = {}
    helpers.call_on_storages(g.cluster, function(server)
        local instance_res = server.net_box:eval([[
        return box.read_view.list()]])
        table.insert(res, instance_res)
    end, res)
    t.assert_equals(res, {{}, {}, {}, {}})

end

pgroup.test_select_all = function(g)
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
        }, {
            id = 4, name = "William", last_name = "White",
            age = 81, city = "Chicago",
        },
    })

    local obj, err = g.router:eval([[
        local crud = require('crud')
        local foo, err = crud.readview({name = 'foo'})
        if err ~= nil then
            return nil, err
        end
        local result, err = foo:select('customers', nil, {fullscan = true})

        foo:close()
        return result, err
    ]])

    t.assert_equals(err, nil)
    t.assert_equals(obj.rows, {
        {1, 477, "Elizabeth", "Jackson", 12, "New York"},
        {2, 401, "Mary", "Brown", 46, "Los Angeles"},
        {3, 2804, "David", "Smith", 33, "Los Angeles"},
        {4, 1161, "William", "White", 81, "Chicago"},
    })

end

pgroup.test_select_with_same_name = function(g)
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

    local obj, err = g.router:eval([[
        local crud = require('crud')
        local foo, err = crud.readview({name = 'foo'})
        if err ~= nil then
            return nil, err
        end
        local boo, err = crud.readview({name = 'foo'})
        if err ~= nil then
            return nil, err
        end
        foo:close()

        local result, err = boo:select('customers', nil, {fullscan = true})

        boo:close()
        return result, err
    ]])

    t.assert_equals(err, nil)
    t.assert_equals(obj.rows, {
        {1, 477, "Elizabeth", "Jackson", 12, "New York"},
        {2, 401, "Mary", "Brown", 46, "Los Angeles"},
        {3, 2804, "David", "Smith", 33, "Los Angeles"},
        {4, 1161, "William", "White", 81, "Chicago"},
    })

end

pgroup.test_select_without_name = function(g)
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

    local obj, err = g.router:eval([[
        local crud = require('crud')
        local boo, err = crud.readview({name = nil})
        if err ~= nil then
            return nil, err
        end
        local result, err = boo:select('customers', nil, {fullscan = true})

        boo:close()
        return result, err
    ]])

    t.assert_equals(err, nil)
    t.assert_equals(obj.rows, {
        {1, 477, "Elizabeth", "Jackson", 12, "New York"},
        {2, 401, "Mary", "Brown", 46, "Los Angeles"},
        {3, 2804, "David", "Smith", 33, "Los Angeles"},
        {4, 1161, "William", "White", 81, "Chicago"},
    })

end

pgroup.test_select_with_insert = function(g)
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

    local obj, err = g.router:eval([[
        local crud = require('crud')
        local boo, err = crud.readview({name = 'foo'})
        if err ~= nil then
            return nil, err
        end
        rawset(_G, 'boo', boo)

        local result, err = boo:select('customers', nil, {fullscan = true})
        return result, err
    ]])

    t.assert_equals(err, nil)
    t.assert_equals(obj.rows, {
        {1, 477, "Elizabeth", "Jackson", 12, "New York"},
        {2, 401, "Mary", "Brown", 46, "Los Angeles"},
        {3, 2804, "David", "Smith", 33, "Los Angeles"},
        {4, 1161, "William", "White", 81, "Chicago"},
    })

    helpers.insert_objects(g, 'customers', {
        {
            id = 5, name = "Andrew", last_name = "White",
            age = 55, city = "Chicago"
        },
    })

    local obj, err = g.router:eval([[
        local crud = require('crud')
        local boo = rawget(_G, 'boo')

        local result, err = boo:select('customers', nil, {fullscan = true})
        boo:close()
        return result, err
    ]])
    t.assert_equals(err, nil)

    t.assert_equals(obj.rows, {
        {1, 477, "Elizabeth", "Jackson", 12, "New York"},
        {2, 401, "Mary", "Brown", 46, "Los Angeles"},
        {3, 2804, "David", "Smith", 33, "Los Angeles"},
        {4, 1161, "William", "White", 81, "Chicago"},
    })

end

pgroup.test_select_with_delete = function(g)
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

    local obj, err = g.router:eval([[
        local crud = require('crud')
        local boo, err = crud.readview({})
        if err ~= nil then
            return nil, err
        end
        rawset(_G, 'boo', boo)

        local result, err = boo:select('customers', nil, {fullscan = true})
        return result, err
    ]])

    t.assert_equals(err, nil)
    t.assert_equals(obj.rows, {
        {1, 477, "Elizabeth", "Jackson", 12, "New York"},
        {2, 401, "Mary", "Brown", 46, "Los Angeles"},
        {3, 2804, "David", "Smith", 33, "Los Angeles"},
        {4, 1161, "William", "White", 81, "Chicago"},
    })

    local _, err = g.router:call('crud.delete', {'customers', 3})
    t.assert_equals(err, nil)

    local obj, err = g.router:eval([[
        local crud = require('crud')
        local boo = rawget(_G, 'boo')

        local result, err = boo:select('customers', nil, {fullscan = true})
        boo:close()
        return result, err
    ]])
    t.assert_equals(err, nil)

    t.assert_equals(obj.rows, {
        {1, 477, "Elizabeth", "Jackson", 12, "New York"},
        {2, 401, "Mary", "Brown", 46, "Los Angeles"},
        {3, 2804, "David", "Smith", 33, "Los Angeles"},
        {4, 1161, "William", "White", 81, "Chicago"},
    })

end

pgroup.test_select_all_with_batch_size = function(g)
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
        }, {
            id = 5, name = "Jack", last_name = "Sparrow",
            age = 35, city = "London",
        }, {
            id = 6, name = "William", last_name = "Terner",
            age = 25, city = "Oxford",
        }, {
            id = 7, name = "Elizabeth", last_name = "Swan",
            age = 18, city = "Cambridge",
        }, {
            id = 8, name = "Hector", last_name = "Barbossa",
            age = 45, city = "London",
        },
    })

    table.sort(customers, function(obj1, obj2) return obj1.id < obj2.id end)

    -- batch size 1
    local result, err = g.router:eval([[
        local crud = require('crud')
        local foo, err = crud.readview({name = 'foo'})
        if err ~= nil then
            return nil, err
        end

        local result, err = foo:select('customers', nil, {batch_size=1, fullscan = true})

        foo:close()
        return result, err
    ]])
    t.assert_equals(err, nil)
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, customers)

    -- batch size 3
    local result, err = g.router:eval([[
        local crud = require('crud')
        local bar, err = crud.readview({name = 'bar'})
        if err ~= nil then
            return nil, err
        end
        local result, err = bar:select('customers', nil, {batch_size=3, fullscan = true})

        bar:close()
        return result, err
    ]])
    t.assert_equals(err, nil)
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, customers)
end

pgroup.test_eq_condition_with_index = function(g)
    local customers = helpers.insert_objects(g, 'customers', {
        {
            id = 1, name = "Elizabeth", last_name = "Jackson",
            age = 33, city = "New York",
        }, {
            id = 2, name = "Mary", last_name = "Brown",
            age = 46, city = "Los Angeles",
        }, {
            id = 3, name = "David", last_name = "Smith",
            age = 33, city = "Los Angeles",
        }, {
            id = 4, name = "William", last_name = "Smith",
            age = 81, city = "Chicago",
        },{
            id = 5, name = "Hector", last_name = "Barbossa",
            age = 33, city = "Chicago",
        },{
            id = 6, name = "William", last_name = "White",
            age = 81, city = "Chicago",
        },{
            id = 7, name = "Jack", last_name = "Sparrow",
            age = 33, city = "Chicago",
        }, {
            id = 8, name = "Nick", last_name = "Smith",
            age = 20, city = "London",
        }
    })

    table.sort(customers, function(obj1, obj2) return obj1.id < obj2.id end)

    local conditions = {
        {'==', 'age_index', 33},
    }

    -- no after
    local result, err = g.router:eval([[
        local crud = require('crud')

        local conditions = ...

        local foo, err = crud.readview({name = 'foo'})
        if err ~= nil then
            return nil, err
        end
        local result, err = foo:select('customers', conditions)

        foo:close()
        return result, err
    ]], {conditions})


    t.assert_equals(err, nil)
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, helpers.get_objects_by_idxs(customers, {1, 3, 5, 7})) -- in id order

    -- after obj 3
    local after = crud_utils.flatten(customers[3], g.space_format)
    local result, err = g.router:eval([[
        local crud = require('crud')

        local conditions, after = ...

        local foo, err = crud.readview({name = 'foo'})
        if err ~= nil then
            return nil, err
        end
        local result, err = foo:select('customers', conditions,{after=after})

        foo:close()
        return result, err
    ]], {conditions, after})

    t.assert_equals(err, nil)
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, helpers.get_objects_by_idxs(customers, {5, 7})) -- in id order

    -- after obj 5 with negative first
    local after = crud_utils.flatten(customers[5], g.space_format)
    local result, err = g.router:eval([[
        local crud = require('crud')

        local conditions, after = ...

        local foo, err = crud.readview({name = 'foo'})
        if err ~= nil then
            return nil, err
        end
        local result, err = foo:select('customers', conditions,{after=after, first = -10})

        foo:close()
        return result, err
    ]], {conditions, after})


    t.assert_equals(err, nil)
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, helpers.get_objects_by_idxs(customers, {1, 3})) -- in id order

    -- after obj 8
    local after = crud_utils.flatten(customers[8], g.space_format)
    local result, err = g.router:eval([[
        local crud = require('crud')

        local conditions, after = ...

        local foo, err = crud.readview({name = 'foo'})
        if err ~= nil then
            return nil, err
        end
        local result, err = foo:select('customers', conditions,{after=after, first = 10})

        foo:close()
        return result, err
    ]], {conditions, after})

    t.assert_equals(err, nil)
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, helpers.get_objects_by_idxs(customers, {1, 3, 5, 7})) -- in id order

    -- after obj 8 with negative first
    local after = crud_utils.flatten(customers[8], g.space_format)
    local result, err = g.router:eval([[
        local crud = require('crud')

        local conditions, after = ...

        local foo, err = crud.readview({name = 'foo'})
        if err ~= nil then
            return nil, err
        end
        local result, err = foo:select('customers', conditions,{after=after, first = -10})

        foo:close()
        return result, err
    ]], {conditions, after})

    t.assert_equals(err, nil)
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, helpers.get_objects_by_idxs(customers, {}))

    -- after obj 2
    local after = crud_utils.flatten(customers[2], g.space_format)
    local result, err = g.router:eval([[
        local crud = require('crud')

        local conditions, after = ...

        local foo, err = crud.readview({name = 'foo'})
        if err ~= nil then
            return nil, err
        end
        local result, err = foo:select('customers', conditions,{after=after, first = 10})

        foo:close()
        return result, err
    ]], {conditions, after})

    t.assert_equals(err, nil)
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, helpers.get_objects_by_idxs(customers, {}))

    -- after obj 2 with negative first
    local after = crud_utils.flatten(customers[2], g.space_format)
    local result, err = g.router:eval([[
        local crud = require('crud')

        local conditions, after = ...

        local foo, err = crud.readview({name = 'foo'})
        if err ~= nil then
            return nil, err
        end
        local result, err = foo:select('customers', conditions,{after=after, first = -10})

        foo:close()
        return result, err
    ]], {conditions, after})

    t.assert_equals(err, nil)
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, helpers.get_objects_by_idxs(customers, {1, 3, 5, 7})) -- in id order
end

pgroup.test_lt_condition_with_index = function(g)
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
        {'<', 'age_index', 33},
    }

    -- no after
    local result, err = g.router:eval([[
        local crud = require('crud')

        local conditions= ...

        local foo, err = crud.readview({name = 'foo'})
        if err ~= nil then
            return nil, err
        end
        local result, err = foo:select('customers', conditions,{fullscan=true})

        foo:close()
        return result, err
    ]], {conditions})

    t.assert_equals(err, nil)
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, helpers.get_objects_by_idxs(customers, {1})) -- in age order

    -- after obj 1
    local after = crud_utils.flatten(customers[1], g.space_format)
    local result, err = g.router:eval([[
        local crud = require('crud')

        local conditions, after = ...

        local foo, err = crud.readview({name = 'foo'})
        if err ~= nil then
            return nil, err
        end
        local result, err = foo:select('customers', conditions,{after=after, fullscan=true})

        foo:close()
        return result, err
    ]], {conditions, after})

    t.assert_equals(err, nil)
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, helpers.get_objects_by_idxs(customers, {})) -- in age order
end

pgroup.test_multiple_conditions = function(g)
    local customers = helpers.insert_objects(g, 'customers', {
        {
            id = 1, name = "Elizabeth", last_name = "Rodriguez",
            age = 20, city = "Los Angeles",
        }, {
            id = 2, name = "Elizabeth", last_name = "Rodriguez",
            age = 44, city = "Chicago",
        }, {
            id = 3, name = "Elizabeth", last_name = "Rodriguez",
            age = 22, city = "New York",
        }, {
            id = 4, name = "David", last_name = "Brown",
            age = 23, city = "Los Angeles",
        }, {
            id = 5, name = "Elizabeth", last_name = "Rodriguez",
            age = 39, city = "Chicago",
        }
    })

    table.sort(customers, function(obj1, obj2) return obj1.id < obj2.id end)

    local conditions = {
        {'>', 'age', 20},
        {'==', 'name', 'Elizabeth'},
        {'==', 'city', 'Chicago'},
    }

    -- no after
    local result, err = g.router:eval([[
        local crud = require('crud')

        local conditions= ...

        local foo, err = crud.readview({name = 'foo'})
        if err ~= nil then
            return nil, err
        end
        local result, err = foo:select('customers', conditions,{fullscan=true})

        foo:close()
        return result, err
    ]], {conditions})

    t.assert_equals(err, nil)
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, helpers.get_objects_by_idxs(customers, {5, 2})) -- in age order

    -- after obj 5
    local after = crud_utils.flatten(customers[5], g.space_format)
    local result, err = g.router:eval([[
        local crud = require('crud')

        local conditions, after = ...

        local foo, err = crud.readview({name = 'foo'})
        if err ~= nil then
            return nil, err
        end
        local result, err = foo:select('customers', conditions,{after=after, fullscan=true})

        foo:close()
        return result, err
    ]], {conditions, after})

    t.assert_equals(err, nil)
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, helpers.get_objects_by_idxs(customers, {2})) -- in age order
end

pgroup.test_composite_index = function(g)
    local customers = helpers.insert_objects(g, 'customers', {
        {
            id = 1, name = "Elizabeth", last_name = "Rodriguez",
            age = 20, city = "Los Angeles",
        }, {
            id = 2, name = "Elizabeth", last_name = "Johnson",
            age = 44, city = "Los Angeles",
        }, {
            id = 3, name = "David", last_name = "Brown",
            age = 23, city = "Chicago",
        }, {
            id = 4, name = "Jessica", last_name = "Jones",
            age = 22, city = "New York",
        }
    })

    table.sort(customers, function(obj1, obj2) return obj1.id < obj2.id end)

    local conditions = {
        {'>=', 'full_name', {"Elizabeth", "Jo"}},
    }

    -- no after
    local result, err = g.router:eval([[
        local crud = require('crud')

        local conditions= ...

        local foo, err = crud.readview({name = 'foo'})
        if err ~= nil then
            return nil, err
        end
        local result, err = foo:select('customers', conditions,{fullscan=true})

        foo:close()
        return result, err
    ]], {conditions})

    t.assert_equals(err, nil)
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, helpers.get_objects_by_idxs(customers, {2, 1, 4})) -- in full_name order

    -- after obj 2
    local after = crud_utils.flatten(customers[2], g.space_format)
    local result, err = g.router:eval([[
        local crud = require('crud')

        local conditions, after = ...

        local foo, err = crud.readview({name = 'foo'})
        if err ~= nil then
            return nil, err
        end
        local result, err = foo:select('customers', conditions,{after=after, fullscan=true})

        foo:close()
        return result, err
    ]], {conditions, after})

    t.assert_equals(err, nil)
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, helpers.get_objects_by_idxs(customers, {1, 4})) -- in full_name order

    -- partial value in conditions
    local conditions = {
        {'==', 'full_name', "Elizabeth"},
    }

    local result, err = g.router:eval([[
        local crud = require('crud')

        local conditions= ...

        local foo, err = crud.readview({name = 'foo'})
        if err ~= nil then
            return nil, err
        end
        local result, err = foo:select('customers', conditions,{fullscan=true})

        foo:close()
        return result, err
    ]], {conditions})

    t.assert_equals(err, nil)
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, helpers.get_objects_by_idxs(customers, {2, 1})) -- in full_name order

    -- first 1

    local result, err = g.router:eval([[
        local crud = require('crud')

        local conditions= ...

        local foo, err = crud.readview({name = 'foo'})
        if err ~= nil then
            return nil, err
        end
        local result, err = foo:select('customers', conditions,{first=1})

        foo:close()
        return result, err
    ]], {conditions})
    t.assert_equals(err, nil)
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, helpers.get_objects_by_idxs(customers, {2})) -- in full_name order

    -- first 1 with full specified key
    local result, err = g.router:eval([[
        local crud = require('crud')

        local conditions= ...

        local foo, err = crud.readview({name = 'foo'})
        if err ~= nil then
            return nil, err
        end
        local result, err = foo:select('customers', {{'==', 'full_name', {'Elizabeth', 'Johnson'}}}, {first = 1})

        foo:close()
        return result, err
    ]], {conditions})
    t.assert_equals(err, nil)
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, helpers.get_objects_by_idxs(customers, {2})) -- in full_name order
end

pgroup.test_composite_primary_index = function(g)
    local book_translation = helpers.insert_objects(g, 'book_translation', {
        {
            id = 5,
            language = 'Ukrainian',
            edition = 55,
            translator = 'Mitro Dmitrienko',
            comments = 'Translation 55',
        }
    })
    t.assert_equals(#book_translation, 1)

    local conditions = {{'=', 'id', {5, 'Ukrainian', 55}}}

    local result, err = g.router:eval([[
        local crud = require('crud')

        local conditions= ...

        local foo, err = crud.readview({name = 'foo'})
        if err ~= nil then
            return nil, err
        end
        local result, err = foo:select('book_translation', conditions)

        foo:close()
        return result, err
    ]], {conditions})
    t.assert_equals(err, nil)
    t.assert_equals(#result.rows, 1)

    local result, err = g.router:eval([[
        local crud = require('crud')

        local conditions= ...

        local foo, err = crud.readview({name = 'foo'})
        if err ~= nil then
            return nil, err
        end
        local result, err = foo:select('book_translation', conditions, {first = 2})

        foo:close()
        return result, err
    ]], {conditions})
    t.assert_equals(err, nil)
    t.assert_equals(#result.rows, 1)

    local result, err = g.router:eval([[
        local crud = require('crud')

        local conditions= ...

        local foo, err = crud.readview({name = 'foo'})
        if err ~= nil then
            return nil, err
        end
        local result, err = foo:select('book_translation', conditions, {first = 1})

        foo:close()
        return result, err
    ]], {conditions})
    t.assert_equals(err, nil)
    t.assert_equals(#result.rows, 1)

    local result, err = g.router:eval([[
        local crud = require('crud')

        local conditions, after = ...

        local foo, err = crud.readview({name = 'foo'})
        if err ~= nil then
            return nil, err
        end
        local result, err = foo:select('book_translation', conditions, {first = 1, after = after})

        foo:close()
        return result, err
    ]], {conditions, result.rows[1]})
    t.assert_equals(err, nil)
    t.assert_equals(#result.rows, 0)
end

pgroup.test_select_with_batch_size_1 = function(g)
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
        }, {
            id = 5, name = "Jack", last_name = "Sparrow",
            age = 35, city = "London",
        }, {
            id = 6, name = "William", last_name = "Terner",
            age = 25, city = "Oxford",
        }, {
            id = 7, name = "Elizabeth", last_name = "Swan",
            age = 18, city = "Cambridge",
        }, {
            id = 8, name = "Hector", last_name = "Barbossa",
            age = 45, city = "London",
        },
    })

    table.sort(customers, function(obj1, obj2) return obj1.id < obj2.id end)

    -- LE
    local conditions = {{'<=', 'age', 35}}
    local result, err = g.router:eval([[
        local crud = require('crud')

        local conditions= ...

        local foo, err = crud.readview({name = 'foo'})
        if err ~= nil then
            return nil, err
        end
        local result, err = foo:select('customers', conditions,{batch_size=1, fullscan = true})

        foo:close()
        return result, err
    ]], {conditions})

    t.assert_equals(err, nil)
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, helpers.get_objects_by_idxs(customers, {5, 3, 6, 7, 1}))

    -- LT
    local conditions = {{'<', 'age', 35}}
    local result, err = g.router:eval([[
        local crud = require('crud')

        local conditions= ...

        local foo, err = crud.readview({name = 'foo'})
        if err ~= nil then
            return nil, err
        end
        local result, err = foo:select('customers', conditions,{batch_size=1, fullscan = true})

        foo:close()
        return result, err
    ]], {conditions})

    t.assert_equals(err, nil)
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, helpers.get_objects_by_idxs(customers, {3, 6, 7, 1}))

    -- GE
    local conditions = {{'>=', 'age', 35}}
    local result, err = g.router:eval([[
        local crud = require('crud')

        local conditions= ...

        local foo, err = crud.readview({name = 'foo'})
        if err ~= nil then
            return nil, err
        end
        local result, err = foo:select('customers', conditions,{batch_size=1, fullscan = true})

        foo:close()
        return result, err
    ]], {conditions})

    t.assert_equals(err, nil)
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, helpers.get_objects_by_idxs(customers, {5, 8, 2, 4}))

    -- GT
    local conditions = {{'>', 'age', 35}}
    local result, err = g.router:eval([[
        local crud = require('crud')

        local conditions= ...

        local foo, err = crud.readview({name = 'foo'})
        if err ~= nil then
            return nil, err
        end
        local result, err = foo:select('customers', conditions,{batch_size=1, fullscan = true})

        foo:close()
        return result, err
    ]], {conditions})

    t.assert_equals(err, nil)
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, helpers.get_objects_by_idxs(customers, {8, 2, 4}))
end

pgroup.test_select_by_full_sharding_key = function(g)
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

    local conditions = {{'==', 'id', 3}}
    local result, err = g.router:eval([[
        local crud = require('crud')

        local conditions= ...

        local foo, err = crud.readview({name = 'foo'})
        if err ~= nil then
            return nil, err
        end
        local result, err = foo:select('customers', conditions)

        foo:close()
        return result, err
    ]], {conditions})

    t.assert_equals(err, nil)
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, helpers.get_objects_by_idxs(customers, {3}))
end

pgroup.test_select_with_collations = function(g)
    local customers = helpers.insert_objects(g, 'customers', {
        {
            id = 1, name = "Elizabeth", last_name = "Jackson",
            age = 12, city = "Oxford",
        }, {
            id = 2, name = "Mary", last_name = "Brown",
            age = 46, city = "oxford",
        }, {
            id = 3, name = "elizabeth", last_name = "brown",
            age = 46, city = "Oxford",
        }, {
            id = 4, name = "Jack", last_name = "Sparrow",
            age = 35, city = "oxford",
        }, {
            id = 5, name = "William", last_name = "Terner",
            age = 25, city = "Oxford",
        }, {
            id = 6, name = "elizabeth", last_name = "Brown",
            age = 33, city = "Los Angeles",
        },
    })

    table.sort(customers, function(obj1, obj2) return obj1.id < obj2.id end)

    -- full name index - unicode ci collation (case-insensitive)
    local conditions = {{'==', 'name', "Elizabeth"}}
    local result, err = g.router:eval([[
        local crud = require('crud')

        local conditions= ...

        local foo, err = crud.readview({name = 'foo'})
        if err ~= nil then
            return nil, err
        end
        local result, err = foo:select('customers', conditions)

        foo:close()
        return result, err
    ]], {conditions})

    t.assert_equals(err, nil)
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, helpers.get_objects_by_idxs(customers, {3, 6, 1}))

    -- city - no collation (case-sensitive)
    local conditions = {{'==', 'city', "oxford"}}
    local result, err = g.router:eval([[
        local crud = require('crud')

        local conditions= ...

        local foo, err = crud.readview({name = 'foo'})
        if err ~= nil then
            return nil, err
        end
        local result, err = foo:select('customers', conditions)

        foo:close()
        return result, err
    ]], {conditions})

    t.assert_equals(err, nil)
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, helpers.get_objects_by_idxs(customers, {2, 4}))
end

pgroup.test_multipart_primary_index = function(g)
    local coords = helpers.insert_objects(g, 'coord', {
        { x = 0, y = 0 }, -- 1
        { x = 0, y = 1 }, -- 2
        { x = 0, y = 2 }, -- 3
        { x = 1, y = 3 }, -- 4
        { x = 1, y = 4 }, -- 5
    })

    local conditions = {{'=', 'primary', 0}}
    local result_0, err = g.router:eval([[
        local crud = require('crud')

        local conditions= ...

        local foo, err = crud.readview({name = 'foo'})
        if err ~= nil then
            return nil, err
        end
        local result, err = foo:select('coord', conditions)

        foo:close()
        return result, err
    ]], {conditions})
    t.assert_equals(err, nil)
    local objects = crud.unflatten_rows(result_0.rows, result_0.metadata)
    t.assert_equals(objects, helpers.get_objects_by_idxs(coords, {1, 2, 3}))

    local result, err = g.router:eval([[
        local crud = require('crud')

        local conditions, after= ...

        local foo, err = crud.readview({name = 'foo'})
        if err ~= nil then
            return nil, err
        end
        local result, err = foo:select('coord', conditions,{after = after})

        foo:close()
        return result, err
    ]], {conditions, result_0.rows[1]})

    t.assert_equals(err, nil)
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, helpers.get_objects_by_idxs(coords, {2, 3}))

    local result, err = g.router:eval([[
        local crud = require('crud')

        local conditions, after= ...

        local foo, err = crud.readview({name = 'foo'})
        if err ~= nil then
            return nil, err
        end
        local result, err = foo:select('coord', conditions,{after = after, first = -2})

        foo:close()
        return result, err
    ]], {conditions, result_0.rows[3]})
    t.assert_equals(err, nil)
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, helpers.get_objects_by_idxs(coords, {1, 2}))

    local new_conditions = {{'=', 'y', 1}, {'=', 'primary', 0}}
    local result, err = g.router:eval([[
        local crud = require('crud')

        local conditions, after= ...

        local foo, err = crud.readview({name = 'foo'})
        if err ~= nil then
            return nil, err
        end
        local result, err = foo:select('coord', conditions,{after = after, first = -2})

        foo:close()
        return result, err
    ]], {new_conditions, result_0.rows[3]})
    t.assert_equals(err, nil)
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, helpers.get_objects_by_idxs(coords, {2}))

    local conditions = {{'=', 'primary', {0, 2}}}
    local result, err = g.router:eval([[
        local crud = require('crud')

        local conditions, after= ...

        local foo, err = crud.readview({name = 'foo'})
        if err ~= nil then
            return nil, err
        end
        local result, err = foo:select('coord', conditions)

        foo:close()
        return result, err
    ]], {conditions, result_0.rows[3]})
    t.assert_equals(err, nil)
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, helpers.get_objects_by_idxs(coords, {3}))

    local conditions_ge = {{'>=', 'primary', 0}}
    local result_ge_0, err = g.router:eval([[
        local crud = require('crud')

        local conditions= ...

        local foo, err = crud.readview({name = 'foo'})
        if err ~= nil then
            return nil, err
        end
        local result, err = foo:select('coord', conditions,{fullscan=true})

        foo:close()
        return result, err
    ]], {conditions_ge})
    t.assert_equals(err, nil)
    local objects = crud.unflatten_rows(result_ge_0.rows, result_ge_0.metadata)
    t.assert_equals(objects, helpers.get_objects_by_idxs(coords, {1, 2, 3, 4, 5}))

    local result, err = g.router:eval([[
        local crud = require('crud')

        local conditions, after= ...

        local foo, err = crud.readview({name = 'foo'})
        if err ~= nil then
            return nil, err
        end
        local result, err = foo:select('coord', conditions,{after = after,fullscan = true})

        foo:close()
        return result, err
    ]], {conditions_ge, result_ge_0.rows[1]})
    t.assert_equals(err, nil)
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, helpers.get_objects_by_idxs(coords, {2, 3, 4, 5}))

    local result, err = g.router:eval([[
        local crud = require('crud')

        local conditions, after= ...

        local foo, err = crud.readview({name = 'foo'})
        if err ~= nil then
            return nil, err
        end
        local result, err = foo:select('coord', conditions,{after = after,first = -3})

        foo:close()
        return result, err
    ]], {conditions_ge, result_ge_0.rows[3]})
    t.assert_equals(err, nil)
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, helpers.get_objects_by_idxs(coords, {1, 2}))
end

pgroup.test_select_partial_result_bad_input = function(g)
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
        }, {
            id = 4, name = "William", last_name = "White",
            age = 81, city = "Chicago",
        },
    })

    local conditions = {{'>=', 'age', 33}}
    local result, err = g.router:eval([[
        local crud = require('crud')

        local conditions= ...

        local foo, err = crud.readview({name = 'foo'})
        if err ~= nil then
            return nil, err
        end
        local result, err = foo:select('customers', conditions,{fields = {'id', 'mame'}, fullscan = true})

        foo:close()
        return result, err
    ]], {conditions})

    t.assert_equals(result, nil)
    t.assert_str_contains(err.err, 'Space format doesn\'t contain field named "mame"')
end

pgroup.test_select_partial_result = function(g)
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

    local result, err = g.router:eval([[
        local crud = require('crud')

        local conditions, fields= ...

        local foo, err = crud.readview({name = 'foo'})
        if err ~= nil then
            return nil, err
        end
        local result, err = foo:select('customers', conditions,{fields = fields, fullscan = true})

        foo:close()
        return result, err
    ]], {conditions, fields})

    t.assert_equals(err, nil)
    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, expected_customers)

    -- same case with after option
    expected_customers = {
        {id = 2, age = 46, name = "Mary", city = "London"},
        {id = 4, age = 46, name = "William", city = "Chicago"},
    }

    local result, err = g.router:eval([[
        local crud = require('crud')

        local conditions, fields, after= ...

        local foo, err = crud.readview({name = 'foo'})
        if err ~= nil then
            return nil, err
        end
        local result, err = foo:select('customers', conditions,{fields = fields, after = after, fullscan = true})

        foo:close()
        return result, err
    ]], {conditions, fields, result.rows[1]})

    t.assert_equals(err, nil)
    objects = crud.unflatten_rows(result.rows, result.metadata)
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

    local result, err = g.router:eval([[
        local crud = require('crud')

        local conditions, fields= ...

        local foo, err = crud.readview({name = 'foo'})
        if err ~= nil then
            return nil, err
        end
        local result, err = foo:select('customers', conditions,{fields = fields, fullscan = true})

        foo:close()
        return result, err
    ]], {conditions, fields})

    t.assert_equals(err, nil)
    objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, expected_customers)

    -- same case with after option
    expected_customers = {
        {id = 2, age = 46, name = "Mary"},
        {id = 4, age = 46, name = "William"},
    }

    local result, err = g.router:eval([[
        local crud = require('crud')

        local conditions, fields, after= ...

        local foo, err = crud.readview({name = 'foo'})
        if err ~= nil then
            return nil, err
        end
        local result, err = foo:select('customers', conditions,{fields = fields, after = after, fullscan = true})

        foo:close()
        return result, err
    ]], {conditions, fields, result.rows[1]})

    t.assert_equals(err, nil)
    objects = crud.unflatten_rows(result.rows, result.metadata)
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

    local result, err = g.router:eval([[
        local crud = require('crud')

        local conditions, fields= ...

        local foo, err = crud.readview({name = 'foo'})
        if err ~= nil then
            return nil, err
        end
        local result, err = foo:select('customers', conditions,{fields = fields, fullscan = true})

        foo:close()
        return result, err
    ]], {conditions, fields})

    t.assert_equals(err, nil)
    objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, expected_customers)

    -- same case with after option
    expected_customers = {
        {id = 2, name = "Mary", age = 46},
        {id = 3, name = "David", age = 33},
    }

    local result, err = g.router:eval([[
        local crud = require('crud')

        local conditions, fields, after= ...

        local foo, err = crud.readview({name = 'foo'})
        if err ~= nil then
            return nil, err
        end
        local result, err = foo:select('customers', conditions,{fields = fields, after = after, fullscan = true})

        foo:close()
        return result, err
    ]], {conditions, fields, result.rows[1]})


    t.assert_equals(err, nil)
    objects = crud.unflatten_rows(result.rows, result.metadata)
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

    local result, err = g.router:eval([[
        local crud = require('crud')

        local conditions, fields= ...

        local foo, err = crud.readview({name = 'foo'})
        if err ~= nil then
            return nil, err
        end
        local result, err = foo:select('customers', conditions,{fields = fields, fullscan = true})

        foo:close()
        return result, err
    ]], {conditions, fields})

    t.assert_equals(err, nil)
    objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, expected_customers)

    -- same case with after option
    expected_customers = {
        {id = 2, name = "Mary", city = "London"},
        {id = 3, name = "David", city = "Los Angeles"},
    }

    local result, err = g.router:eval([[
        local crud = require('crud')

        local conditions, fields, after= ...

        local foo, err = crud.readview({name = 'foo'})
        if err ~= nil then
            return nil, err
        end
        local result, err = foo:select('customers', conditions,{fields = fields, after = after, fullscan = true})

        foo:close()
        return result, err
    ]], {conditions, fields, result.rows[1]})

    t.assert_equals(err, nil)
    objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, expected_customers)
end

pgroup.test_select_force_map_call = function(g)
    local key = 1

    local first_bucket_id = g.router:eval([[
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

    local result, err = g.router:eval([[
        local crud = require('crud')

        local foo, err = crud.readview({name = 'foo'})
        if err ~= nil then
            return nil, err
        end
        local result, err = foo:select('customers', {{'==', 'id', 1}})

        foo:close()
        return result, err
    ]])

    t.assert_equals(err, nil)
    t.assert_equals(#result.rows, 1)

    local result, err = g.router:eval([[
        local crud = require('crud')

        local foo, err = crud.readview({name = 'foo'})
        if err ~= nil then
            return nil, err
        end
        local result, err = foo:select('customers', {{'==', 'id', 1}}, {force_map_call = true})

        foo:close()
        return result, err
    ]])

    t.assert_equals(err, nil)

    local objects = crud.unflatten_rows(result.rows, result.metadata)
    table.sort(objects, function(obj1, obj2) return obj1.bucket_id < obj2.bucket_id end)
    t.assert_equals(objects, customers)
end

pgroup.test_jsonpath = function(g)
    helpers.insert_objects(g, 'developers', {
        {
            id = 1, name = "Alexey", last_name = "Smith",
            age = 20, additional = { a = { b = 140 } },
        }, {
            id = 2, name = "Sergey", last_name = "Choppa",
            age = 21, additional = { a = { b = 120 } },
        }, {
            id = 3, name = "Mikhail", last_name = "Crossman",
            age = 42, additional = {},
        }, {
            id = 4, name = "Pavel", last_name = "White",
            age = 51, additional = { a = { b = 50 } },
        }, {
            id = 5, name = "Tatyana", last_name = "May",
            age = 17, additional = { a = 55 },
        },
    })
    local result, err = g.router:eval([[
        local crud = require('crud')

        local foo, err = crud.readview({name = 'foo'})
        if err ~= nil then
            return nil, err
        end
        local result, err = foo:select('developers', {{'>=', '[5]', 40}},
         {fields = {'name', 'last_name'}, fullscan = true})

        foo:close()
        return result, err
    ]])
    t.assert_equals(err, nil)

    local objects = crud.unflatten_rows(result.rows, result.metadata)
    local expected_objects = {
        {id = 3, name = "Mikhail", last_name = "Crossman"},
        {id = 4, name = "Pavel", last_name = "White"},
    }
    t.assert_equals(objects, expected_objects)
    local result, err = g.router:eval([[
        local crud = require('crud')

        local foo, err = crud.readview({name = 'foo'})
        if err ~= nil then
            return nil, err
        end
        local result, err = foo:select('developers', {{'<', '["age"]', 21}},
         {fields = {'name', 'last_name'}, fullscan = true})

        foo:close()
        return result, err
    ]])
    t.assert_equals(err, nil)

    local objects = crud.unflatten_rows(result.rows, result.metadata)
    local expected_objects = {
        {id = 1, name = "Alexey", last_name = "Smith"},
        {id = 5, name = "Tatyana", last_name = "May"},
    }
    t.assert_equals(objects, expected_objects)
end

pgroup.test_jsonpath_index_field = function(g)
    t.skip_if(
        not crud_utils.tarantool_supports_jsonpath_indexes(),
        "Jsonpath indexes supported since 2.6.3/2.7.2/2.8.1"
    )

    helpers.insert_objects(g, 'cars', {
        {
            id = {car_id = {signed = 1}},
            age = 2,
            manufacturer = 'VAG',
            data = {car = { model = 'BMW', color = 'Black' }},
        },
        {
            id = {car_id = {signed = 2}},
            age = 5,
            manufacturer = 'FIAT',
            data = {car = { model = 'Cadillac', color = 'White' }},
        },
        {
            id = {car_id = {signed = 3}},
            age = 17,
            manufacturer = 'Ford',
            data = {car = { model = 'BMW', color = 'Yellow' }},
        },
        {
            id = {car_id = {signed = 4}},
            age = 3,
            manufacturer = 'General Motors',
            data = {car = { model = 'Mercedes', color = 'Yellow' }},
        },
    })

    -- PK jsonpath index
    local result, err = g.router:eval([[
        local crud = require('crud')

        local foo, err = crud.readview({name = 'foo'})
        if err ~= nil then
            return nil, err
        end
        local result, err = foo:select('cars', {{'<=', 'id_ind', 3}, {'<=', 'age', 5}},
         {fields = {'id', 'age'}, fullscan = true})

        foo:close()
        return result, err
    ]])
    t.assert_equals(err, nil)

    local objects = crud.unflatten_rows(result.rows, result.metadata)
    local expected_objects = {
    {
        id = {car_id = {signed = 2}},
        age = 5,
    },
    {
        id = {car_id = {signed = 1}},
        age = 2,
    }}

    t.assert_equals(objects, expected_objects)

    -- Secondary jsonpath index (partial)
    local result, err = g.router:eval([[
        local crud = require('crud')

        local foo, err = crud.readview({name = 'foo'})
        if err ~= nil then
            return nil, err
        end
        local result, err = foo:select('cars', {{'==', 'data_index', 'Yellow'}}, {fields = {'id', 'age'}})

        foo:close()
        return result, err
    ]])
    t.assert_equals(err, nil)

    local objects = crud.unflatten_rows(result.rows, result.metadata)
    local expected_objects = {
    {
        id = {car_id = {signed = 3}},
        age = 17,
        data = {car = { model = 'BMW', color = 'Yellow' }},
    },
    {
        id = {car_id = {signed = 4}},
        age = 3,
        data = {car = { model = 'Mercedes', color = 'Yellow' }}
    }}

    t.assert_equals(objects, expected_objects)

    -- Secondary jsonpath index (full)
    local result, err = g.router:eval([[
        local crud = require('crud')

        local foo, err = crud.readview({name = 'foo'})
        if err ~= nil then
            return nil, err
        end
        local result, err = foo:select('cars', {{'==', 'data_index', {'Yellow', 'Mercedes'}}}, {fields = {'id', 'age'}})

        foo:close()
        return result, err
    ]])
    t.assert_equals(err, nil)

    local objects = crud.unflatten_rows(result.rows, result.metadata)
    local expected_objects = {
    {
        id = {car_id = {signed = 4}},
        age = 3,
        data = {car = { model = 'Mercedes', color = 'Yellow' }}
    }}

    t.assert_equals(objects, expected_objects)
end

pgroup.test_jsonpath_index_field_pagination = function(g)
    t.skip_if(
        not crud_utils.tarantool_supports_jsonpath_indexes(),
        "Jsonpath indexes supported since 2.6.3/2.7.2/2.8.1"
    )

    local cars = helpers.insert_objects(g, 'cars', {
        {
            id = {car_id = {signed = 1}},
            age = 5,
            manufacturer = 'VAG',
            data = {car = { model = 'A', color = 'Yellow' }},
        },
        {
            id = {car_id = {signed = 2}},
            age = 17,
            manufacturer = 'FIAT',
            data = {car = { model = 'B', color = 'Yellow' }},
        },
        {
            id = {car_id = {signed = 3}},
            age = 5,
            manufacturer = 'Ford',
            data = {car = { model = 'C', color = 'Yellow' }},
        },
        {
            id = {car_id = {signed = 4}},
            age = 3,
            manufacturer = 'General Motors',
            data = {car = { model = 'D', color = 'Yellow' }},
        },
        {
            id = {car_id = {signed = 5}},
            age = 3,
            manufacturer = 'General Motors',
            data = {car = { model = 'E', color = 'Yellow' }},
        },
        {
            id = {car_id = {signed = 6}},
            age = 3,
            manufacturer = 'General Motors',
            data = {car = { model = 'F', color = 'Yellow' }},
        },
        {
            id = {car_id = {signed = 7}},
            age = 3,
            manufacturer = 'General Motors',
            data = {car = { model = 'G', color = 'Yellow' }},
        },
        {
            id = {car_id = {signed = 8}},
            age = 3,
            manufacturer = 'General Motors',
            data = {car = { model = 'H', color = 'Yellow' }},
        },
    })


    -- Pagination (primary index)
    local result, err = g.router:eval([[
        local crud = require('crud')

        local foo, err = crud.readview({name = 'foo'})
        if err ~= nil then
            return nil, err
        end
        rawset(_G, 'foo', foo)

        local result, err = foo:select('cars', nil, {first = 2})

        return result, err
    ]])

    t.assert_equals(err, nil)

    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, helpers.get_objects_by_idxs(cars, {1, 2}))
    local result, err = g.router:eval([[
        local crud = require('crud')

        local foo = rawget(_G, 'foo')
        local after = ...

        local result, err = foo:select('cars', nil, {first = 2, after = after})
        return result, err
    ]], {result.rows[2]})

    t.assert_equals(err, nil)

    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, helpers.get_objects_by_idxs(cars, {3, 4}))

    -- Reverse pagination (primary index)
    local result, err = g.router:eval([[
        local crud = require('crud')

        local foo = rawget(_G, 'foo')
        local after = ...

        local result, err = foo:select('cars', nil, {first = -2, after = after})

        return result, err
    ]], {result.rows[1]})
    t.assert_equals(err, nil)

    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, helpers.get_objects_by_idxs(cars, {1, 2}))

    -- Pagination (secondary index - 1 field)
    local result, err = g.router:eval([[
        local crud = require('crud')

        local foo = rawget(_G, 'foo')

        local result, err = foo:select('cars', {{'==', 'data_index', 'Yellow'}}, {first = 2})

        foo:close()
        return result, err
    ]])
    t.assert_equals(err, nil)

    local objects = crud.unflatten_rows(result.rows, result.metadata)
    t.assert_equals(objects, helpers.get_objects_by_idxs(cars, {1, 2}))
end

pgroup.test_select_timeout = function(g)
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

    local result, err = g.router:eval([[
        local crud = require('crud')

        local foo, err = crud.readview({name = 'foo'})
        if err ~= nil then
            return nil, err
        end
        local result, err = foo:select('customers', nil, {timeout = 1, fullscan = true})

        foo:close()
        return result, err
    ]])

    t.assert_equals(err, nil)
    t.assert_equals(result.rows, {
        {1, 477, "Elizabeth", "Jackson", 12, "New York"},
        {2, 401, "Mary", "Brown", 46, "Los Angeles"},
        {3, 2804, "David", "Smith", 33, "Los Angeles"},
        {4, 1161, "William", "White", 81, "Chicago"},
    })
end

-- gh-220: bucket_id argument is ignored when it cannot be deduced
-- from provided select/pairs conditions.
pgroup.test_select_no_map_reduce = function(g)
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

    local router = g.router.net_box
    local map_reduces_before = helpers.get_map_reduces_stat(router, 'customers')

    -- Case: no conditions, just bucket id.
    local result, err = g.router:eval([[
        local crud = require('crud')

        local foo, err = crud.readview({name = 'foo'})
        if err ~= nil then
            return nil, err
        end
        local result, err = foo:select('customers', nil, {bucket_id = 2804, timeout = 1, fullscan = true})

        foo:close()
        return result, err
    ]])
    t.assert_equals(err, nil)
    t.assert_equals(result.rows, {
        {3, 2804, 'David', 'Smith', 33, 'Los Angeles'},
    })

    local map_reduces_after_1 = helpers.get_map_reduces_stat(router, 'customers')
    local diff_1 = map_reduces_after_1 - map_reduces_before
    t.assert_equals(diff_1, 0, 'Select request was not a map reduce')

    -- Case: EQ on secondary index, which is not in the sharding
    -- index (primary index in the case).
    local result, err = g.router:eval([[
        local crud = require('crud')

        local foo, err = crud.readview({name = 'foo'})
        if err ~= nil then
            return nil, err
        end
        local result, err = foo:select( 'customers', {{'==', 'age', 81}}, {bucket_id = 1161, timeout = 1})

        foo:close()
        return result, err
    ]])
    t.assert_equals(err, nil)
    t.assert_equals(result.rows, {
        {4, 1161, 'William', 'White', 81, 'Chicago'},
    })

    local map_reduces_after_2 = helpers.get_map_reduces_stat(router, 'customers')
    local diff_2 = map_reduces_after_2 - map_reduces_after_1
    t.assert_equals(diff_2, 0, 'Select request was not a map reduce')
end

pgroup.test_select_yield_every_0 = function(g)
    local resp, err = g.router:eval([[
        local crud = require('crud')

        local foo, err = crud.readview({name = 'foo'})
        if err ~= nil then
            return nil, err
        end
        local result, err = foo:select('customers', nil, { yield_every = 0, fullscan = true })

        foo:close()
        return result, err
    ]])
    t.assert_equals(resp, nil)
    t.assert_str_contains(err.err, "yield_every should be > 0")
end

pgroup.test_stop_select = function(g)
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
        }, {
            id = 4, name = "William", last_name = "White",
            age = 81, city = "Chicago",
        },
    })

    local _, err = g.router:eval([[
        local crud = require('crud')
        local foo = crud.readview({name = 'foo'})
        rawset(_G, 'foo', foo)
    ]])

    t.assert_equals(err, nil)

    g.cluster:server('s2-master'):stop()
    local _, err = g.router:eval([[
        local crud = require('crud')
        local foo = rawget(_G, 'foo', foo)
        local result, err = foo:select('customers', nil, {fullscan = true})
        return result, err
    ]])
    t.assert_error(err.err)
    g.cluster:server('s2-master'):start()

    if g.params.backend == helpers.backend.VSHARD then
        local bootstrap_key
        if type(g.params.backend_cfg) == 'table'
        and g.params.backend_cfg.identification_mode == 'name_as_key' then
            bootstrap_key = 'name'
        else
            bootstrap_key = 'uuid'
        end

        g.cluster:server('s2-master'):exec(function(cfg, bootstrap_key)
            require('vshard.storage').cfg(cfg, box.info[bootstrap_key])
            require('crud').init_storage()
        end, {g.cfg, bootstrap_key})
    end

    local _, err = g.router:eval([[
        local crud = require('crud')
        local foo = rawget(_G, 'foo', foo)
        foo:close()
        return nil, nil
    ]])
    t.assert_equals(err, nil)
end

pgroup.after_test('test_stop_select', function(g)
    -- It seems more easy to restart the cluster rather then restore it
    -- original state.
    helpers.stop_cluster(g.cluster, g.params.backend)
    g.cluster = nil
    init_cluster(g)
end)

pgroup.test_select_switch_master = function(g)
    helpers.skip_not_cartridge_backend(g.params.backend)

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
        }, {
            id = 4, name = "William", last_name = "White",
            age = 81, city = "Chicago",
        },
    })

    local _, err = g.router:eval([[
        local crud = require('crud')
        local temp, err = crud.readview({name = 'temp'})
        if err ~= nil then
            return nil, err
        end
        rawset(_G, 'temp', temp)
        return nil, err
    ]])
    t.assert_equals(err, nil)

    local replicasets = helpers.get_test_cartridge_replicasets()
    set_master(g.cluster, replicasets[2].uuid, replicasets[2].servers[2].instance_uuid)

    local obj, err = g.router:eval([[
        local crud = require('crud')
        local temp = rawget(_G, 'temp')
        local result, err = temp:select('customers', nil, {fullscan = true})

        temp:close()
        return result, err
    ]])

    t.assert_equals(err, nil)
    t.assert_equals(obj.rows, {
        {1, 477, "Elizabeth", "Jackson", 12, "New York"},
        {2, 401, "Mary", "Brown", 46, "Los Angeles"},
        {3, 2804, "David", "Smith", 33, "Los Angeles"},
        {4, 1161, "William", "White", 81, "Chicago"},
    })

end

pgroup.after_test('test_select_switch_master', function(g)
    local replicasets = helpers.get_test_cartridge_replicasets()
    set_master(g.cluster, replicasets[2].uuid, replicasets[2].servers[1].instance_uuid)
end)

-- TODO: https://github.com/tarantool/crud/issues/383
pgroup.test_select_switch_master_first = function(g)
    helpers.skip_not_cartridge_backend(g.params.backend)

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

    local obj, err = g.router:eval([[
        local crud = require('crud')
        local temp, err = crud.readview({name = 'temp'})
        if err ~= nil then
            return nil, err
        end
        local result, err = temp:select('customers', nil, {first = 2})
        rawset(_G, 'temp', temp)
        return result, err
    ]])
    t.assert_equals(err, nil)
    local objects = crud.unflatten_rows(obj.rows, obj.metadata)
    t.assert_equals(objects, helpers.get_objects_by_idxs(customers, {1, 2}))

    local replicasets = helpers.get_test_cartridge_replicasets()
    set_master(g.cluster, replicasets[3].uuid, replicasets[3].servers[2].instance_uuid)

    local obj, err = g.router:eval([[
        local crud = require('crud')
        local temp = rawget(_G, 'temp')
        local result, err = temp:select('customers', nil, {fullscan = true})

        temp:close()
        return result, err
    ]])

    t.assert_equals(err, nil)
    t.assert_equals(obj.rows, {
        {1, 477, "Elizabeth", "Jackson", 12, "New York"},
        {2, 401, "Mary", "Brown", 46, "Los Angeles"},
        {3, 2804, "David", "Smith", 33, "Los Angeles"},
        {4, 1161, "William", "White", 81, "Chicago"},
    })

end

pgroup.after_test('test_select_switch_master', function(g)
    local replicasets = helpers.get_test_cartridge_replicasets()
    set_master(g.cluster, replicasets[2].uuid, replicasets[2].servers[1].instance_uuid)
end)

-- TODO: https://github.com/tarantool/crud/issues/383
pgroup.test_select_closed_readview = function(g)
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
        }, {
            id = 4, name = "William", last_name = "White",
            age = 81, city = "Chicago",
        },
    })

    local _, err = g.router:eval([[
        local crud = require('crud')
        local temp, err = crud.readview({name = 'temp'})
        if err ~= nil then
            return nil, err
        end
        temp.opened = false

        local result, err = temp:select('customers', nil, {fullscan = true})

        temp.opened = true
        temp:close()
        return result, err
    ]])

    t.assert_str_contains(err.str, 'Read view is closed')
end

local function read_impl(cg, space, conditions, opts)
    return cg.router:exec(function(space, conditions, opts)
        opts = table.deepcopy(opts) or {}
        opts.fullscan = true

        local crud = require('crud')
        local rv, err = crud.readview()
        t.assert_equals(err, nil)

        local resp, err = rv:select(space, conditions, opts)
        rv:close()

        if err ~= nil then
            return nil, err
        end

        return crud.unflatten_rows(resp.rows, resp.metadata), nil
    end, {space, conditions, opts})
end

pgroup.test_gh_418_select_with_secondary_noneq_index_condition = function(g)
    read_scenario.gh_418_read_with_secondary_noneq_index_condition(g, read_impl)
end

local gh_373_types_cases = helpers.merge_tables(
    read_scenario.gh_373_read_with_decimal_condition_cases,
    read_scenario.gh_373_read_with_datetime_condition_cases,
    read_scenario.gh_373_read_with_interval_condition_cases
)

for case_name_template, case in pairs(gh_373_types_cases) do
    local case_name = 'test_' .. case_name_template:format('select')

    pgroup[case_name] = function(g)
        case(g, read_impl)
    end
end

pgroup.before_test(
    'test_select_merger_process_storage_error',
    read_scenario.before_merger_process_storage_error
)

pgroup.test_select_merger_process_storage_error = function(g)
    read_scenario.merger_process_storage_error(g, read_impl)
end

pgroup.after_test(
    'test_select_merger_process_storage_error',
    read_scenario.after_merger_process_storage_error
)

for case_name_template, case in pairs(read_scenario.gh_422_nullability_cases) do
    local case_name = 'test_' .. case_name_template:format('select')

    pgroup[case_name] = function(g)
        case(g, read_impl)
    end
end
