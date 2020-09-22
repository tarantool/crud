local fio = require('fio')

local t = require('luatest')
local g = t.group('select')

local helpers = require('test.helper')

math.randomseed(os.time())

g.before_all = function()
    g.cluster = helpers.Cluster:new({
        datadir = fio.tempdir(),
        server_command = helpers.entrypoint('srv_select'),
        use_vshard = true,
        replicasets = {
            {
                uuid = helpers.uuid('a'),
                alias = 'router',
                roles = { 'vshard-router' },
                servers = {
                    { instance_uuid = helpers.uuid('a', 1), alias = 'router' },
                },
            },
            {
                uuid = helpers.uuid('b'),
                alias = 's-1',
                roles = { 'customers-storage' },
                servers = {
                    { instance_uuid = helpers.uuid('b', 1), alias = 's1-master' },
                    { instance_uuid = helpers.uuid('b', 2), alias = 's1-replica' },
                },
            },
            {
                uuid = helpers.uuid('c'),
                alias = 's-2',
                roles = { 'customers-storage' },
                servers = {
                    { instance_uuid = helpers.uuid('c', 1), alias = 's2-master' },
                    { instance_uuid = helpers.uuid('c', 2), alias = 's2-replica' },
                },
            }
        },
    })
    g.cluster:start()
end

g.after_all = function()
    g.cluster:stop()
    fio.rmtree(g.cluster.datadir)
end

g.before_each(function()
    for _, server in ipairs(g.cluster.servers) do
        server.net_box:eval([[
            local space = box.space.customers
            if space ~= nil and not box.cfg.read_only then
                space:truncate()
            end
        ]])
    end
end)

local function insert_customers(customers)
    local inserted_objects = {}

    for _, customer in ipairs(customers) do
        local obj, err = g.cluster.main_server.net_box:eval([[
            local crud = require('crud')
            return crud.insert('customers', ...)
        ]],{customer})

        t.assert_equals(err, nil)

        table.insert(inserted_objects, obj)
    end

    return inserted_objects
end

local function get_by_ids(customers, ids)
    local results = {}
    for _, id in ipairs(ids) do
        table.insert(results, customers[id])
    end
    return results
end

g.test_non_existent_space = function()
    -- insert
    local obj, err = g.cluster.main_server.net_box:eval([[
        local crud = require('crud')
        return crud.select('non_existent_space')
    ]])

    t.assert_equals(obj, nil)
    t.assert_str_contains(err.err, "Space non_existent_space doesn't exists")
end

g.test_select_all = function()
    local customers = insert_customers({
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

    -- no after
    local objects, err = g.cluster.main_server.net_box:eval([[
        local crud = require('crud')

        local iter, err = crud.select('customers', nil)
        if err ~= nil then return nil, err end

        local objects = {}
        while iter:has_next() do
            object, err = iter:get()
            if err ~= nil then
                return nil, err
            end
            table.insert(objects, object)
        end

        return objects
    ]])

    t.assert_equals(err, nil)
    t.assert_equals(objects, customers)

    -- after obj 2
    local after = customers[2]
    local objects, err = g.cluster.main_server.net_box:eval([[
        local crud = require('crud')

        local iter, err = crud.select('customers', nil, {
            after = ...,
        })
        if err ~= nil then return nil, err end

        local objects = {}
        while iter:has_next() do
            object, err = iter:get()
            if err ~= nil then
                return nil, err
            end
            table.insert(objects, object)
        end

        return objects
    ]], {after}
)

    t.assert_equals(err, nil)
    t.assert_equals(objects, get_by_ids(customers, {3, 4}))

    -- after obj 4 (last)
    local after = customers[4]
    local objects, err = g.cluster.main_server.net_box:eval([[
        local crud = require('crud')

        local iter, err = crud.select('customers', nil, {
            after = ...,
        })
        if err ~= nil then return nil, err end

        local objects = {}
        while iter:has_next() do
            object, err = iter:get()
            if err ~= nil then
                return nil, err
            end
            table.insert(objects, object)
        end

        return objects
    ]], {after})

    t.assert_equals(err, nil)
    t.assert_equals(#objects, 0)
end

g.test_select_all_with_limit = function()
    local customers = insert_customers({
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

    -- limit 2
    local objects, err = g.cluster.main_server.net_box:eval([[
        local crud = require('crud')

        local iter, err = crud.select('customers', nil, {
            limit = 2,
        })
        if err ~= nil then return nil, err end

        local objects = {}
        while iter:has_next() do
            object, err = iter:get()
            if err ~= nil then
                return nil, err
            end
            table.insert(objects, object)
        end

        return objects
    ]])

    t.assert_equals(err, nil)
    t.assert_equals(objects, get_by_ids(customers, {1, 2}))

    -- limit 0
    local objects, err = g.cluster.main_server.net_box:eval([[
        local crud = require('crud')

        local iter, err = crud.select('customers', nil, {
            limit = 0,
        })
        if err ~= nil then return nil, err end

        local objects = {}
        while iter:has_next() do
            object, err = iter:get()
            if err ~= nil then
                return nil, err
            end
            table.insert(objects, object)
        end

        return objects
    ]])

    t.assert_equals(err, nil)
    t.assert_equals(#objects, 0)

end

g.test_select_all_with_batch_size = function()
    local customers = insert_customers({
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
    local objects, err = g.cluster.main_server.net_box:eval([[
        local crud = require('crud')

        local iter, err = crud.select('customers', nil, {
            batch_size = 1,
        })
        if err ~= nil then return nil, err end

        local objects = {}
        while iter:has_next() do
            object, err = iter:get()
            if err ~= nil then
                return nil, err
            end
            table.insert(objects, object)
        end

        return objects
    ]])

    t.assert_equals(err, nil)
    t.assert_equals(objects, customers)

    -- batch size 3
    local objects, err = g.cluster.main_server.net_box:eval([[
        local crud = require('crud')

        local iter, err = crud.select('customers', nil, {
            batch_size = 3
        })
        if err ~= nil then return nil, err end

        local objects = {}
        while iter:has_next() do
            object, err = iter:get()
            if err ~= nil then
                return nil, err
            end
            table.insert(objects, object)
        end

        return objects
    ]])

    t.assert_equals(err, nil)
    t.assert_equals(objects, customers)
end

g.test_ge_condition_with_index = function()
    local customers = insert_customers({
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

        local iter, err = crud.select('customers', conditions)
        if err ~= nil then return nil, err end

        local objects = {}
        while iter:has_next() do
            object, err = iter:get()
            if err ~= nil then
                return nil, err
            end
            table.insert(objects, object)
        end

        return objects
    ]], {conditions})

    t.assert_equals(err, nil)
    t.assert_equals(objects, get_by_ids(customers, {3, 2, 4})) -- in age order

    -- after obj 3
    local after = customers[3]
    local objects, err = g.cluster.main_server.net_box:eval([[
        local crud = require('crud')

        local conditions, after = ...

        local iter, err = crud.select('customers', conditions, {
            after = after,
        })
        if err ~= nil then return nil, err end

        local objects = {}
        while iter:has_next() do
            object, err = iter:get()
            if err ~= nil then
                return nil, err
            end
            table.insert(objects, object)
        end

        return objects
    ]], {conditions, after})

    t.assert_equals(err, nil)
    t.assert_equals(objects, get_by_ids(customers, {2, 4})) -- in age order
end

g.test_le_condition_with_index = function()
    local customers = insert_customers({
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

        local iter, err = crud.select('customers', conditions)
        if err ~= nil then return nil, err end

        local objects = {}
        while iter:has_next() do
            object, err = iter:get()
            if err ~= nil then
                return nil, err
            end
            table.insert(objects, object)
        end

        return objects
    ]], {conditions})

    t.assert_equals(err, nil)
    t.assert_equals(objects, get_by_ids(customers, {3, 1})) -- in age order

    -- after obj 3
    local after = customers[3]
    local objects, err = g.cluster.main_server.net_box:eval([[
        local crud = require('crud')

        local conditions, after = ...

        local iter, err = crud.select('customers', conditions, {
            after = after,
        })
        if err ~= nil then return nil, err end

        local objects = {}
        while iter:has_next() do
            object, err = iter:get()
            if err ~= nil then
                return nil, err
            end
            table.insert(objects, object)
        end

        return objects
    ]], {conditions, after})

    t.assert_equals(err, nil)
    t.assert_equals(objects, get_by_ids(customers, {1})) -- in age order
end

g.test_lt_condition_with_index = function()
    local customers = insert_customers({
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
        {'<', 'age', 33},
    }

    -- no after
    local objects, err = g.cluster.main_server.net_box:eval([[
        local crud = require('crud')

        local conditions = ...

        local iter, err = crud.select('customers', conditions)
        if err ~= nil then return nil, err end

        local objects = {}
        while iter:has_next() do
            object, err = iter:get()
            if err ~= nil then
                return nil, err
            end
            table.insert(objects, object)
        end

        return objects
    ]], {conditions})

    t.assert_equals(err, nil)
    t.assert_equals(objects, get_by_ids(customers, {1})) -- in age order

    -- after obj 1
    local after = customers[1]
    local objects, err = g.cluster.main_server.net_box:eval([[
        local crud = require('crud')

        local conditions, after = ...

        local iter, err = crud.select('customers', conditions, {
            after = after,
        })
        if err ~= nil then return nil, err end

        local objects = {}
        while iter:has_next() do
            object, err = iter:get()
            if err ~= nil then
                return nil, err
            end
            table.insert(objects, object)
        end

        return objects
    ]], {conditions, after})

    t.assert_equals(err, nil)
    t.assert_equals(objects, get_by_ids(customers, {})) -- in age order
end

g.test_multiple_conditions = function()
    local customers = insert_customers({
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
    local objects, err = g.cluster.main_server.net_box:eval([[
        local crud = require('crud')

        local conditions = ...

        local iter, err = crud.select('customers', conditions)
        if err ~= nil then return nil, err end

        local objects = {}
        while iter:has_next() do
            object, err = iter:get()
            if err ~= nil then
                return nil, err
            end
            table.insert(objects, object)
        end

        return objects
    ]], {conditions})

    t.assert_equals(err, nil)
    t.assert_equals(objects, get_by_ids(customers, {5, 2})) -- in age order

    -- after obj 5
    local after = customers[5]
    local objects, err = g.cluster.main_server.net_box:eval([[
        local crud = require('crud')

        local conditions, after = ...

        local iter, err = crud.select('customers', conditions, {
            after = after,
        })
        if err ~= nil then return nil, err end

        local objects = {}
        while iter:has_next() do
            object, err = iter:get()
            if err ~= nil then
                return nil, err
            end
            table.insert(objects, object)
        end

        return objects
    ]], {conditions, after})

    t.assert_equals(err, nil)
    t.assert_equals(objects, get_by_ids(customers, {2})) -- in age order
end

g.test_composite_index = function()
    local customers = insert_customers({
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
    local objects, err = g.cluster.main_server.net_box:eval([[
        local crud = require('crud')

        local conditions = ...

        local iter, err = crud.select('customers', conditions)
        if err ~= nil then return nil, err end

        local objects = {}
        while iter:has_next() do
            object, err = iter:get()
            if err ~= nil then
                return nil, err
            end
            table.insert(objects, object)
        end

        return objects
    ]], {conditions})

    t.assert_equals(err, nil)
    t.assert_equals(objects, get_by_ids(customers, {2, 1, 4})) -- in full_name order

    -- after obj 2
    local after = customers[2]
    local objects, err = g.cluster.main_server.net_box:eval([[
        local crud = require('crud')

        local conditions, after = ...

        local iter, err = crud.select('customers', conditions, {
            after = after,
        })
        if err ~= nil then return nil, err end

        local objects = {}
        while iter:has_next() do
            object, err = iter:get()
            if err ~= nil then
                return nil, err
            end
            table.insert(objects, object)
        end

        return objects
    ]], {conditions, after})

    t.assert_equals(err, nil)
    t.assert_equals(objects, get_by_ids(customers, {1, 4})) -- in full_name order
end

g.test_select_with_batch_size_1 = function()
    local customers = insert_customers({
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
    local objects, err = g.cluster.main_server.net_box:eval([[
        local crud = require('crud')

        local conditions = ...

        local iter, err = crud.select('customers', conditions, {
            batch_size = 1,
        })
        if err ~= nil then return nil, err end

        local objects = {}
        while iter:has_next() do
            object, err = iter:get()
            if err ~= nil then
                return nil, err
            end
            table.insert(objects, object)
        end

        return objects
    ]], {conditions})

    t.assert_equals(err, nil)
    t.assert_equals(objects, get_by_ids(customers, {5, 3, 6, 7, 1}))

    -- LT
    local conditions = {{'<', 'age', 35}}
    local objects, err = g.cluster.main_server.net_box:eval([[
        local crud = require('crud')

        local conditions = ...

        local iter, err = crud.select('customers', conditions, {
            batch_size = 1,
        })
        if err ~= nil then return nil, err end

        local objects = {}
        while iter:has_next() do
            object, err = iter:get()
            if err ~= nil then
                return nil, err
            end
            table.insert(objects, object)
        end

        return objects
    ]], {conditions})

    t.assert_equals(err, nil)
    t.assert_equals(objects, get_by_ids(customers, {3, 6, 7, 1}))

    -- GE
    local conditions = {{'>=', 'age', 35}}
    local objects, err = g.cluster.main_server.net_box:eval([[
        local crud = require('crud')

        local conditions = ...

        local iter, err = crud.select('customers', conditions, {
            batch_size = 1,
        })
        if err ~= nil then return nil, err end

        local objects = {}
        while iter:has_next() do
            object, err = iter:get()
            if err ~= nil then
                return nil, err
            end
            table.insert(objects, object)
        end

        return objects
    ]], {conditions})

    t.assert_equals(err, nil)
    t.assert_equals(objects, get_by_ids(customers, {5, 8, 2, 4}))

    -- GT
    local conditions = {{'>', 'age', 35}}
    local objects, err = g.cluster.main_server.net_box:eval([[
        local crud = require('crud')

        local conditions = ...

        local iter, err = crud.select('customers', conditions, {
            batch_size = 1,
        })
        if err ~= nil then return nil, err end

        local objects = {}
        while iter:has_next() do
            object, err = iter:get()
            if err ~= nil then
                return nil, err
            end
            table.insert(objects, object)
        end

        return objects
    ]], {conditions})

    t.assert_equals(err, nil)
    t.assert_equals(objects, get_by_ids(customers, {8, 2, 4}))
end
