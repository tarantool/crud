local fio = require('fio')

local t = require('luatest')
local g = t.group('pairs')

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

g.test_pairs_no_conditions = function()
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

        local objects = {}
        for _, object in crud.pairs('customers') do
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

        local after = ...

        local objects = {}
        for _, object in crud.pairs('customers', nil, {after = after}) do
            table.insert(objects, object)
        end

        return objects
    ]], {after})

    t.assert_equals(err, nil)
    t.assert_equals(objects, get_by_ids(customers, {3, 4}))

    -- after obj 4 (last)
    local after = customers[4]
    local objects, err = g.cluster.main_server.net_box:eval([[
        local crud = require('crud')

        local after = ...

        local objects = {}
        for _, object in crud.pairs('customers', nil, {after = after}) do
            table.insert(objects, object)
        end

        return objects
    ]], {after})

    t.assert_equals(err, nil)
    t.assert_equals(#objects, 0)
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

        local objects = {}
        for _, object in crud.pairs('customers', conditions) do
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

        local objects = {}
        for _, object in crud.pairs('customers', conditions, {after = after}) do
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

        local objects = {}
        for _, object in crud.pairs('customers', conditions) do
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

        local objects = {}
        for _, object in crud.pairs('customers', conditions, {after = after}) do
            table.insert(objects, object)
        end

        return objects
    ]], {conditions, after})

    t.assert_equals(err, nil)
    t.assert_equals(objects, get_by_ids(customers, {1})) -- in age order
end

g.test_le_condition_with_index_as_tuple = function()
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

        local objects = {}
        for _, object in crud.pairs('customers', conditions, {tuples_tomap = false}) do
            table.insert(objects, object)
        end

        return objects
    ]], {conditions})

    t.assert_equals(err, nil)

    local tuples = {}
    for _, val in pairs(get_by_ids(customers, {3, 1})) do
        local row = {}
        table.insert(row, val.id)
        table.insert(row,0)
        table.insert(row, val.name)
        table.insert(row, val.last_name)
        table.insert(row, val.age)
        table.insert(row, val.city)

        table.insert(tuples, row)
    end

    -- in age order
    for i, _ in pairs(tuples) do
        tuples[i][2] = objects[i][2] --set bucket_id for expected
        t.assert_equals(objects[i], tuples[i])
    end
end