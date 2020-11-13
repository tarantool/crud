local fio = require('fio')

local t = require('luatest')
local g_memtx = t.group('pairs_memtx')
local g_vinyl = t.group('pairs_vinyl')

local crud_utils = require('crud.common.utils')

local helpers = require('test.helper')

math.randomseed(os.time())

local function before_all(g, engine)
    g.cluster = helpers.Cluster:new({
        datadir = fio.tempdir(),
        server_command = helpers.entrypoint('srv_select'),
        use_vshard = true,
        replicasets = {
            {
                uuid = helpers.uuid('a'),
                alias = 'router',
                roles = { 'customers-router' },
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
        env = {
            ['ENGINE'] = engine,
        },
    })
    g.engine = engine
    g.cluster:start()

    g.space_format = g.cluster.servers[2].net_box.space.customers:format()
end

local function after_all(g)
    g.cluster:stop()
    fio.rmtree(g.cluster.datadir)
end

local function before_each(g)
    for _, server in ipairs(g.cluster.servers) do
        server.net_box:eval([[
            local space = box.space.customers
            if space ~= nil and not box.cfg.read_only then
                space:truncate()
            end
        ]])
    end
end

g_memtx.before_all = function() before_all(g_memtx, 'memtx') end
g_vinyl.before_all = function() before_all(g_vinyl, 'vinyl') end

g_memtx.after_all = function() after_all(g_memtx) end
g_vinyl.after_all = function() after_all(g_vinyl) end

g_memtx.before_each(function() before_each(g_memtx) end)
g_vinyl.before_each(function() before_each(g_vinyl) end)

local function add(name, fn)
    g_memtx[name] = fn
    g_vinyl[name] = fn
end

add('test_pairs_no_conditions', function(g)
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
end)

add('test_ge_condition_with_index', function(g)
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
end)

add('test_le_condition_with_index', function(g)
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
end)

add('test_negative_first', function(g)
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
end)

add('test_empty_space', function(g)
    local count = g.cluster.main_server.net_box:eval([[
        local crud = require('crud')
        local count = 0
        for _, object in crud.pairs('customers') do
            count = count + 1
        end
        return count
    ]])
    t.assert_equals(count, 0)
end)
