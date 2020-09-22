local fio = require('fio')
local fiber = require('fiber')
local errors = require('errors')
local net_box = require('net.box')
local log = require('log')

local t = require('luatest')
local g = t.group('perf')

local helpers = require('test.helper')

g.before_all = function()
    g.cluster = helpers.Cluster:new({
        datadir = fio.tempdir(),
        server_command = helpers.entrypoint('srv_select'),
        use_vshard = true,
        replicasets = {
            {
                uuid = helpers.uuid('a'),
                alias = 'router',
                roles = { 'crud-router' },
                servers = {
                    { instance_uuid = helpers.uuid('a', 1), alias = 'router' },
                },
            },
            {
                uuid = helpers.uuid('b'),
                alias = 's-1',
                roles = { 'customers-storage', 'crud-storage' },
                servers = {
                    { instance_uuid = helpers.uuid('b', 1), alias = 's1-master' },
                    { instance_uuid = helpers.uuid('b', 2), alias = 's1-replica' },
                },
            },
            {
                uuid = helpers.uuid('c'),
                alias = 's-2',
                roles = { 'customers-storage', 'crud-storage' },
                servers = {
                    { instance_uuid = helpers.uuid('c', 1), alias = 's2-master' },
                    { instance_uuid = helpers.uuid('c', 2), alias = 's2-replica' },
                },
            },
            {
                uuid = helpers.uuid('d'),
                alias = 's-2',
                roles = { 'customers-storage', 'crud-storage' },
                servers = {
                    { instance_uuid = helpers.uuid('d', 1), alias = 's3-master' },
                    { instance_uuid = helpers.uuid('d', 2), alias = 's3-replica' },
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

g.before_each(function() end)

local function insert_customers(conn, id, count, timeout, report)
    local customer = {id, box.NULL, 'David', 'Smith', 33, 'Los Angeles'}
    local start = fiber.clock()

    while (fiber.clock() - start) < timeout do
        local ok, res, err = pcall(conn.call, conn, [[package.loaded.crud.insert]], {'customers', customer})
        if not ok then
            log.error('Insert error: %s', res)
            table.insert(report.errors, res)
        elseif err ~= nil then
            errors.wrap(err)
            log.error('Insert error: %s', err)
            table.insert(report.errors, err)
        else
            report.count = report.count + 1
        end
        customer[1] = customer[1] + count
    end
end

local function select_customers(conn, id, timeout, report)
    local start = fiber.clock()
    local ok, err = pcall(function()
        while (fiber.clock() - start) < timeout do
            local _, err = conn:call([[package.loaded.crud.select]], {'customers', {{'>', 'id', id}}, {first = 10}})
            if err ~= nil then
                errors.wrap(err)
                log.error(err)
                table.insert(report.errors, err)
            else
                report.count = report.count + 1
            end
        end
    end)
    if not ok then
        table.insert(report.errors, err)
        log.error(err)
    end
end

g.test_insert = function()
    local timeout = 30
    local fiber_count = 600
    local connection_count = 10
    local connections = {}

    local server = g.cluster.main_server
    server.net_box:eval([[require('crud')]])
    for _ = 1, connection_count do
        local c = net_box:connect(server.net_box_uri, server.net_box_credentials)
        assert(c)
        table.insert(connections, c)
    end

    local fibers = {}
    local report = {errors = {}, count = 0}
    for id = 1, fiber_count do
        local conn_id = id % connection_count + 1
        local conn = connections[conn_id]
        local f = fiber.new(insert_customers, conn, id, fiber_count, timeout, report)
        f:set_joinable(true)
        table.insert(fibers, f)
    end

    for i = 1, fiber_count do
        fibers[i]:join()
    end

    log.error('INSERT')
    log.error('Fibers count - %d', fiber_count)
    log.error('Connection count - %d', connection_count)
    log.error('Timeout  - %f', timeout)
    log.error('Requests - %d', report.count)
    log.error('Errors - %s', #report.errors)
    log.error('RPS - %f', report.count / timeout)
end

g.test_select = function()
    local timeout = 30
    local fiber_count = 200
    local connection_count = 10
    local connections = {}

    local server = g.cluster.main_server
    server.net_box:eval([[require('crud')]])
    for _ = 1, connection_count do
        local c = net_box:connect(server.net_box_uri, server.net_box_credentials)
        assert(c)
        table.insert(connections, c)
    end

    local fibers = {}
    local report = {errors = {}, count = 0}
    for id = 1, fiber_count do
        local conn_id = id % connection_count + 1
        local conn = connections[conn_id]
        local f = fiber.new(select_customers, conn, id, timeout, report)
        f:set_joinable(true)
        table.insert(fibers, f)
    end

    for i = 1, fiber_count do
        fibers[i]:join()
    end

    log.error('SELECT')
    log.error('Fibers count - %d', fiber_count)
    log.error('Connection count - %d', connection_count)
    log.error('Timeout  - %f', timeout)
    log.error('Requests - %d', report.count)
    log.error('Errors - %s', #report.errors)
    log.error('RPS - %f', report.count / timeout)
end
