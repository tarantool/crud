local fio = require('fio')
local log = require('log')
local fiber = require('fiber')
local errors = require('errors')

local t = require('luatest')
local g = t.group()

local helpers = require('test.helper')

local function reload(srv)
    local ok, err = srv.net_box:eval([[
        return require("cartridge.roles").reload()
    ]])

    t.assert_equals({ok, err}, {true, nil})
end

g.before_all(function()
    g.cluster = helpers.Cluster:new({
        datadir = fio.tempdir(),
        use_vshard = true,
        server_command = helpers.entrypoint('srv_reload'),
        replicasets = helpers.get_test_replicasets(),
    })

    g.cluster:start()

    local simple_schema = {
        engine = 'memtx',
        is_local = false,
        temporary = false,
        format = {
            {name = 'bucket_id', type = 'unsigned', is_nullable = false},
            {name = 'record_id', type = 'unsigned', is_nullable = false},
        },
        indexes = {
            {
                name = 'pk', type = 'TREE', unique = true,
                parts = {{path = 'record_id', is_nullable = false, type = 'unsigned'}},
            },
            {
                name = 'bucket_id', type = 'TREE', unique = false,
                parts = {{path = 'bucket_id', is_nullable = false, type = 'unsigned'}},
            }
        },
        sharding_key = {'record_id'},
    }

    g.cluster.main_server.net_box:call('cartridge_set_schema',
        {require('yaml').encode({spaces = {simple_space = simple_schema}})}
    )

    g.router = assert(g.cluster:server('router'))
    g.s1_master = assert(g.cluster:server('s1-master'))
    g.s1_replica = assert(g.cluster:server('s1-replica'))

    g.insertions_passed = {}
    g.insertions_failed = {}
end)

g.after_all(function()
    g.cluster:stop()
    fio.rmtree(g.cluster.datadir)
end)

local function _insert(cnt, label)
    local result, err = g.router.net_box:call('crud.insert', {'simple_space', {1, cnt, label}})

    if result == nil then
        log.error('CNT %d: %s', cnt, err)
        table.insert(g.insertions_failed, {cnt = cnt, err = err})
    else
        table.insert(g.insertions_passed, result.rows[1])
    end

    return true
end

local highload_cnt = 0
local function highload_loop(label)
    fiber.name('test.highload')
    log.warn('Highload started ----------')

    while true do
        highload_cnt = highload_cnt + 1
        local ok, err = errors.pcall('E', _insert, highload_cnt, label)

        if ok == nil then
            log.error('CNT %d: %s', highload_cnt, err)
        end

        fiber.sleep(0.002)
    end
end

g.after_each(function()
    if g.highload_fiber ~= nil and g.highload_fiber:status() == 'suspended' then
        g.highload_fiber:cancel()
    end

    log.warn(
        'Total insertions: %d (%d good, %d failed)',
        highload_cnt, #g.insertions_passed, #g.insertions_failed
    )

    for _, e in ipairs(g.insertions_failed) do
        log.error('#%d: %s', e.cnt, e.err)
    end
end)

function g.test_router()
    g.highload_fiber = fiber.new(highload_loop, 'A')

    g.cluster:retrying({}, function()
        local last_insert = g.insertions_passed[#g.insertions_passed]
        t.assert_equals(last_insert[3], 'A', 'No workload for label A')
    end)

    reload(g.router)

    local cnt = #g.insertions_passed
    g.cluster:retrying({}, function()
        assert(#g.insertions_passed > cnt)
    end)

    g.highload_fiber:cancel()

    local result, err = g.router.net_box:call('crud.select', {'simple_space'})
    t.assert_equals(err, nil)
    t.assert_items_include(result.rows, g.insertions_passed)
end

function g.test_storage()
    g.highload_fiber = fiber.new(highload_loop, 'B')

    g.cluster:retrying({}, function()
        local last_insert = g.insertions_passed[#g.insertions_passed]
        t.assert_equals(last_insert[3], 'B', 'No workload for label B')
    end)

    -- snapshot with a signal
    g.s1_master.process:kill('USR1')

    reload(g.s1_master)

    g.cluster:retrying({}, function()
        g.s1_master.net_box:call('box.snapshot')
    end)

    local cnt = #g.insertions_passed
    local ok = pcall(g.cluster.retrying, g.cluster, {timeout = 2}, function()
        helpers.assert_ge(#g.insertions_passed, cnt+1)
    end)

    if not ok then
        g.highload_fiber:cancel()
        fiber.sleep(10000)
    end

    g.highload_fiber:cancel()

    local result, err = g.router.net_box:call('crud.select', {'simple_space'})
    t.assert_equals(err, nil)
    t.assert_items_include(result.rows, g.insertions_passed)
end
