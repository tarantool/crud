local fio = require('fio')

local t = require('luatest')

local stats = require('crud.stats')
local helpers = require('test.helper')

local group = t.group('cfg')

group.before_all(function(g)
    g.cluster = helpers.Cluster:new({
        datadir = fio.tempdir(),
        server_command = helpers.entrypoint('srv_stats'),
        use_vshard = true,
        replicasets = helpers.get_test_replicasets(),
    })

    g.cluster:start()
end)

group.after_all(function(g) helpers.stop_cluster(g.cluster) end)

group.test_defaults = function(g)
    local cfg = g.cluster:server('router'):eval("return require('crud').cfg")
    t.assert_equals(cfg, {
        stats = false,
        stats_driver = stats.get_default_driver(),
        stats_quantiles = false,
    })
end

group.test_change_value = function(g)
    local new_cfg = g.cluster:server('router'):eval("return require('crud').cfg({ stats = true })")
    t.assert_equals(new_cfg.stats, true)
end

group.test_table_is_immutable = function(g)
    local router = g.cluster:server('router')

    t.assert_error_msg_contains(
        'Use crud.cfg{} instead',
        router.eval, router,
        [[
            local cfg = require('crud').cfg()
            cfg.stats = 'newvalue'
        ]])

    t.assert_error_msg_contains(
        'Use crud.cfg{} instead',
        router.eval, router,
        [[
            local cfg = require('crud').cfg()
            cfg.newfield = 'newvalue'
        ]])
end

group.test_package_reload_preserves_values = function(g)
    local router = g.cluster:server('router')

    -- Generate some non-default values.
    router:eval("return require('crud').cfg({ stats = true })")

    helpers.reload_package(router)

    local cfg = router:eval("return require('crud').cfg")
    t.assert_equals(cfg.stats, true)
end

group.test_role_reload_preserves_values = function(g)
    local router = g.cluster:server('router')

    -- Generate some non-default values.
    router:eval("return require('crud').cfg({ stats = true })")

    helpers.reload_roles(router)

    local cfg = router:eval("return require('crud').cfg")
    t.assert_equals(cfg.stats, true)
end
