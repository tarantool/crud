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
        stats_quantile_tolerated_error = 1e-3,
        stats_quantile_age_buckets_count = 2,
        stats_quantile_max_age_time = 60,
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

group.test_gh_284_preset_stats_quantile_tolerated_error_is_preserved = function(g)
    -- Arrange some cfg values so test case will not depend on defaults.
    local cfg = g.cluster:server('router'):eval(
        "return require('crud').cfg(...)",
        {{ stats = false }})
    t.assert_equals(cfg.stats, false)

    -- Set stats_quantile_tolerated_error.
    local cfg = g.cluster:server('router'):eval(
        "return require('crud').cfg(...)",
        {{ stats_quantile_tolerated_error = 1e-4 }})
    t.assert_equals(cfg.stats_quantile_tolerated_error, 1e-4)

    -- Set another cfg parameter, assert preset stats_quantile_tolerated_error presents.
    local cfg = g.cluster:server('router'):eval(
        "return require('crud').cfg(...)",
        {{ stats = true }})
    t.assert_equals(cfg.stats, true)
    t.assert_equals(cfg.stats_quantile_tolerated_error, 1e-4,
        'Preset stats_quantile_tolerated_error presents')
end

group.test_gh_284_preset_stats_quantile_age_buckets_count_is_preserved = function(g)
    -- Arrange some cfg values so test case will not depend on defaults.
    local cfg = g.cluster:server('router'):eval(
        "return require('crud').cfg(...)",
        {{ stats = false }})
    t.assert_equals(cfg.stats, false)

    -- Set stats_age_buckets_count.
    local cfg = g.cluster:server('router'):eval(
        "return require('crud').cfg(...)",
        {{ stats_quantile_age_buckets_count = 3 }})
    t.assert_equals(cfg.stats_quantile_age_buckets_count, 3)

    -- Set another cfg parameter, assert preset stats_quantile_age_buckets_count presents.
    local cfg = g.cluster:server('router'):eval(
        "return require('crud').cfg(...)",
        {{ stats = true }})
    t.assert_equals(cfg.stats, true)
    t.assert_equals(cfg.stats_quantile_age_buckets_count, 3,
        'Preset stats_quantile_age_buckets_count presents')
end

group.test_gh_284_preset_stats_quantile_max_age_time_is_preserved = function(g)
    -- Arrange some cfg values so test case will not depend on defaults.
    local cfg = g.cluster:server('router'):eval(
        "return require('crud').cfg(...)",
        {{ stats = false }})
    t.assert_equals(cfg.stats, false)

    -- Set stats_age_buckets_count.
    local cfg = g.cluster:server('router'):eval(
        "return require('crud').cfg(...)",
        {{ stats_quantile_max_age_time = 30 }})
    t.assert_equals(cfg.stats_quantile_max_age_time, 30)

    -- Set another cfg parameter, assert preset stats_quantile_max_age_time presents.
    local cfg = g.cluster:server('router'):eval(
        "return require('crud').cfg(...)",
        {{ stats = true }})
    t.assert_equals(cfg.stats, true)
    t.assert_equals(cfg.stats_quantile_max_age_time, 30,
        'Preset stats_quantile_max_age_time presents')
end
