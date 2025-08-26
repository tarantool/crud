local t = require('luatest')

local stats = require('crud.stats')
local helpers = require('test.helper')

local group = t.group('cfg', helpers.backend_matrix())

group.before_all(function(g)
    helpers.start_default_cluster(g, 'srv_stats')
end)

group.after_all(function(g)
    helpers.stop_cluster(g.cluster, g.params.backend)
end)

group.test_defaults = function(g)
    local cfg = g.router:eval("return require('crud').cfg")
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
    local new_cfg = g.router:eval("return require('crud').cfg({ stats = true })")
    t.assert_equals(new_cfg.stats, true)
end

group.test_table_is_immutable = function(g)
    local router = g.router

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
    local router = g.router

    -- Generate some non-default values.
    router:eval("return require('crud').cfg({ stats = true })")

    helpers.reload_package(router)

    local cfg = router:eval("return require('crud').cfg")
    t.assert_equals(cfg.stats, true)
end

group.test_role_reload_preserves_values = function(g)
    helpers.skip_not_cartridge_backend(g.params.backend)
    t.skip_if(not helpers.is_cartridge_hotreload_supported(),
        "Cartridge roles reload is not supported")
    helpers.skip_old_tarantool_cartridge_hotreload()

    local router = g.router

    -- Generate some non-default values.
    router:eval("return require('crud').cfg({ stats = true })")

    helpers.reload_roles(router)

    local cfg = router:eval("return require('crud').cfg")
    t.assert_equals(cfg.stats, true)
end

group.test_gh_284_preset_stats_quantile_tolerated_error_is_preserved = function(g)
    -- Arrange some cfg values so test case will not depend on defaults.
    local cfg = g.router:eval(
        "return require('crud').cfg(...)",
        {{ stats = false }})
    t.assert_equals(cfg.stats, false)

    -- Set stats_quantile_tolerated_error.
    local cfg = g.router:eval(
        "return require('crud').cfg(...)",
        {{ stats_quantile_tolerated_error = 1e-4 }})
    t.assert_equals(cfg.stats_quantile_tolerated_error, 1e-4)

    -- Set another cfg parameter, assert preset stats_quantile_tolerated_error presents.
    local cfg = g.router:eval(
        "return require('crud').cfg(...)",
        {{ stats = true }})
    t.assert_equals(cfg.stats, true)
    t.assert_equals(cfg.stats_quantile_tolerated_error, 1e-4,
        'Preset stats_quantile_tolerated_error presents')
end

group.test_gh_284_preset_stats_quantile_age_buckets_count_is_preserved = function(g)
    -- Arrange some cfg values so test case will not depend on defaults.
    local cfg = g.router:eval(
        "return require('crud').cfg(...)",
        {{ stats = false }})
    t.assert_equals(cfg.stats, false)

    -- Set stats_age_buckets_count.
    local cfg = g.router:eval(
        "return require('crud').cfg(...)",
        {{ stats_quantile_age_buckets_count = 3 }})
    t.assert_equals(cfg.stats_quantile_age_buckets_count, 3)

    -- Set another cfg parameter, assert preset stats_quantile_age_buckets_count presents.
    local cfg = g.router:eval(
        "return require('crud').cfg(...)",
        {{ stats = true }})
    t.assert_equals(cfg.stats, true)
    t.assert_equals(cfg.stats_quantile_age_buckets_count, 3,
        'Preset stats_quantile_age_buckets_count presents')
end

group.test_gh_284_preset_stats_quantile_max_age_time_is_preserved = function(g)
    -- Arrange some cfg values so test case will not depend on defaults.
    local cfg = g.router:eval(
        "return require('crud').cfg(...)",
        {{ stats = false }})
    t.assert_equals(cfg.stats, false)

    -- Set stats_age_buckets_count.
    local cfg = g.router:eval(
        "return require('crud').cfg(...)",
        {{ stats_quantile_max_age_time = 30 }})
    t.assert_equals(cfg.stats_quantile_max_age_time, 30)

    -- Set another cfg parameter, assert preset stats_quantile_max_age_time presents.
    local cfg = g.router:eval(
        "return require('crud').cfg(...)",
        {{ stats = true }})
    t.assert_equals(cfg.stats, true)
    t.assert_equals(cfg.stats_quantile_max_age_time, 30,
        'Preset stats_quantile_max_age_time presents')
end

group.test_role_cfg = function(g)
    helpers.skip_not_cartridge_backend(g.params.backend)

    local cfg = {
        stats = true,
        stats_driver = 'local',
        stats_quantiles = false,
        stats_quantile_tolerated_error = 1e-2,
        stats_quantile_age_buckets_count = 5,
        stats_quantile_max_age_time = 180,
    }

    g.router:upload_config({["crud"] = cfg})

    local actual_cfg = g.router:eval("return require('crud').cfg")
    t.assert_equals(cfg, actual_cfg)
end

group.test_role_partial_cfg = function(g)
    helpers.skip_not_cartridge_backend(g.params.backend)

    local router = g.router
    local cfg_before = router:eval("return require('crud').cfg()")

    local cfg_after = table.deepcopy(cfg_before)
    cfg_after.stats = not cfg_before.stats

    g.router:upload_config({["crud"] = {stats = cfg_after.stats}})

    local actual_cfg = g.router:eval("return require('crud').cfg")
    t.assert_equals(cfg_after, actual_cfg, "Only requested field were updated")
end

local role_cfg_error_cases = {
    wrong_section_type = {
        args = 'enabled',
        err_cartridge = 'Configuration \\\"crud\\\" section must be a table',
        err_tarantool3 = 'Wrong config for role roles.crud-router: TarantoolRoleConfigurationError: '..
                         'roles_cfg must be a table',
    },
    wrong_structure = {
        args = {crud = {stats = true}},
        err_cartridge = '\\\"crud\\\" section is already presented as a name of \\\"crud.yml\\\", ' ..
                        'do not use it as a top-level section name',
        err_tarantool3 = 'Wrong config for role roles.crud-router: TarantoolRoleConfigurationError: '..
                         'Unknown field \"crud\"',
    },
    wrong_type = {
        args = {stats = 'enabled'},
        err_cartridge = 'Invalid crud configuration field \\\"stats\\\" type: expected boolean, got string',
        err_tarantool3 = 'Wrong config for role roles.crud-router: TarantoolRoleConfigurationError: '..
                         'Invalid \"stats\" field type: expected boolean, got string',
    },
    wrong_value = {
        args = {stats_driver = 'prometheus'},
        err_cartridge = 'Invalid crud configuration field \\\"stats_driver\\\" value: '..
                        '\\\"prometheus\\\" is not supported',
        err_tarantool3 = 'Wrong config for role roles.crud-router: TarantoolRoleConfigurationError: '..
                         'Invalid \"stats_driver\" field value: \"prometheus\" is not supported',
    }
}

for name, case in pairs(role_cfg_error_cases) do
    group['test_cartridge_role_cfg_' .. name] = function(g)
        helpers.skip_not_cartridge_backend(g.params.backend)
        local success, error = pcall(function()
            g.router:upload_config({
                ["crud"] = case.args,
            })
        end)

        t.assert_equals(success, false)
        t.assert_str_contains(error.response.body, case.err_cartridge)
    end

    group['test_tarantool3_role_cfg_' .. name] = function(g)
        helpers.skip_if_not_config_backend(g.params.backend)
        local success, error = pcall(function()
            local cfg = g.cluster:cfg()

            cfg.groups['routers'].roles_cfg = {
                ['roles.crud-router'] = case.args,
            }

            g.cluster:reload_config(cfg)
        end)

        t.assert_equals(success, false)
        t.assert_str_contains(tostring(error), case.err_tarantool3)
    end
end
