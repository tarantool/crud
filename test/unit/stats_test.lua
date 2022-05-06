local clock = require('clock')
local fio = require('fio')
local fun = require('fun')
local t = require('luatest')

local stats_module = require('crud.stats')

local pgroup = t.group('stats_unit', {
    { driver = 'local' },
    { driver = 'metrics', quantiles = false },
    { driver = 'metrics', quantiles = true },
})
local group_driver = t.group('stats_driver_unit')
local helpers = require('test.helper')

local space_name = 'customers'

local function before_all(g)
    -- Enable test cluster for "is space exist?" checks.
    g.cluster = helpers.Cluster:new({
        datadir = fio.tempdir(),
        server_command = helpers.entrypoint('srv_stats'),
        use_vshard = true,
        replicasets = helpers.get_test_replicasets(),
    })
    g.cluster:start()
    g.router = g.cluster:server('router').net_box

    helpers.prepare_simple_functions(g.router)
    g.router:eval("stats_module = require('crud.stats')")

    g.is_metrics_supported = g.router:eval([[
        return require('crud.stats.metrics_registry').is_supported()
    ]])

    if g.params ~= nil and g.params.driver == 'metrics' then
        t.skip_if(g.is_metrics_supported == false, 'Metrics registry is unsupported')
    end
end

local function after_all(g)
    helpers.stop_cluster(g.cluster)
end

local function get_stats(g, space_name)
    return g.router:eval("return stats_module.get(...)", { space_name })
end

local function enable_stats(g, params)
    params = params or g.params
    g.router:eval("stats_module.enable(...)", { params })
end

local function disable_stats(g)
    g.router:eval("stats_module.disable()")
end

local function reset_stats(g)
    g.router:eval("return stats_module.reset()")
end

pgroup.before_all(before_all)

pgroup.after_all(after_all)

-- Reset statistics between tests, reenable if needed.
pgroup.before_each(enable_stats)

pgroup.after_each(disable_stats)


group_driver.before_all(before_all)

group_driver.after_all(after_all)

group_driver.after_each(disable_stats)

pgroup.test_get_format_after_enable = function(g)
    local stats = get_stats(g)

    t.assert_type(stats, 'table')
    t.assert_equals(stats.spaces, {})
end

pgroup.test_get_by_space_name_format_after_enable = function(g)
    local stats = get_stats(g, space_name)

    t.assert_type(stats, 'table')
    t.assert_equals(stats, {})
end

-- Test statistics values after wrapped functions call.
local observe_cases = {
    wrapper_observes_expected_values_on_ok = {
        operations = stats_module.op,
        func = 'return_true',
        changed_coll = 'ok',
        unchanged_coll = 'error',
    },
    wrapper_observes_expected_values_on_error_return = {
        operations = stats_module.op,
        func = 'return_err',
        changed_coll = 'error',
        unchanged_coll = 'ok',
    },
    wrapper_observes_expected_values_on_error_throw = {
        operations = stats_module.op,
        func = 'throws_error',
        changed_coll = 'error',
        unchanged_coll = 'ok',
        pcall = true,
    },
}

local call_wrapped = [[
    local func = rawget(_G, select(1, ...))
    local op = select(2, ...)
    local opts = select(3, ...)
    local space_name = select(4, ...)

    stats_module.wrap(func, op, opts)(space_name)
]]

for name, case in pairs(observe_cases) do
    for _, op in pairs(case.operations) do
        local test_name = ('test_%s_%s'):format(op, name)

        pgroup[test_name] = function(g)
            -- Call wrapped functions on server side.
            -- Collect execution times from outside.
            local run_count = 10
            local time_diffs = {}

            local args = { case.func, op, case.opts, space_name }

            for _ = 1, run_count do
                local before_start = clock.monotonic()

                if case.pcall then
                    pcall(g.router.eval, g.router, call_wrapped, args)
                else
                    g.router:eval(call_wrapped, args)
                end

                local after_finish = clock.monotonic()

                table.insert(time_diffs, after_finish - before_start)
            end

            table.sort(time_diffs)
            local total_time = fun.sum(time_diffs)

            -- Validate stats format after execution.
            local total_stats = get_stats(g)
            t.assert_type(total_stats, 'table', 'Total stats present after observations')

            local space_stats = get_stats(g, space_name)
            t.assert_type(space_stats, 'table', 'Space stats present after observations')

            t.assert_equals(total_stats.spaces[space_name], space_stats,
                'Space stats is a section of total stats')

            local op_stats = space_stats[op]
            t.assert_type(op_stats, 'table', 'Op stats present after observations for the space')

            -- Expected collectors (changed_coll: 'ok' or 'error') have changed.
            local changed = op_stats[case.changed_coll]
            t.assert_type(changed, 'table', 'Status stats present after observations')

            t.assert_equals(changed.count, run_count, 'Count incremented by count of runs')

            local sleep_time = helpers.simple_functions_params().sleep_time
            t.assert_ge(changed.latency, sleep_time, 'Latency has appropriate value')
            t.assert_le(changed.latency, time_diffs[#time_diffs], 'Latency has appropriate value')

            t.assert_ge(changed.time, sleep_time * run_count,
                'Total time increase has appropriate value')
            t.assert_le(changed.time, total_time, 'Total time increase has appropriate value')

            -- Other collectors (unchanged_coll: 'error' or 'ok')
            -- have been initialized and have default values.
            local unchanged = op_stats[case.unchanged_coll]
            t.assert_type(unchanged, 'table', 'Other status stats present after observations')

            t.assert_equals(
                unchanged,
                {
                    count = 0,
                    latency = 0,
                    time = 0
                },
                'Other status collectors initialized after observations'
            )

            -- SELECT collectors have additional details section.
            if op == stats_module.op.SELECT then
                t.assert_equals(
                    op_stats.details,
                    {
                        tuples_fetched = 0,
                        tuples_lookup = 0,
                        map_reduces = 0,
                    },
                    'Detail collectors initialized after select observations'
                )
            end
        end
    end
end

local pairs_cases = {
    success_run = {
        prepare = [[
            local params = ...
            local sleep_time = params.sleep_time

            local function sleep_ten_times(param, state)
                if state == 10 then
                    return nil
                end

                sleep_for(sleep_time)

                return state + 1, param
            end
            rawset(_G, 'sleep_ten_times', sleep_ten_times)
        ]],
        eval = [[
            local params, space_name, op = ...
            local sleep_time = params.sleep_time

            local build_sleep_multiplier = 2

            local wrapped = stats_module.wrap(
                function(space_name)
                    sleep_for(build_sleep_multiplier * sleep_time)

                    return sleep_ten_times, {}, 0
                end,
                op,
                { pairs = true }
            )

            for _, _ in wrapped(space_name) do end
        ]],
        build_sleep_multiplier = 2,
        iterations_expected = 10,
        changed_coll = 'ok',
        unchanged_coll = 'error',
    },
    error_throw = {
        prepare = [[
            local params = ...
            local sleep_time = params.sleep_time
            local error_table = params.error


            local function sleep_five_times_and_throw_error(param, state)
                if state == 5 then
                    error(error_table)
                end

                sleep_for(sleep_time)

                return state + 1, param
            end
            rawset(_G, 'sleep_five_times_and_throw_error', sleep_five_times_and_throw_error)
        ]],
        eval = [[
            local params, space_name, op = ...
            local sleep_time = params.sleep_time

            local build_sleep_multiplier = 2

            local wrapped = stats_module.wrap(
                function(space_name)
                    sleep_for(build_sleep_multiplier * sleep_time)

                    return sleep_five_times_and_throw_error, {}, 0
                end,
                op,
                { pairs = true }
            )

            for _, _ in wrapped(space_name) do end
        ]],
        build_sleep_multiplier = 2,
        iterations_expected = 5,
        changed_coll = 'error',
        unchanged_coll = 'ok',
        pcall = true,
    },
    break_after_gc = {
        prepare = [[
            local params = ...
            local sleep_time = params.sleep_time

            local function sleep_ten_times(param, state)
                if state == 10 then
                    return nil
                end

                sleep_for(sleep_time)

                return state + 1, param
            end
            rawset(_G, 'sleep_ten_times', sleep_ten_times)
        ]],
        eval = [[
            local params, space_name, op = ...
            local sleep_time = params.sleep_time

            local build_sleep_multiplier = 2

            local wrapped = stats_module.wrap(
                function(space_name)
                    sleep_for(build_sleep_multiplier * sleep_time)

                    return sleep_ten_times, {}, 0
                end,
                op,
                { pairs = true }
            )

            for i, _ in wrapped(space_name) do
                if i == 5 then
                    break
                end
            end
        ]],
        post_eval = [[
            collectgarbage('collect')
            collectgarbage('collect')
            require('fiber').yield()
        ]],
        build_sleep_multiplier = 2,
        iterations_expected = 5,
        changed_coll = 'ok',
        unchanged_coll = 'error',
    }
}

for name, case in pairs(pairs_cases) do
    local test_name = ('test_pairs_wrapper_observes_all_iterations_on_%s'):format(name)

    pgroup.before_test(test_name, function(g)
        g.router:eval(case.prepare, { helpers.simple_functions_params() })
    end)

    pgroup[test_name] = function(g)
        local op = stats_module.op.SELECT

        local params = helpers.simple_functions_params()
        local args = { params, space_name, op }

        local before_start = clock.monotonic()

        if case.pcall then
            pcall(g.router.eval, g.router, case.eval, args)
        else
            g.router:eval(case.eval, args)
        end

        if case.post_eval then
            g.router:eval(case.post_eval)
        end

        local after_finish = clock.monotonic()
        local time_diff = after_finish - before_start

        -- Validate stats format after execution.
        local total_stats = get_stats(g)
        t.assert_type(total_stats, 'table', 'Total stats present after observations')

        local space_stats = get_stats(g, space_name)
        t.assert_type(space_stats, 'table', 'Space stats present after observations')

        t.assert_equals(total_stats.spaces[space_name], space_stats,
            'Space stats is a section of total stats')

        local op_stats = space_stats[op]
        t.assert_type(op_stats, 'table', 'Op stats present after observations for the space')

        -- Expected collectors (changed_coll: 'ok' or 'error') have changed.
        local changed = op_stats[case.changed_coll]
        t.assert_type(changed, 'table', 'Status stats present after observations')

        t.assert_equals(changed.count, 1, 'Count incremented by 1')

        t.assert_ge(changed.latency,
            params.sleep_time * (case.build_sleep_multiplier + case.iterations_expected),
            'Latency has appropriate value')
        t.assert_le(changed.latency, time_diff, 'Latency has appropriate value')

        t.assert_ge(changed.time,
            params.sleep_time * (case.build_sleep_multiplier + case.iterations_expected),
            'Total time has appropriate value')
        t.assert_le(changed.time, time_diff, 'Total time  has appropriate value')

        -- Other collectors (unchanged_coll: 'error' or 'ok')
        -- have been initialized and have default values.
        local unchanged = op_stats[case.unchanged_coll]
        t.assert_type(unchanged, 'table', 'Other status stats present after observations')

        t.assert_equals(
            unchanged,
            {
                count = 0,
                latency = 0,
                time = 0
            },
            'Other status collectors initialized after observations'
        )
    end
end

-- Test wrapper preserves return values.
local disable_stats_cases = {
    stats_disable_before_wrap_ = {
        before_wrap = 'stats_module.disable()',
        after_wrap = '',
    },
    stats_disable_after_wrap_ = {
        before_wrap = '',
        after_wrap = 'stats_module.disable()',
    },
    [''] = {
        before_wrap = '',
        after_wrap = '',
    },
}

local preserve_return_cases = {
    wrapper_preserves_return_values_on_ok = {
        func = 'return_true',
        res = true,
        err = nil,
    },
    wrapper_preserves_return_values_on_error = {
        func = 'return_err',
        res = nil,
        err = helpers.simple_functions_params().error,
    },
}

local preserve_throw_cases = {
    wrapper_preserves_error_throw = {
        opts = { pairs = false },
    },
    pairs_wrapper_preserves_error_throw = {
        opts = { pairs = true },
    },
}

for name_head, disable_case in pairs(disable_stats_cases) do
    for name_tail, return_case in pairs(preserve_return_cases) do
        local test_name = ('test_%s%s'):format(name_head, name_tail)

        pgroup[test_name] = function(g)
            local op = stats_module.op.INSERT

            local eval = ([[
                local func = rawget(_G, select(1, ...))
                local op = select(2, ...)
                local space_name = select(3, ...)

                %s -- before_wrap
                local w_func = stats_module.wrap(func, op)
                %s -- after_wrap

                return w_func(space_name)
            ]]):format(disable_case.before_wrap, disable_case.after_wrap)

            local res, err = g.router:eval(eval, { return_case.func, op, space_name })

            t.assert_equals(res, return_case.res, 'Wrapper preserves first return value')
            t.assert_equals(err, return_case.err, 'Wrapper preserves second return value')
        end
    end

    local test_name = ('test_%spairs_wrapper_preserves_return_values'):format(name_head)

    pgroup[test_name] = function(g)
        local op = stats_module.op.INSERT

        local input = { a = 'a', b = 'b' }
        local eval = ([[
            local input = select(1, ...)
            local func = function() return pairs(input) end
            local op = select(2, ...)
            local space_name = select(3, ...)

            %s -- before_wrap
            local w_func = stats_module.wrap(func, op, { pairs = true })
            %s -- after_wrap

            local res = {}
            for k, v in w_func(space_name) do
                res[k] = v
            end

            return res
        ]]):format(disable_case.before_wrap, disable_case.after_wrap)

        local res = g.router:eval(eval, { input, op, space_name })

        t.assert_equals(input, res, 'Wrapper preserves pairs return values')
    end

    for name_tail, throw_case in pairs(preserve_throw_cases) do
        local test_name = ('test_%s%s'):format(name_head, name_tail)

        pgroup[test_name] = function(g)
            local op = stats_module.op.INSERT

            local eval = ([[
                local func = rawget(_G, 'throws_error')
                local opts = select(1, ...)
                local op = select(2, ...)
                local space_name = select(3, ...)

                %s -- before_wrap
                local w_func = stats_module.wrap(func, op, opts)
                %s -- after_wrap

                w_func(space_name)
            ]]):format(disable_case.before_wrap, disable_case.after_wrap)

            t.assert_error_msg_contains(
                helpers.simple_functions_params().error_msg,
                g.router.eval, g.router, eval, { throw_case.opts, op, space_name }
            )
        end
    end
end

pgroup.test_stats_is_empty_after_disable = function(g)
    disable_stats(g)

    local op = stats_module.op.INSERT
    g.router:eval(call_wrapped, { 'return_true', op, {}, space_name })

    local stats = get_stats(g)
    t.assert_equals(stats, {})
end


local function prepare_non_default_stats(g)
    local op = stats_module.op.INSERT
    g.router:eval(call_wrapped, { 'return_true', op, {}, space_name })

    local stats = get_stats(g, space_name)
    t.assert_equals(stats[op].ok.count, 1, 'Non-zero stats prepared')

    return stats
end

pgroup.test_enable_is_idempotent = function(g)
    local stats_before = prepare_non_default_stats(g)

    enable_stats(g)

    local stats_after = get_stats(g, space_name)

    t.assert_equals(stats_after, stats_before, 'Stats have not been reset')
end

pgroup.test_reset = function(g)
    prepare_non_default_stats(g)

    reset_stats(g)

    local stats = get_stats(g, space_name)

    t.assert_equals(stats, {}, 'Stats have been reset')
end

pgroup.test_reset_for_disabled_stats_does_not_init_module = function(g)
    disable_stats(g)

    local stats_before = get_stats(g)
    t.assert_equals(stats_before, {}, "Stats is empty")

    reset_stats(g)

    local stats_after = get_stats(g)
    t.assert_equals(stats_after, {}, "Stats is still empty")
end

pgroup.test_fetch_stats_update = function(g)
    local storage_cursor_stats = { tuples_fetched = 5, tuples_lookup = 25 }

    g.router:eval([[ stats_module.update_fetch_stats(...) ]],
        { storage_cursor_stats, space_name })

    local op = stats_module.op.SELECT
    local stats = get_stats(g, space_name)

    t.assert_not_equals(stats[op], nil,
        'Fetch stats update inits SELECT collectors')

    local details = stats[op].details

    t.assert_equals(details.tuples_fetched, 5,
        'tuples_fetched is inremented by expected value')
    t.assert_equals(details.tuples_lookup, 25,
        'tuples_lookup is inremented by expected value')
end

pgroup.test_disable_stats_do_not_break_fetch_stats_update_call = function(g)
    local storage_cursor_stats = { tuples_fetched = 5, tuples_lookup = 25 }

    disable_stats(g)

    local _, err = g.router:eval([[ stats_module.update_fetch_stats(...) ]],
        { storage_cursor_stats, space_name })
    t.assert_equals(err, nil)
end

pgroup.test_map_reduce_increment = function(g)
    local op = stats_module.op.SELECT

    local _, err = g.router:eval([[ stats_module.update_map_reduces(...) ]], { space_name })
    t.assert_equals(err, nil)

    local stats = get_stats(g)

    t.assert_equals(stats.spaces[space_name][op].details.map_reduces, 1,
        "Counter of map reduces incremented")
end

pgroup.test_disable_stats_do_not_break_map_reduce_update_call = function(g)
    disable_stats(g)

    local _, err = g.router:eval([[ stats_module.update_map_reduces(...) ]], { space_name })
    t.assert_equals(err, nil)
end


group_driver.test_default_driver = function(g)
    enable_stats(g)

    local driver = g.router:eval(" return stats_module.internal.driver ")

    if g.is_metrics_supported then
        t.assert_equals(driver, 'metrics')
    else
        t.assert_equals(driver, 'local')
    end
end


group_driver.test_default_quantiles = function(g)
    enable_stats(g)

    local quantiles = g.router:eval(" return stats_module.internal.quantiles ")
    t.assert_equals(quantiles, false)
end


group_driver.test_default_quantile_tolerated_error = function(g)
    enable_stats(g)

    local quantile_tolerated_error = g.router:eval(" return stats_module.internal.quantile_tolerated_error ")
    t.assert_equals(quantile_tolerated_error, 1e-3)
end


group_driver.before_test(
    'test_custom_quantile_tolerated_error',
    function(g)
        t.skip_if(g.is_metrics_supported == false, 'Metrics registry is unsupported')
    end
)

group_driver.test_custom_quantile_tolerated_error = function(g)
    g.router:call('crud.cfg', {{
        stats = true,
        stats_driver = 'metrics',
        stats_quantiles = true,
        stats_quantile_tolerated_error = 5e-4,
    }})

    local resp = g.router:eval([[
        local metrics = require('metrics')

        local summary = metrics.summary('tnt_crud_stats')
        return summary.objectives
    ]])

    t.assert_equals(resp, {[0.99] = 5e-4})
end


group_driver.before_test(
    'test_stats_reenable_with_different_driver_reset_stats',
    function(g)
        t.skip_if(g.is_metrics_supported == false, 'Metrics registry is unsupported')
    end
)

group_driver.test_stats_reenable_with_different_driver_reset_stats = function(g)
    enable_stats(g, { driver = 'metrics' })

    prepare_non_default_stats(g)

    enable_stats(g, { driver = 'local' })
    local stats = get_stats(g)
    t.assert_equals(stats.spaces, {}, 'Stats have been reset')
end


group_driver.test_unknown_driver_throws_error = function(g)
    t.assert_error_msg_contains(
        'Unsupported driver: unknown',
        enable_stats, g, { driver = 'unknown' })
end


group_driver.before_test(
    'test_stats_enable_with_metrics_throws_error_if_unsupported',
    function(g)
        t.skip_if(g.is_metrics_supported == true, 'Metrics registry is supported')
    end
)

group_driver.test_stats_enable_with_metrics_throws_error_if_unsupported = function(g)
    t.assert_error_msg_contains(
        'Unsupported driver: metrics',
        enable_stats, g, { driver = 'metrics' })
end


group_driver.test_stats_enable_with_local_throws_error_if_quantiles_enabled = function(g)
    t.assert_error_msg_contains(
        'Quantiles are not supported',
        enable_stats, g, { driver = 'local', quantiles = true })
end
