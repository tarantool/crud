local fio = require('fio')
local clock = require('clock')
local fiber = require('fiber')
local errors = require('errors')
local net_box = require('net.box')
local log = require('log')

local t = require('luatest')
local g = t.group('perf')

local helpers = require('test.helper')
helpers.disable_dev_checks()


local id = 0
local function gen()
    id = id + 1
    return id
end

local function reset_gen()
    id = 0
end

g.before_all(function(g)
    g.cluster = helpers.Cluster:new({
        datadir = fio.tempdir(),
        server_command = helpers.entrypoint('srv_ddl'),
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
                alias = 's-3',
                roles = { 'customers-storage', 'crud-storage' },
                servers = {
                    { instance_uuid = helpers.uuid('d', 1), alias = 's3-master' },
                    { instance_uuid = helpers.uuid('d', 2), alias = 's3-replica' },
                },
            }
        },
    })
    g.cluster:start()

    g.router = g.cluster:server('router').net_box

    g.router:eval([[
        rawset(_G, 'crud', require('crud'))
    ]])

    -- Run real perf tests only with flag, otherwise run short version
    -- to test compatibility as part of unit/integration test run.
    g.perf_mode_on = os.getenv('PERF_MODE_ON')

    g.total_report = {}
end)

g.before_each(function(g)
    helpers.truncate_space_on_cluster(g.cluster, 'customers')
    reset_gen()
end)

local function normalize(s, n)
    if type(s) == 'number' then
        s = ('%.2f'):format(s)
    end

    local len = s:len()
    if len > n then
        return s:sub(1, n)
    end

    return (' '):rep(n - len) .. s
end

local row_name = {
    insert = 'insert',
    select_pk = 'select by pk',
    select_gt_pk = 'select gt by pk (limit 10)',
    pairs_gt = 'pairs gt by pk (limit 100)',
}

local column_name = {
    without_stats_wrapper = 'without stats wrapper',
    stats_disabled = 'stats disabled',
    local_stats = 'local stats',
    metrics_stats = 'metrics stats (no quantiles)',
    metrics_quantile_stats = 'metrics stats (with quantiles)',
}

local function visualize_section(total_report, name, comment, section, params)
    local report_str = ('== %s ==\n(%s)\n\n'):format(name, comment or '')

    local normalized_row_header = normalize('', params.row_header_width)
    local headers       = '| ' .. normalized_row_header                  .. ' |'
    local after_headers = '| ' .. ('-'):rep(normalized_row_header:len()) .. ' |'

    for _, column in ipairs(params.columns) do
        local normalized_column_header = normalize(column, params.col_width[column])
        headers       = headers       .. ' ' .. normalized_column_header                  .. ' |'
        after_headers = after_headers .. ' ' .. ('-'):rep(normalized_column_header:len()) .. ' |'
    end

    report_str = report_str .. headers       .. '\n'
    report_str = report_str .. after_headers .. '\n'

    for _, row in ipairs(params.rows) do
        local row_str = '| ' .. normalize(row, params.row_header_width) .. ' |'

        for _, column in ipairs(params.columns) do
            local report = nil
            if total_report[row] ~= nil then
                report = total_report[row][column]
            end

            local report_str
            if report ~= nil then
                report_str = report.str[section]
            else
                report_str = 'unknown'
            end

            row_str = row_str .. ' ' .. normalize(report_str, params.col_width[column]) .. ' |'
        end

        report_str = report_str .. row_str .. '\n'
    end

    report_str = report_str .. '\n\n\n'

    return report_str
end

local function visualize_report(report)
    local params = {}

    params.col_width = 2
    for _, name in pairs(column_name) do
        params.col_width = math.max(name:len() + 2, params.col_width)
    end

    params.row_header_width = 30

    -- Set columns and rows explicitly to preserve custom order.
    params.columns = {
        column_name.without_stats_wrapper,
        column_name.stats_disabled,
        column_name.local_stats,
        column_name.metrics_stats,
        column_name.metrics_quantile_stats,
    }

    params.rows = {
        row_name.select_pk,
        row_name.select_gt_pk,
        row_name.pairs_gt,
        row_name.insert,
    }

    params.row_header_width = 1
    for _, name in pairs(row_name) do
        params.row_header_width = math.max(name:len(), params.row_header_width)
    end

    local min_col_width = 12
    params.col_width = {}
    for _, name in ipairs(params.columns) do
        params.col_width[name] = math.max(name:len(), min_col_width)
    end

    local report_str = '\n==== PERFORMANCE REPORT ====\n\n\n'

    report_str = report_str .. visualize_section(report, 'SUCCESS REQUESTS',
        'The higher the better', 'success_count', params)
    report_str = report_str .. visualize_section(report, 'SUCCESS REQUESTS PER SECOND',
        'The higher the better', 'success_rps', params)
    report_str = report_str .. visualize_section(report, 'ERRORS',
        'Bad if higher than zero', 'error_count', params)
    report_str = report_str .. visualize_section(report, 'AVERAGE CALL TIME',
        'The lower the better', 'average_time', params)
    report_str = report_str .. visualize_section(report, 'MAX CALL TIME',
        'The lower the better', 'max_time', params)

    -- Expect every row to have the same run parameters.
    local comment_str = ''
    for _, row in ipairs(params.rows) do
        local row_report = report[row]
        if row_report == nil then
            goto continue
        end

        local row_report_sample = select(2, next(row_report))
        if row_report_sample == nil then
            goto continue
        end

        local row_comment = ('%q was planned for %d seconds with %d connections and %d fibers total.\n'):format(
            row,
            row_report_sample.params.timeout,
            row_report_sample.params.connection_count,
            row_report_sample.params.fiber_count)
        comment_str = comment_str .. row_comment

        ::continue::
    end

    report_str = report_str .. comment_str

    log.info(report_str)
end

g.after_each(function(g)
    g.router:call('crud.cfg', {{ stats = false }})
end)

g.after_all(function(g)
    g.cluster:stop()
    fio.rmtree(g.cluster.datadir)

    visualize_report(g.total_report)
end)

local function generate_customer()
    return { gen(), box.NULL, 'David Smith', 33 }
end

local select_prepare = function(g)
    local count
    if g.perf_mode_on then
        count = 10100
    else
        count = 100
    end

    for _ = 1, count do
        g.router:call('crud.insert', { 'customers', generate_customer() })
    end
    reset_gen()
end

local insert_params = function()
    return { 'customers', generate_customer() }
end

local select_params_pk_eq = function()
    return { 'customers', {{'==', 'id', gen() % 10000}} }
end

local select_params_pk_gt = function()
    return { 'customers', {{'>', 'id', gen() % 10000}}, { first = 10 } }
end

local pairs_params_pk_gt = function()
    return { 'customers', {{'>', 'id', gen() % 10000}}, { first = 100, batch_size = 50 } }
end

local stats_cases = {
    stats_disabled = {
        column_name = column_name.stats_disabled,
    },
    local_stats = {
        prepare = function(g)
            g.router:call('crud.cfg', {{ stats = true, stats_driver = 'local', stats_quantiles = false }})
        end,
        column_name = column_name.local_stats,
    },
    metrics_stats = {
        prepare = function(g)
            local is_metrics_supported = g.router:eval([[
                return require('crud.stats.metrics_registry').is_supported()
            ]])
            t.skip_if(is_metrics_supported == false, 'Metrics registry is unsupported')
            g.router:call('crud.cfg', {{ stats = true, stats_driver = 'metrics', stats_quantiles = false }})
        end,
        column_name = column_name.metrics_stats,
    },
    metrics_quantile_stats = {
        prepare = function(g)
            local is_metrics_supported = g.router:eval([[
                return require('crud.stats.metrics_registry').is_supported()
            ]])
            t.skip_if(is_metrics_supported == false, 'Metrics registry is unsupported')
            g.router:call('crud.cfg', {{ stats = true, stats_driver = 'metrics', stats_quantiles = true }})
        end,
        column_name = column_name.metrics_quantile_stats,
    },
}

local integration_params = {
    timeout = 2,
    fiber_count = 5,
    connection_count = 2,
}

local pairs_integration = {
    timeout = 5,
    fiber_count = 1,
    connection_count = 1,
}

local insert_perf = {
    timeout = 30,
    fiber_count = 600,
    connection_count = 10,
}

-- Higher load may lead to net_msg_max limit break.
local select_perf = {
    timeout = 30,
    fiber_count = 200,
    connection_count = 10,
}

local pairs_perf = {
    timeout = 30,
    fiber_count = 100,
    connection_count = 10,
}

local cases = {
    crud_insert = {
        call = 'crud.insert',
        params = insert_params,
        matrix = stats_cases,
        integration_params = integration_params,
        perf_params = insert_perf,
        row_name = row_name.insert,
    },

    crud_insert_without_stats_wrapper = {
        prepare = function(g)
            g.router:eval([[
                rawset(_G, '_plain_insert', require('crud.insert').tuple)
            ]])
        end,
        call = '_plain_insert',
        params = insert_params,
        matrix = { [''] = { column_name = column_name.without_stats_wrapper } },
        integration_params = integration_params,
        perf_params = insert_perf,
        row_name = row_name.insert,
    },

    crud_select_pk_eq = {
        prepare = select_prepare,
        call = 'crud.select',
        params = select_params_pk_eq,
        matrix = stats_cases,
        integration_params = integration_params,
        perf_params = select_perf,
        row_name = row_name.select_pk,
    },

    crud_select_without_stats_wrapper_pk_eq = {
        prepare = function(g)
            g.router:eval("_plain_select = require('crud.select').call")
            select_prepare(g)
        end,
        call = '_plain_select',
        params = select_params_pk_eq,
        matrix = { [''] = { column_name = column_name.without_stats_wrapper } },
        integration_params = integration_params,
        perf_params = select_perf,
        row_name = row_name.select_pk,
    },

    crud_select_pk_gt = {
        prepare = select_prepare,
        call = 'crud.select',
        params = select_params_pk_gt,
        matrix = stats_cases,
        integration_params = integration_params,
        perf_params = select_perf,
        row_name = row_name.select_gt_pk,
    },

    crud_select_without_stats_wrapper_pk_gt = {
        prepare = function(g)
            g.router:eval([[
                rawset(_G, '_plain_select', require('crud.select').call)
            ]])
            select_prepare(g)
        end,
        call = '_plain_select',
        params = select_params_pk_gt,
        matrix = { [''] = { column_name = column_name.without_stats_wrapper } },
        integration_params = integration_params,
        perf_params = select_perf,
        row_name = row_name.select_gt_pk,
    },

    crud_pairs_gt = {
        prepare = function(g)
            g.router:eval([[
                _run_pairs = function(...)
                    local t = {}
                    for _, tuple in require('crud').pairs(...) do
                        table.insert(t, tuple)
                    end

                    return t
                end
            ]])
            select_prepare(g)
        end,
        call = '_run_pairs',
        params = pairs_params_pk_gt,
        matrix = stats_cases,
        integration_params = pairs_integration,
        perf_params = pairs_perf,
        row_name = row_name.pairs_gt,
    },

    crud_pairs_without_stats_wrapper_pk_gt = {
        prepare = function(g)
            g.router:eval([[
                _run_pairs = function(...)
                    local t = {}
                    for _, tuple in require('crud.select').pairs(...) do
                        table.insert(t, tuple)
                    end

                    return t
                end
            ]])
            select_prepare(g)
        end,
        call = '_run_pairs',
        params = pairs_params_pk_gt,
        matrix = { [''] = { column_name = column_name.without_stats_wrapper } },
        integration_params = pairs_integration,
        perf_params = pairs_perf,
        row_name = row_name.pairs_gt,
    },
}

local function generator_f(conn, call, params, report, timeout)
    local start = clock.monotonic()

    while (clock.monotonic() - start) < timeout do
        local call_start = clock.monotonic()
        local ok, res, err = pcall(conn.call, conn, call, params())
        local call_time = clock.monotonic() - call_start

        if not ok then
            log.error(res)
            table.insert(report.errors, res)
        elseif err ~= nil then
            errors.wrap(err)
            log.error(err)
            table.insert(report.errors, err)
        else
            report.count = report.count + 1
        end

        report.total_time = report.total_time + call_time
        report.max_time = math.max(report.max_time, call_time)
    end
end

for name, case in pairs(cases) do
    local matrix = case.matrix or { [''] = { { column_name = '' } } }

    for subname, subcase in pairs(matrix) do
        local name_tail = ''
        if subname ~= '' then
            name_tail = ('_with_%s'):format(subname)
        end

        local test_name = ('test_%s%s'):format(name, name_tail)

        g.before_test(test_name, function(g)
            if case.prepare ~= nil then
                case.prepare(g)
            end

            if subcase.prepare ~= nil then
                subcase.prepare(g)
            end
        end)

        g[test_name] = function(g)
            local params
            if g.perf_mode_on then
                params = case.perf_params
            else
                params = case.integration_params
            end

            local connections = {}

            local router = g.cluster:server('router')
            for _ = 1, params.connection_count do
                local c = net_box:connect(router.net_box_uri, router.net_box_credentials)
                if c == nil then
                    t.fail('Failed to prepare connections')
                end
                table.insert(connections, c)
            end

            local fibers = {}
            local report = { errors = {}, count = 0, total_time = 0, max_time = 0 }
            for id = 1, params.fiber_count do
                local conn_id = id % params.connection_count + 1
                local conn = connections[conn_id]
                local f = fiber.new(generator_f, conn, case.call, case.params, report, params.timeout)
                f:set_joinable(true)
                table.insert(fibers, f)
            end

            local start_time = clock.monotonic()
            for i = 1, params.fiber_count do
                fibers[i]:join()
            end
            local run_time = clock.monotonic() - start_time

            report.str = {
                success_count = ('%d'):format(report.count),
                error_count = ('%d'):format(#report.errors),
                success_rps = ('%.2f'):format(report.count / run_time),
                max_time = ('%.3f ms'):format(report.max_time * 1e3),
            }

            report.params = params

            local total_count = report.count + #report.errors
            if total_count > 0 then
                report.str.average_time = ('%.3f ms'):format(report.total_time / total_count * 1e3)
            else
                report.str.average_time = 'unknown'
            end

            g.total_report[case.row_name] = g.total_report[case.row_name] or {}
            g.total_report[case.row_name][subcase.column_name] = report

            log.info('\n%s: %s success requests (rps %s), %s errors, call average time %s, call max time %s \n',
                test_name, report.str.success_count, report.str.success_rps, report.str.error_count,
                report.str.average_time, report.str.max_time)
        end
    end
end
