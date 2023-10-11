local fio = require('fio')
local clock = require('clock')
local fiber = require('fiber')
local errors = require('errors')
local net_box = require('net.box')
local log = require('log')
local fun = require('fun')

local t = require('luatest')

local helpers = require('test.helper')

local g = t.group('perf', helpers.backend_matrix())

local id = 0
local function gen()
    id = id + 1
    return id
end

local function reset_gen()
    id = 0
end

local vshard_cfg_template = {
    sharding = {
        {
            replicas = {
                ['s1-master'] = {
                    master = true,
                },
                ['s1-replica'] = {},
            },
        },
        {
            replicas = {
                ['s2-master'] = {
                    master = true,
                },
                ['s2-replica'] = {},
            },
        },
        {
            replicas = {
                ['s3-master'] = {
                    master = true,
                },
                ['s3-replica'] = {},
            },
        },
    },
    bucket_count = 3000,
    storage_init = helpers.entrypoint_vshard_storage('srv_ddl'),
    crud_init = true,
}

local cartridge_cfg_template = {
    datadir = fio.tempdir(),
    server_command = helpers.entrypoint_cartridge('srv_ddl'),
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
}

g.before_all(function(g)
    -- Run real perf tests only with flag, otherwise run short version
    -- to test compatibility as part of unit/integration test run.
    g.perf_mode_on = os.getenv('PERF_MODE_ON')

    if g.perf_mode_on then
        g.old_dev_checks_value = os.getenv('TARANTOOL_CRUD_ENABLE_INTERNAL_CHECKS')
        helpers.disable_dev_checks()
    end

    helpers.start_cluster(g, cartridge_cfg_template, vshard_cfg_template)

    g.router = helpers.get_router(g.cluster, g.params.backend).net_box

    g.router:eval([[
        rawset(_G, 'crud', require('crud'))
    ]])

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

local function generate_batch_insert_cases(min_value, denominator, count)
    local matrix = {}
    local cols_names = {}

    local batch_size = min_value
    for _ = 1, count do
        local col_name = tostring(batch_size)
        table.insert(cols_names, col_name)
        matrix[col_name] = { column_name = col_name, arg = batch_size }
        batch_size = batch_size * denominator
    end

    return {
        matrix = matrix,
        cols_names = cols_names,
    }
end

local min_batch_size = 1
local denominator_batch_size = 10
local count_batch_cases = 5
local batch_insert_cases = generate_batch_insert_cases(min_batch_size, denominator_batch_size, count_batch_cases)

local row_name = {
    insert = 'insert',
    insert_many = 'insert_many',
    insert_many_noreturn = 'insert_many (noreturn)',
    select_pk = 'select by pk',
    select_gt_pk = 'select gt by pk (limit 10)',
    select_secondary_eq = 'select eq by secondary (limit 10)',
    select_secondary_sharded = 'select eq by sharding secondary',
    pairs_gt = 'pairs gt by pk (limit 100)',
}

local column_name = {
    vshard = 'vshard',
    without_stats_wrapper = 'crud (without stats wrapper)',
    stats_disabled = 'crud (stats disabled)',
    bucket_id = 'crud (stats disabled, known bucket_id)',
    local_stats = 'crud (local stats)',
    metrics_stats = 'crud (metrics stats, no quantiles)',
    metrics_quantile_stats = 'crud (metrics stats, with quantiles)',
}

-- insert column names for insert_many, insert_many (noreturn) and insert comparison cases
fun.reduce(
        function(list, value) list[value] = value return list end,
        column_name, pairs(batch_insert_cases.cols_names)
)

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
                report_str = ''
            end

            row_str = row_str .. ' ' .. normalize(report_str, params.col_width[column]) .. ' |'
        end

        report_str = report_str .. row_str .. '\n'
    end

    report_str = report_str .. '\n\n\n'

    return report_str
end

local function visualize_report(report, title, params)
    params.col_width = 2
    for _, name in pairs(column_name) do
        params.col_width = math.max(name:len() + 2, params.col_width)
    end

    params.row_header_width = 1
    for _, name in pairs(row_name) do
        params.row_header_width = math.max(name:len(), params.row_header_width)
    end

    local min_col_width = 12
    params.col_width = {}
    for _, name in ipairs(params.columns) do
        params.col_width[name] = math.max(name:len(), min_col_width)
    end

    local report_str = ('\n==== %s ====\n\n\n'):format(title)

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
    helpers.stop_cluster(g.cluster, g.params.backend)

    visualize_report(g.total_report, 'STATISTICS PERFORMANCE REPORT', {
        columns = {
            column_name.without_stats_wrapper,
            column_name.stats_disabled,
            column_name.local_stats,
            column_name.metrics_stats,
            column_name.metrics_quantile_stats,
        },

        rows = {
            row_name.select_pk,
            row_name.select_gt_pk,
            row_name.pairs_gt,
            row_name.select_secondary_eq,
            row_name.select_secondary_sharded,
            row_name.insert,
            row_name.insert_many,
            row_name.insert_many_noreturn,
        }
    })

    visualize_report(g.total_report, 'VSHARD COMPARISON PERFORMANCE REPORT', {
        columns = {
            column_name.vshard,
            column_name.stats_disabled,
            column_name.bucket_id,
        },

        rows = {
            row_name.select_pk,
            row_name.select_gt_pk,
            row_name.select_secondary_eq,
            row_name.select_secondary_sharded,
            row_name.insert,
        }
    })

    visualize_report(g.total_report, 'BATCH COMPARISON PERFORMANCE REPORT', {
        columns = batch_insert_cases.cols_names,

        rows = {
            row_name.insert,
            row_name.insert_many,
            row_name.insert_many_noreturn,
        },
    })

    if g.perf_mode_on then
        os.setenv('TARANTOOL_CRUD_ENABLE_INTERNAL_CHECKS', g.old_dev_checks_value)
    end
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

local select_sharded_by_secondary_prepare = function(g)
    local count
    if g.perf_mode_on then
        count = 10100
    else
        count = 100
    end

    for _ = 1, count do
        g.router:call(
            'crud.insert',
            {
                'customers_name_age_key_different_indexes',
                { gen(), box.NULL, 'David Smith', gen() % 50 + 18 }
            }
        )
    end
    reset_gen()
end

local vshard_prepare = function(g)
    g.router:eval([[
        local vshard = require('vshard')

        local function _vshard_insert(space_name, tuple)
            local replicaset = select(2, next(vshard.router.routeall()))
            local space = replicaset.master.conn.space[space_name]
            assert(space ~= nil)

            local bucket_id = vshard.router.bucket_id_strcrc32(tuple[1])

            assert(space.index.bucket_id ~= nil)
            tuple[space.index.bucket_id.parts[1].fieldno] = bucket_id

            return vshard.router.callrw(
                bucket_id,
                '_vshard_insert_storage',
                { space_name, tuple, bucket_id }
            )
        end

        rawset(_G, '_vshard_insert', _vshard_insert)


        local function _vshard_select(space_name, key)
            local bucket_id = vshard.router.bucket_id_strcrc32(key)
            return vshard.router.callrw(
                bucket_id,
                '_vshard_select_storage',
                { space_name, key }
            )
        end

        rawset(_G, '_vshard_select', _vshard_select)


        local function pk_sort(a, b)
            return a[1] < b[1]
        end

        local function _vshard_select_gt(space, key, opts)
            assert(type(opts.limit) == 'number')
            assert(opts.limit > 0)

            local tuples = {}

            for id, replicaset in pairs(vshard.router.routeall()) do
                local resp, err = replicaset:call(
                    '_vshard_select_storage',
                    { space, key, nil, box.index.GT, opts.limit }
                )
                if err ~= nil then
                    error(err)
                end

                for _, v in ipairs(resp) do
                    table.insert(tuples, v)
                end

            end

            -- Merger.
            local response = { }

            table.sort(tuples, pk_sort)

            for i = 1, opts.limit do
                response[i] = tuples[i]
            end

            return response
        end

        rawset(_G, '_vshard_select_gt', _vshard_select_gt)


        local function _vshard_select_secondary(space_name, index_name, key, opts)
            assert(type(opts.limit) == 'number')
            assert(opts.limit > 0)

            local tuples = {}

            for id, replicaset in pairs(vshard.router.routeall()) do
                local resp, err = replicaset:call(
                    '_vshard_select_storage',
                    { space_name, key, index_name, box.index.EQ, opts.limit }
                )
                if err ~= nil then
                    error(err)
                end

                for _, tuple in ipairs(resp) do
                    table.insert(tuples, tuple)
                end
            end

            local replicaset = select(2, next(vshard.router.routeall()))
            local space = replicaset.master.conn.space[space_name]
            assert(space ~= nil)

            local id = space.index[index_name].parts[1].fieldno

            local function sec_sort(a, b)
                return a[id] < b[id]
            end

            -- Merger.
            local response = { }

            table.sort(tuples, sec_sort)

            for i = 1, opts.limit do
                response[i] = tuples[i]
            end

            return response
        end

        rawset(_G, '_vshard_select_secondary', _vshard_select_secondary)


        local function _vshard_select_customer_by_name_and_age(key)
            local bucket_id = vshard.router.bucket_id_strcrc32(key)

            return vshard.router.callrw(
                bucket_id,
                '_vshard_select_customer_by_name_and_age_storage',
                { key }
            )
        end

        rawset(_G, '_vshard_select_customer_by_name_and_age', _vshard_select_customer_by_name_and_age)
    ]])

    for _, server in ipairs(g.cluster.servers) do
        server.net_box:eval([[
            local function add_storage_execute(func_name)
                if box.cfg.read_only == false and box.schema.user.exists('storage') then
                    box.schema.func.create(func_name, {setuid = true, if_not_exists = true})
                    box.schema.user.grant('storage', 'execute', 'function', func_name,
                        {if_not_exists = true})
                end
            end

            local function _vshard_insert_storage(space_name, tuple, bucket_id)
                local space = box.space[space_name]
                assert(space ~= nil)

                local ok = space:insert(tuple)
                assert(ok ~= nil)
            end

            rawset(_G, '_vshard_insert_storage', _vshard_insert_storage)
            add_storage_execute('_vshard_insert_storage')

            local function _vshard_select_storage(space_name, key, index_name, iterator, limit)
                local space = box.space[space_name]
                assert(space ~= nil)

                local index = nil
                if index_name == nil then
                    index = box.space[space_name].index[0]
                else
                    index = box.space[space_name].index[index_name]
                end
                assert(index ~= nil)

                iterator = iterator or box.index.EQ
                return index:select(key, { limit = limit, iterator = iterator })
            end

            rawset(_G, '_vshard_select_storage', _vshard_select_storage)
            add_storage_execute('_vshard_select_storage')

            local function _vshard_select_customer_by_name_and_age_storage(key)
                local space = box.space.customers_name_age_key_different_indexes
                local index = space.index.age

                for _, tuple in index:pairs(key[2]) do
                    if tuple.name == key[1] then
                        return { tuple }
                    end
                end
                return {}
            end

            rawset(_G, '_vshard_select_customer_by_name_and_age_storage',
                _vshard_select_customer_by_name_and_age_storage)
            add_storage_execute('_vshard_select_customer_by_name_and_age_storage')
        ]])
    end
end

local insert_params = function()
    return { 'customers', generate_customer() }
end

local batch_insert_params = function(count)
    local batch = {}

    count = count or 1

    for _ = 1, count do
        table.insert(batch, generate_customer())
    end

    return { 'customers', batch }
end

local batch_insert_params_with_noreturn = function(count)
    local batch = {}

    count = count or 1

    for _ = 1, count do
        table.insert(batch, generate_customer())
    end

    return { 'customers', batch, { noreturn=true } }
end

local select_params_pk_eq = function()
    return { 'customers', {{'==', 'id', gen() % 10000}} }
end

local select_params_pk_eq_bucket_id = function()
    local id = gen() % 10000
    return { 'customers', {{'==', 'id', id}}, id }
end

local vshard_select_params_pk_eq = function()
    return { 'customers', gen() % 10000 }
end

local select_params_pk_gt = function()
    return { 'customers', {{'>', 'id', gen() % 10000}}, { first = 10 } }
end

local vshard_select_params_pk_gt = function()
    return { 'customers', gen() % 10000, { limit = 10 } }
end

local select_params_secondary_eq = function()
    return { 'customers', {{'==', 'age', 33}}, { first = 10 } }
end

local vshard_select_params_secondary_eq = function()
    return { 'customers', 'age', 33, { limit = 10 } }
end

local select_params_sharded_by_secondary = function()
    return {
        'customers_name_age_key_different_indexes',
        { { '==', 'name', 'David Smith' }, { '==', 'age', gen() % 50 + 18 }, },
        { first = 1 }
    }
end

local select_params_sharded_by_secondary_bucket_id = function()
    local age = gen() % 50 + 18
    return {
        'customers_name_age_key_different_indexes',
        { { '==', 'name', 'David Smith' }, { '==', 'age', age } },
        { first = 1 },
        age
    }
end

local vshard_select_params_sharded_by_secondary = function()
    return {{ 'David Smith', gen() % 50 + 18 }}
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

local batch_insert_comparison_perf = {
    timeout = 30,
    fiber_count = 1,
    connection_count = 1,
}

local cases = {
    vshard_insert = {
        prepare = vshard_prepare,
        call = '_vshard_insert',
        params = insert_params,
        matrix = { [''] = { column_name = column_name.vshard } },
        integration_params = integration_params,
        perf_params = insert_perf,
        row_name = row_name.insert,
    },

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

    crud_insert_many = {
        call = 'crud.insert_many',
        params = batch_insert_params,
        matrix = stats_cases,
        integration_params = integration_params,
        perf_params = insert_perf,
        row_name = row_name.insert_many,
    },

    crud_insert_many_without_stats_wrapper = {
        prepare = function(g)
            g.router:eval([[
                rawset(_G, '_plain_insert_many', require('crud.insert_many').tuples)
            ]])
        end,
        call = '_plain_insert_many',
        params = batch_insert_params,
        matrix = { [''] = { column_name = column_name.without_stats_wrapper } },
        integration_params = integration_params,
        perf_params = batch_insert_comparison_perf,
        row_name = row_name.insert_many,
    },

    crud_insert_many_noreturn = {
        call = 'crud.insert_many',
        params = batch_insert_params_with_noreturn,
        matrix = stats_cases,
        integration_params = integration_params,
        perf_params = insert_perf,
        row_name = row_name.insert_many_noreturn,
    },

    crud_insert_many_noreturn_without_stats_wrapper = {
        prepare = function(g)
            g.router:eval([[
                rawset(_G, '_plain_insert_many', require('crud.insert_many').tuples)
            ]])
        end,
        call = '_plain_insert_many',
        params = batch_insert_params_with_noreturn,
        matrix = { [''] = { column_name = column_name.without_stats_wrapper } },
        integration_params = integration_params,
        perf_params = batch_insert_comparison_perf,
        row_name = row_name.insert_many_noreturn,
    },

    vshard_select_pk_eq = {
        prepare = function(g)
            select_prepare(g)
            vshard_prepare(g)
        end,
        call = '_vshard_select',
        params = vshard_select_params_pk_eq,
        matrix = { [''] = { column_name = column_name.vshard } },
        integration_params = integration_params,
        perf_params = select_perf,
        row_name = row_name.select_pk,
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

    crud_select_known_bucket_id_pk_eq = {
        prepare = function(g)
            select_prepare(g)

            g.router:eval([[
                local vshard = require('vshard')

                local function _crud_select_bucket(space_name, conditions, sharding_key)
                    local bucket_id = vshard.router.bucket_id_strcrc32(sharding_key)
                    return crud.select(space_name, conditions, { bucket_id = bucket_id })
                end

                rawset(_G, '_crud_select_bucket', _crud_select_bucket)
            ]])
        end,
        call = '_crud_select_bucket',
        params = select_params_pk_eq_bucket_id,
        matrix = { [''] = { column_name = column_name.bucket_id } },
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

    vshard_select_pk_gt = {
        prepare = function(g)
            select_prepare(g)
            vshard_prepare(g)
        end,
        call = '_vshard_select_gt',
        params = vshard_select_params_pk_gt,
        matrix = { [''] = { column_name = column_name.vshard } },
        integration_params = integration_params,
        perf_params = select_perf,
        row_name = row_name.select_gt_pk,
    },

    crud_select_secondary_eq = {
        prepare = select_prepare,
        call = 'crud.select',
        params = select_params_secondary_eq,
        matrix = stats_cases,
        integration_params = integration_params,
        perf_params = select_perf,
        row_name = row_name.select_secondary_eq,
    },

    crud_select_without_stats_wrapper_secondary_eq = {
        prepare = function(g)
            g.router:eval("_plain_select = require('crud.select').call")
            select_prepare(g)
        end,
        call = '_plain_select',
        params = select_params_secondary_eq,
        matrix = { [''] = { column_name = column_name.without_stats_wrapper } },
        integration_params = integration_params,
        perf_params = select_perf,
        row_name = row_name.select_secondary_eq,
    },

    vshard_select_secondary_eq = {
        prepare = function(g)
            select_prepare(g)
            vshard_prepare(g)
        end,
        call = '_vshard_select_secondary',
        params = vshard_select_params_secondary_eq,
        matrix = { [''] = { column_name = column_name.vshard } },
        integration_params = integration_params,
        perf_params = select_perf,
        row_name = row_name.select_secondary_eq,
    },

    crud_select_sharding_secondary_eq = {
        prepare = select_sharded_by_secondary_prepare,
        call = 'crud.select',
        params = select_params_sharded_by_secondary,
        matrix = stats_cases,
        integration_params = integration_params,
        perf_params = select_perf,
        row_name = row_name.select_secondary_sharded,
    },

    crud_select_sharding_secondary_eq_bucket_id = {
        prepare = function(g)
            select_sharded_by_secondary_prepare(g)

            g.router:eval([[
                local vshard = require('vshard')

                local function _crud_select_bucket_secondary(space_name, conditions, opts, sharding_key)
                    local bucket_id = vshard.router.bucket_id_strcrc32(sharding_key)
                    opts.bucket_id = bucket_id
                    return crud.select(space_name, conditions, opts)
                end

                rawset(_G, '_crud_select_bucket_secondary', _crud_select_bucket_secondary)
            ]])
        end,
        call = '_crud_select_bucket_secondary',
        params = select_params_sharded_by_secondary_bucket_id,
        matrix = { [''] = { column_name = column_name.bucket_id } },
        integration_params = integration_params,
        perf_params = select_perf,
        row_name = row_name.select_secondary_sharded,
    },

    crud_select_without_stats_wrapper_sharding_secondary_eq = {
        prepare = function(g)
            g.router:eval("_plain_select = require('crud.select').call")
            select_sharded_by_secondary_prepare(g)
        end,
        call = '_plain_select',
        params = select_params_sharded_by_secondary,
        matrix = { [''] = { column_name = column_name.without_stats_wrapper } },
        integration_params = integration_params,
        perf_params = select_perf,
        row_name = row_name.select_secondary_sharded,
    },

    vshard_select_sharding_secondary_eq = {
        prepare = function(g)
            select_sharded_by_secondary_prepare(g)
            vshard_prepare(g)
        end,
        call = '_vshard_select_customer_by_name_and_age',
        params = vshard_select_params_sharded_by_secondary,
        matrix = { [''] = { column_name = column_name.vshard } },
        integration_params = integration_params,
        perf_params = select_perf,
        row_name = row_name.select_secondary_sharded,
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

    crud_insert_loop = {
        prepare = function(g)
            g.router:eval([[
                _insert_loop = function(space_name, tuples)
                    local results
                    local errors

                    for _, tuple in ipairs(tuples) do
                         local res, err = crud.insert(space_name, tuple)
                         if res ~= nil then
                             results = results or {}
                             table.insert(results, res)
                         end

                         if err ~= nil then
                             errors = errors or {}
                             table.insert(errors, err)
                         end
                    end

                    return results, errors
                end
            ]])
        end,
        call = '_insert_loop',
        params = batch_insert_params,
        matrix = batch_insert_cases.matrix,
        integration_params = integration_params,
        perf_params = batch_insert_comparison_perf,
        row_name = row_name.insert,
    },

    crud_insert_many_different_batch_size = {
        call = 'crud.insert_many',
        params = batch_insert_params,
        matrix = batch_insert_cases.matrix,
        integration_params = integration_params,
        perf_params = batch_insert_comparison_perf,
        row_name = row_name.insert_many,
    },

    crud_insert_many_noreturn_different_batch_size = {
        call = 'crud.insert_many',
        params = batch_insert_params_with_noreturn,
        matrix = batch_insert_cases.matrix,
        integration_params = integration_params,
        perf_params = batch_insert_comparison_perf,
        row_name = row_name.insert_many_noreturn,
    },
}

local function generator_f(conn, call, params, report, timeout, arg)
    local start = clock.monotonic()

    while (clock.monotonic() - start) < timeout do
        local call_start = clock.monotonic()
        local ok, res, err = pcall(conn.call, conn, call, params(arg))
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

            local router = helpers.get_router(g.cluster, g.params.backend)
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
                local f = fiber.new(generator_f, conn, case.call, case.params, report, params.timeout, subcase.arg)
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
