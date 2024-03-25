local clock = require('clock')
local helpers = require('test.helper')
local t = require('luatest')

local stats_registry_utils = require('crud.stats.registry_utils')

local matrix = helpers.backend_matrix({
    { way = 'call', args = { driver = 'local' }},
    { way = 'call', args = { driver = 'metrics', quantiles = false }},
    { way = 'call', args = { driver = 'metrics', quantiles = true }},
})
table.insert(matrix, {backend = helpers.backend.CARTRIDGE,
    way = 'role', args = { driver = 'local' },
})
table.insert(matrix, {backend = helpers.backend.CARTRIDGE,
    way = 'role', args = { driver = 'metrics', quantiles = false },
})
table.insert(matrix, {backend = helpers.backend.CARTRIDGE,
    way = 'role', args = { driver = 'metrics', quantiles = true },
})
local pgroup = t.group('stats_integration', matrix)

matrix = helpers.backend_matrix({
    { way = 'call', args = { driver = 'metrics', quantiles = false }},
})
table.insert(matrix, {backend = helpers.backend.CARTRIDGE,
    way = 'role', args = { driver = 'metrics', quantiles = true },
})
local group_metrics = t.group('stats_metrics_integration', matrix)

local space_name = 'customers'
local non_existing_space_name = 'non_existing_space'
local new_space_name = 'newspace'

local function before_all(g)
    helpers.start_default_cluster(g, 'srv_stats')

    if g.params.args.driver == 'metrics' then
        local is_metrics_supported = g.router:eval([[
            return require('crud.stats.metrics_registry').is_supported()
        ]])
        t.skip_if(is_metrics_supported == false, 'Metrics registry is unsupported')
    end
end

local function after_all(g)
    helpers.stop_cluster(g.cluster, g.params.backend)
end

local function get_stats(g, space_name)
    return g.router:eval("return require('crud').stats(...)", { space_name })
end

local call_cfg = function(g, way, cfg)
    if way == 'call' then
        g.router:eval([[
            require('crud').cfg(...)
        ]], { cfg })
    elseif way == 'role' then
        g.router:upload_config{crud = cfg}
    end
end

local function enable_stats(g, args)
    args = args or g.params.args

    local cfg = {
        stats = true,
        stats_driver = args.driver,
        stats_quantiles = args.quantiles,
        stats_quantile_tolerated_error = 1e-3,
        stats_quantile_age_buckets_count = 3,
        stats_quantile_max_age_time = 60,
    }

    call_cfg(g, g.params.way, cfg)
end

local function disable_stats(g)
    local cfg = {
        stats = false,
    }

    call_cfg(g, g.params.way, cfg)
end

local function before_each(g)
    g.router:eval("crud = require('crud')")
    enable_stats(g)
    helpers.truncate_space_on_cluster(g.cluster, space_name)
    helpers.drop_space_on_cluster(g.cluster, new_space_name)
end

local function get_metrics(g)
    return g.router:eval("return require('metrics').collect()")
end

pgroup.before_all(before_all)

pgroup.after_all(after_all)

pgroup.before_each(before_each)

pgroup.after_each(disable_stats)


group_metrics.before_all(before_all)

group_metrics.after_all(after_all)

group_metrics.before_each(before_each)

group_metrics.after_each(disable_stats)


local function create_new_space(g)
    helpers.call_on_storages(g.cluster, function(server)
        server:exec(function(space_name)
            if not box.info.ro then
                local sp = box.schema.space.create(space_name, {
                    format = {
                        {name = 'id', type = 'unsigned'},
                        {name = 'bucket_id', type = 'unsigned'},
                    },
                    if_not_exists = true,
                })

                sp:create_index('pk', {
                    parts = { {field = 'id'} },
                    if_not_exists = true,
                })

                sp:create_index('bucket_id', {
                    parts = { {field = 'bucket_id'} },
                    unique = false,
                    if_not_exists = true,
                })
            end
        end, {new_space_name})
    end)
end

local function truncate_space_on_cluster(g)
    helpers.truncate_space_on_cluster(g.cluster, space_name)
end

-- If there weren't any operations, space stats is {}.
-- To compute stats diff, this helper return real stats
-- if they're already present or default stats if
-- this operation of space hasn't been observed yet.
local function set_defaults_if_empty(space_stats, op)
    if space_stats[op] ~= nil then
        return space_stats[op]
    else
        return stats_registry_utils.build_collectors(op)
    end
end

local eval = {
    pairs = [[
        local space_name = select(1, ...)
        local conditions = select(2, ...)
        local opts = select(3, ...)

        local result = {}
        for _, v in crud.pairs(space_name, conditions, opts) do
            table.insert(result, v)
        end

        return result
    ]],

    pairs_pcall = [[
        local space_name = select(1, ...)
        local conditions = select(2, ...)
        local opts = select(3, ...)

        local _, err = pcall(crud.pairs, space_name, conditions, opts)

        return nil, tostring(err)
    ]],
}

local simple_operation_cases = {
    insert = {
        func = 'crud.insert',
        args = {
            space_name,
            { 12, box.NULL, 'Ivan', 'Ivanov', 20, 'Moscow' },
        },
        op = 'insert',
    },
    insert_object = {
        func = 'crud.insert_object',
        args = {
            space_name,
            { id = 13, name = 'Ivan', last_name = 'Ivanov', age = 20, city = 'Moscow' },
        },
        op = 'insert',
    },
    insert_many = {
        func = 'crud.insert_many',
        args = {
            space_name,
            {
                { 21, box.NULL, 'Ivan', 'Ivanov', 20, 'Moscow' },
                { 31, box.NULL, 'Oleg', 'Petrov', 25, 'Moscow' },
            }
        },
        op = 'insert_many',
    },
    insert_object_many = {
        func = 'crud.insert_object_many',
        args = {
            space_name,
            {
                { id = 22, name = 'Ivan', last_name = 'Ivanov', age = 20, city = 'Moscow' },
                { id = 32, name = 'Oleg', last_name = 'Petrov', age = 25, city = 'Moscow' },
            }
        },
        op = 'insert_many',
    },
    get = {
        func = 'crud.get',
        args = { space_name, { 12 }, {mode = 'write'}, },
        op = 'get',
    },
    select = {
        func = 'crud.select',
        args = { space_name, {{ '==', 'id_index', 3 }}, {mode = 'write'}, },
        op = 'select',
    },
    pairs = {
        eval = eval.pairs,
        args = { space_name, {{ '==', 'id_index', 3 }}, {batch_size = 1, mode = 'write'}, },
        op = 'select',
    },
    replace = {
        func = 'crud.replace',
        args = {
            space_name,
            { 12, box.NULL, 'Ivan', 'Ivanov', 20, 'Moscow' },
        },
        op = 'replace',
    },
    replace_object = {
        func = 'crud.replace_object',
        args = {
            space_name,
            { id = 12, name = 'Ivan', last_name = 'Ivanov', age = 20, city = 'Moscow' },
        },
        op = 'replace',
    },
    replace_many = {
        func = 'crud.replace_many',
        args = {
            space_name,
            {
                { 21, box.NULL, 'Peter', 'Ivanov', 40, 'Moscow' },
                { 31, box.NULL, 'Ivan', 'Petrov', 35, 'Moscow' },
            }
        },
        op = 'replace_many',
    },
    replace_object_many = {
        func = 'crud.replace_object_many',
        args = {
            space_name,
            {
                { id = 22, name = 'Peter', last_name = 'Ivanov', age = 40, city = 'Moscow' },
                { id = 32, name = 'Ivan', last_name = 'Petrov', age = 35, city = 'Moscow' },
            }
        },
        op = 'replace_many',
    },
    update = {
        prepare = function(g)
            helpers.insert_objects(g, space_name, {{
                id = 15, name = 'Ivan', last_name = 'Ivanov',
                age = 20, city = 'Moscow'
            }})
        end,
        func = 'crud.update',
        args = { space_name, 12, {{'+', 'age', 10}} },
        op = 'update',
    },
    upsert = {
        func = 'crud.upsert',
        args = {
            space_name,
            { 16, box.NULL, 'Ivan', 'Ivanov', 20, 'Moscow' },
            {{'+', 'age', 1}},
        },
        op = 'upsert',
    },
    upsert_object = {
        func = 'crud.upsert_object',
        args = {
            space_name,
            { id = 17, name = 'Ivan', last_name = 'Ivanov', age = 20, city = 'Moscow' },
            {{'+', 'age', 1}}
        },
        op = 'upsert',
    },
    upsert_many = {
        func = 'crud.upsert_many',
        args = {
            space_name,
            {
                {{ 26, box.NULL, 'Ivan', 'Ivanov', 20, 'Moscow' }, {{'+', 'age', 1}}},
                {{ 36, box.NULL, 'Oleg', 'Petrov', 25, 'Moscow' }, {{'+', 'age', 1}}},
            },
        },
        op = 'upsert_many',
    },
    upsert_object_many = {
        func = 'crud.upsert_object_many',
        args = {
            space_name,
            {
                {{ id = 27, name = 'Ivan', last_name = 'Ivanov', age = 20, city = 'Moscow' }, {{'+', 'age', 1}}},
                {{ id = 37, name = 'Oleg', last_name = 'Petrov', age = 25, city = 'Moscow' }, {{'+', 'age', 1}}},
            },
        },
        op = 'upsert_many',
    },
    delete = {
        func = 'crud.delete',
        args = { space_name, { 12 } },
        op = 'delete',
    },
    truncate = {
        func = 'crud.truncate',
        args = { space_name },
        op = 'truncate',
    },
    len = {
        func = 'crud.len',
        args = { space_name },
        op = 'len',
    },
    count = {
        func = 'crud.count',
        args = { space_name, {{ '==', 'id_index', 3 }}, {mode = 'write'}, },
        op = 'count',
    },
    min = {
        func = 'crud.min',
        args = { space_name, nil, {mode = 'write'}, },
        op = 'borders',
    },
    max = {
        func = 'crud.max',
        args = { space_name, nil, {mode = 'write'}, },
        op = 'borders',
    },
    insert_error = {
        func = 'crud.insert',
        args = { space_name, { 'id' } },
        op = 'insert',
        expect_error = true,
    },
    insert_object_error = {
        func = 'crud.insert_object',
        args = { space_name, { 'id' } },
        op = 'insert',
        expect_error = true,
    },
    insert_many_error = {
        func = 'crud.insert_many',
        args = { space_name, {{ 'id' }} },
        op = 'insert_many',
        expect_error = true,
    },
    insert_object_many_error = {
        func = 'crud.insert_object_many',
        args = { space_name, {{ 'id' }} },
        op = 'insert_many',
        expect_error = true,
    },
    get_error = {
        func = 'crud.get',
        args = { space_name, { 'id' }, {mode = 'write'}, },
        op = 'get',
        expect_error = true,
    },
    select_error = {
        func = 'crud.select',
        args = { space_name, {{ '==', 'id_index', 'sdf' }}, {mode = 'write'}, },
        op = 'select',
        expect_error = true,
    },
    pairs_error = {
        eval = eval.pairs,
        args = { space_name, {{ '%=', 'id_index', 'sdf' }}, {batch_size = 1, mode = 'write'}, },
        op = 'select',
        expect_error = true,
        pcall = true,
    },
    replace_error = {
        func = 'crud.replace',
        args = { space_name, { 'id' } },
        op = 'replace',
        expect_error = true,
    },
    replace_object_error = {
        func = 'crud.replace_object',
        args = { space_name, { 'id' } },
        op = 'replace',
        expect_error = true,
    },
    replace_many_error = {
        func = 'crud.replace_many',
        args = { space_name, {{ 'id' }} },
        op = 'replace_many',
        expect_error = true,
    },
    replace_object_many_error = {
        func = 'crud.replace_object_many',
        args = { space_name, {{ 'id' }} },
        op = 'replace_many',
        expect_error = true,
    },
    update_error = {
        func = 'crud.update',
        args = { space_name, { 'id' }, {{'+', 'age', 1}} },
        op = 'update',
        expect_error = true,
    },
    upsert_error = {
        func = 'crud.upsert',
        args = { space_name, { 'id' }, {{'+', 'age', 1}} },
        op = 'upsert',
        expect_error = true,
    },
    upsert_object_error = {
        func = 'crud.upsert_object',
        args = { space_name, { 'id' }, {{'+', 'age', 1}} },
        op = 'upsert',
        expect_error = true,
    },
    upsert_many_error = {
        func = 'crud.upsert_many',
        args = { space_name, { {{ 'id' }, {{'+', 'age', 1}}} }, },
        op = 'upsert_many',
        expect_error = true,
    },
    upsert_object_many_error = {
        func = 'crud.upsert_object_many',
        args = { space_name, { {{ 'id' }, {{'+', 'age', 1}}} } },
        op = 'upsert_many',
        expect_error = true,
    },
    delete_error = {
        func = 'crud.delete',
        args = { space_name, { 'id' } },
        op = 'delete',
        expect_error = true,
    },
    count_error = {
        func = 'crud.count',
        args = { space_name, {{ '==', 'id_index', 'sdf' }}, {mode = 'write'}, },
        op = 'count',
        expect_error = true,
    },
    min_error = {
        func = 'crud.min',
        args = { space_name, 'badindex', {mode = 'write'}, },
        op = 'borders',
        expect_error = true,
    },
    max_error = {
        func = 'crud.max',
        args = { space_name, 'badindex', {mode = 'write'}, },
        op = 'borders',
        expect_error = true,
    },
}

local prepare_select_data = function(g)
    helpers.insert_objects(g, space_name, {
        -- Storage is s-2.
        {
            id = 1, name = "Elizabeth", last_name = "Jackson",
            age = 12, city = "New York",
        },
        -- Storage is s-2.
        {
            id = 2, name = "Mary", last_name = "Brown",
            age = 46, city = "Los Angeles",
        },
        -- Storage is s-1.
        {
            id = 3, name = "David", last_name = "Smith",
            age = 33, city = "Los Angeles",
        },
        -- Storage is s-2.
        {
            id = 4, name = "William", last_name = "White",
            age = 81, city = "Chicago",
        }
    })
end

local is_readview_supported = function()
    if (not helpers.tarantool_version_at_least(2, 11, 0))
    or (not require('luatest.tarantool').is_enterprise_package()) then
        t.skip('Readview is supported only for Tarantool Enterprise starting from v2.11.0')
    end
end

local select_cases = {
    select_by_primary_index = {
        func = 'crud.select',
        conditions = {{ '==', 'id_index', 3 }},
        opts = {mode = 'write'},
        map_reduces = 0,
        tuples_fetched = 1,
        tuples_lookup = 1,
    },
    select_by_secondary_index = {
        func = 'crud.select',
        conditions = {{ '==', 'age_index', 46 }},
        opts = {mode = 'write'},
        map_reduces = 1,
        tuples_fetched = 1,
        tuples_lookup = 1,
    },
    select_full_scan = {
        func = 'crud.select',
        conditions = {{ '>', 'id_index', 0 }, { '==', 'city', 'Kyoto' }},
        opts = {fullscan = true, mode = 'write'},
        map_reduces = 1,
        tuples_fetched = 0,
        tuples_lookup = 4,
    },
    pairs_by_primary_index = {
        eval = eval.pairs,
        conditions = {{ '==', 'id_index', 3 }},
        opts = {batch_size = 1, mode = 'write'},
        map_reduces = 0,
        tuples_fetched = 1,
        -- Since batch_size == 1, extra lookup is generated with
        -- after_tuple scroll for second batch.
        tuples_lookup = 2,
    },
    pairs_by_secondary_index = {
        eval = eval.pairs,
        conditions = {{ '==', 'age_index', 46 }},
        opts = {batch_size = 1, mode = 'write'},
        map_reduces = 1,
        tuples_fetched = 1,
        -- Since batch_size == 1, extra lookup is generated with
        -- after_tuple scroll for second batch.
        tuples_lookup = 2,
    },
    pairs_full_scan = {
        eval = eval.pairs,
        conditions = {{ '>', 'id_index', 0 }, { '==', 'city', 'Kyoto' }},
        opts = {batch_size = 1, mode = 'write'},
        map_reduces = 1,
        tuples_fetched = 0,
        tuples_lookup = 4,
    },
}

-- Generate non-null stats for all cases.
local function generate_stats(g)
    for _, case in pairs(simple_operation_cases) do
        if case.prepare ~= nil then
            case.prepare(g)
        end

        local _, err
        if case.eval ~= nil then
            if case.pcall then
                _, err = pcall(g.router.eval, g.router, case.eval, case.args)
            else
                _, err = g.router:eval(case.eval, case.args)
            end
        else
            _, err = g.router:call(case.func, case.args)
        end

        if case.expect_error ~= true then
            t.assert_equals(err, nil)
        else
            t.assert_not_equals(err, nil)
        end

        helpers.truncate_space_on_cluster(g.cluster, space_name)
    end

    -- Generate non-null select details.
    prepare_select_data(g)
    for _, case in pairs(select_cases) do
        local _, err
        if case.eval ~= nil then
            _, err = g.router:eval(case.eval, { space_name, case.conditions, case.opts })
        else
            _, err = g.router:call(case.func, { space_name, case.conditions, case.opts })
        end

        t.assert_equals(err, nil)
    end
end


-- Call some operations for existing
-- spaces and ensure statistics is updated.
for name, case in pairs(simple_operation_cases) do
    local test_name = ('test_%s'):format(name)

    if case.prepare ~= nil then
        pgroup.before_test(test_name, case.prepare)
    end

    pgroup[test_name] = function(g)
        -- Collect stats before call.
        local stats_before = get_stats(g, space_name)
        t.assert_type(stats_before, 'table')

        -- Call operation.
        local before_start = clock.monotonic()

        local _, err
        if case.eval ~= nil then
            if case.pcall then
                _, err = pcall(g.router.eval, g.router, case.eval, case.args)
            else
                _, err = g.router:eval(case.eval, case.args)
            end
        else
            _, err = g.router:call(case.func, case.args)
        end

        local after_finish = clock.monotonic()

        if case.expect_error ~= true then
            t.assert_equals(err, nil)
        else
            t.assert_not_equals(err, nil)
        end

        -- Collect stats after call.
        local stats_after = get_stats(g, space_name)
        t.assert_type(stats_after, 'table')
        t.assert_not_equals(stats_after[case.op], nil)

        -- Expecting 'ok' metrics to change on `expect_error == false`
        -- or 'error' to change otherwise.
        local changed, unchanged
        if case.expect_error == true then
            changed = 'error'
            unchanged = 'ok'
        else
            unchanged = 'error'
            changed = 'ok'
        end

        local op_before = set_defaults_if_empty(stats_before, case.op)
        local changed_before = op_before[changed]
        local op_after = set_defaults_if_empty(stats_after, case.op)
        local changed_after = op_after[changed]

        t.assert_equals(changed_after.count - changed_before.count, 1,
            'Expected count incremented')

        local ok_latency_max = math.max(changed_before.latency, after_finish - before_start)

        t.assert_gt(changed_after.latency, 0,
            'Changed latency has appropriate value')
        t.assert_le(changed_after.latency, ok_latency_max,
            'Changed latency has appropriate value')

        local ok_average_max = math.max(changed_before.latency_average, after_finish - before_start)

        t.assert_gt(changed_after.latency_average, 0,
            'Changed average has appropriate value')
        t.assert_le(changed_after.latency_average, ok_average_max,
            'Changed average has appropriate value')

        if g.params.args.quantiles == true then
            local ok_quantile_max = math.max(
                changed_before.latency_quantile_recent or 0,
                after_finish - before_start)

            t.assert_gt(changed_after.latency_quantile_recent, 0,
                'Changed quantile has appropriate value')
            t.assert_le(changed_after.latency_quantile_recent, ok_quantile_max,
                'Changed quantile has appropriate value')
        end

        local time_diff = changed_after.time - changed_before.time

        t.assert_gt(time_diff, 0, 'Total time increase has appropriate value')
        t.assert_le(time_diff, after_finish - before_start,
            'Total time increase has appropriate value')

        local unchanged_before = op_before[unchanged]
        local unchanged_after = stats_after[case.op][unchanged]

        t.assert_equals(unchanged_before, unchanged_after, 'Other stats remained the same')
    end

    pgroup.after_test(test_name, truncate_space_on_cluster)
end


-- Call some operation on non-existing
-- space and ensure statistics are updated.
pgroup.before_test('test_non_existing_space', function(g)
    t.assert_equals(
        helpers.is_space_exist(g.router, non_existing_space_name),
        false,
        ('Space %s does not exist'):format(non_existing_space_name)
    )
end)

pgroup.test_non_existing_space = function(g)
    local op = 'get'

    -- Collect stats before call.
    local stats_before = get_stats(g, non_existing_space_name)
    t.assert_type(stats_before, 'table')
    local op_before = set_defaults_if_empty(stats_before, op)

    -- Call operation.
    local _, err = g.router:call('crud.get', { non_existing_space_name, { 1 } })
    t.assert_not_equals(err, nil)

    -- Collect stats after call.
    local stats_after = get_stats(g, non_existing_space_name)
    t.assert_type(stats_after, 'table')
    local op_after = stats_after[op]
    t.assert_type(op_after, 'table', 'Section has been created if not existed')

    t.assert_equals(op_after.error.count - op_before.error.count, 1,
        'Error count for non-existing space incremented')
end


for name, case in pairs(select_cases) do
    local test_name = ('test_%s_details'):format(name)

    pgroup.before_test(test_name, prepare_select_data)

    pgroup[test_name] = function(g)
        local op = 'select'
        local space_name = space_name

        -- Collect stats before call.
        local stats_before = get_stats(g, space_name)
        t.assert_type(stats_before, 'table')

        -- Call operation.
        local _, err
        if case.eval ~= nil then
            _, err = g.router:eval(case.eval, { space_name, case.conditions, case.opts })
        else
            _, err = g.router:call(case.func, { space_name, case.conditions, case.opts })
        end

        t.assert_equals(err, nil)

        -- Collect stats after call.
        local stats_after = get_stats(g, space_name)
        t.assert_type(stats_after, 'table')

        local op_before = set_defaults_if_empty(stats_before, op)
        local details_before = op_before.details
        local op_after = set_defaults_if_empty(stats_after, op)
        local details_after = op_after.details

        local tuples_fetched_diff = details_after.tuples_fetched - details_before.tuples_fetched
        t.assert_equals(tuples_fetched_diff, case.tuples_fetched,
            'Expected count of tuples fetched')

        local tuples_lookup_diff = details_after.tuples_lookup - details_before.tuples_lookup
        t.assert_equals(tuples_lookup_diff, case.tuples_lookup,
            'Expected count of tuples looked up on storage')

        local map_reduces_diff = details_after.map_reduces - details_before.map_reduces
        t.assert_equals(map_reduces_diff, case.map_reduces,
            'Expected count of map reduces planned')
    end
end

for name, case in pairs(select_cases) do
    local test_name = ('test_%s_details_readview'):format(name)
    pgroup.before_test(test_name, is_readview_supported)
    pgroup.before_test(test_name, prepare_select_data)

    pgroup[test_name] = function(g)
        local op = 'select'
        local space_name = space_name

        -- Collect stats before call.
        local stats_before = get_stats(g, space_name)
        t.assert_type(stats_before, 'table')

        -- Call operation.
        local _, err
        if case.eval ~= nil then
            _, err = g.router:eval([[
                local crud = require('crud')
                local conditions, space_name = ...

                local foo, err = crud.readview({name = 'foo'})
                if err ~= nil then
                    return nil, err
                end

                local result = {}
                for _, v in foo:pairs(space_name, conditions, { batch_size = 1 }) do
                    table.insert(result, v)
                end
                foo:close()

                return result

            ]], {case.conditions, space_name})
        else
            _, err = g.router:eval([[
                local crud = require('crud')
                local conditions, space_name = ...

                local foo, err = crud.readview({name = 'foo'})
                if err ~= nil then
                    return nil, err
                end
                local result, err = foo:select(space_name, conditions)

                foo:close()

                return result, err
            ]], {case.conditions, space_name})
        end

        t.assert_equals(err, nil)

        -- Collect stats after call.
        local stats_after = get_stats(g, space_name)
        t.assert_type(stats_after, 'table')

        local op_before = set_defaults_if_empty(stats_before, op)
        local details_before = op_before.details
        local op_after = set_defaults_if_empty(stats_after, op)
        local details_after = op_after.details

        local tuples_fetched_diff = details_after.tuples_fetched - details_before.tuples_fetched
        t.assert_equals(tuples_fetched_diff, case.tuples_fetched,
            'Expected count of tuples fetched')

        local tuples_lookup_diff = details_after.tuples_lookup - details_before.tuples_lookup
        t.assert_equals(tuples_lookup_diff, case.tuples_lookup,
            'Expected count of tuples looked up on storage')

        local map_reduces_diff = details_after.map_reduces - details_before.map_reduces
        t.assert_equals(map_reduces_diff, case.map_reduces,
            'Expected count of map reduces planned')
    end
end


pgroup.before_test(
    'test_role_reload_do_not_reset_observations',
    generate_stats)

pgroup.test_role_reload_do_not_reset_observations = function(g)
    helpers.skip_not_cartridge_backend(g.params.backend)
    t.skip_if(not helpers.is_cartridge_hotreload_supported(),
        "Cartridge roles reload is not supported")
    t.skip_if((g.params.args.driver == 'metrics')
        and helpers.is_metrics_0_12_0_or_older(),
        "See https://github.com/tarantool/metrics/issues/334")

    helpers.skip_old_tarantool_cartridge_hotreload()

    local stats_before = get_stats(g)

    helpers.reload_roles(g.router)

    local stats_after = get_stats(g)
    t.assert_equals(stats_after, stats_before)
end


pgroup.before_test(
    'test_module_reload_do_not_reset_observations',
    generate_stats)

pgroup.test_module_reload_do_not_reset_observations = function(g)
    helpers.skip_not_cartridge_backend(g.params.backend)

    local stats_before = get_stats(g)

    helpers.reload_package(g.router)

    local stats_after = get_stats(g)
    t.assert_equals(stats_after, stats_before)
end


pgroup.test_spaces_created_in_runtime_supported_with_stats = function(g)
    local op = 'insert'
    local stats_before = get_stats(g, new_space_name)
    local op_before = set_defaults_if_empty(stats_before, op)

    create_new_space(g)

    local _, err = g.router:call('crud.insert', { new_space_name, { 1, box.NULL }})
    t.assert_equals(err, nil)

    local stats_after = get_stats(g, new_space_name)
    local op_after = stats_after[op]
    t.assert_type(op_after, 'table', "'insert' stats found for new space")
    t.assert_type(op_after.ok, 'table', "success 'insert' stats found for new space")
    t.assert_equals(op_after.ok.count - op_before.ok.count, 1,
        "Success requests count incremented for new space")
end


pgroup.before_test(
    'test_spaces_dropped_in_runtime_supported_with_stats',
    function(g)
        create_new_space(g)

        local _, err = g.router:call('crud.insert', { new_space_name, { 1, box.NULL }})
        t.assert_equals(err, nil)
    end)

pgroup.test_spaces_dropped_in_runtime_supported_with_stats = function(g)
    local op = 'insert'
    local stats_before = get_stats(g, new_space_name)
    local op_before = set_defaults_if_empty(stats_before, op)
    t.assert_type(op_before, 'table', "'insert' stats found for new space")

    helpers.drop_space_on_cluster(g.cluster, new_space_name)

    local _, err = g.router:call('crud.insert', { new_space_name, { 2, box.NULL }})
    t.assert_not_equals(err, nil, "Should trigger 'space not found' error")

    local stats_after = get_stats(g, new_space_name)
    local op_after = stats_after[op]
    t.assert_type(op_after, 'table', "'insert' stats found for dropped new space")
    t.assert_type(op_after.error, 'table', "error 'insert' stats found for dropped new space")
    t.assert_equals(op_after.error.count - op_before.error.count, 1,
        "Error requests count incremented since space was known to registry before drop")
end

-- https://github.com/tarantool/metrics/blob/fc5a67072340b12f983f09b7d383aca9e2f10cf1/test/utils.lua#L22-L31
local function find_obs(metric_name, label_pairs, observations)
    for _, obs in pairs(observations) do
        local same_label_pairs = pcall(t.assert_covers, obs.label_pairs, label_pairs)
        if obs.metric_name == metric_name and same_label_pairs then
            return obs
        end
    end

    return { value = 0 }
end

-- https://github.com/tarantool/metrics/blob/fc5a67072340b12f983f09b7d383aca9e2f10cf1/test/utils.lua#L55-L63
local function find_metric(metric_name, metrics_data)
    local m = {}
    for _, v in ipairs(metrics_data) do
        if v.metric_name == metric_name then
            table.insert(m, v)
        end
    end
    return #m > 0 and m or nil
end

local function get_unique_label_values(metrics_data, label_key)
    local label_values_map = {}
    for _, v in ipairs(metrics_data) do
        local label_pairs = v.label_pairs or {}
        if label_pairs[label_key] ~= nil then
            label_values_map[label_pairs[label_key]] = true
        end
    end

    local label_values = {}
    for k, _ in pairs(label_values_map) do
        table.insert(label_values, k)
    end

    return label_values
end

local function validate_metrics(g, metrics)
    local quantile_stats
    if g.params.args.quantiles == true then
        quantile_stats = find_metric('tnt_crud_stats', metrics)
        t.assert_type(quantile_stats, 'table', '`tnt_crud_stats` summary metrics found')
    end

    local stats_count = find_metric('tnt_crud_stats_count', metrics)
    t.assert_type(stats_count, 'table', '`tnt_crud_stats` summary metrics found')

    local stats_sum = find_metric('tnt_crud_stats_sum', metrics)
    t.assert_type(stats_sum, 'table', '`tnt_crud_stats` summary metrics found')


    local expected_operations = { 'insert', 'insert_many', 'get', 'replace', 'replace_many', 'update',
        'upsert', 'upsert_many', 'delete', 'select', 'truncate', 'len', 'count', 'borders' }

    if g.params.args.quantiles == true then
        t.assert_items_equals(get_unique_label_values(quantile_stats, 'operation'), expected_operations,
            'Metrics are labelled with operation')
    end

    t.assert_items_equals(get_unique_label_values(stats_count, 'operation'), expected_operations,
        'Metrics are labelled with operation')

    t.assert_items_equals(get_unique_label_values(stats_sum, 'operation'), expected_operations,
        'Metrics are labelled with operation')


    local expected_statuses = { 'ok', 'error' }

    if g.params.args.quantiles == true then
        t.assert_items_equals(
            get_unique_label_values(quantile_stats, 'status'),
            expected_statuses,
            'Metrics are labelled with status')
    end

    t.assert_items_equals(get_unique_label_values(stats_count, 'status'), expected_statuses,
        'Metrics are labelled with status')

    t.assert_items_equals(get_unique_label_values(stats_sum, 'status'), expected_statuses,
        'Metrics are labelled with status')


    local expected_names = { space_name }

    if g.params.args.quantiles == true then
        t.assert_items_equals(
            get_unique_label_values(quantile_stats, 'name'),
            expected_names,
            'Metrics are labelled with space name')
    end

    t.assert_items_equals(get_unique_label_values(stats_count, 'name'),
        expected_names,
        'Metrics are labelled with space name')

    t.assert_items_equals(
        get_unique_label_values(stats_sum, 'name'),
        expected_names,
        'Metrics are labelled with space name')

    if g.params.args.quantiles == true then
        local expected_quantiles = { 0.99 }
        t.assert_items_equals(get_unique_label_values(quantile_stats, 'quantile'), expected_quantiles,
            'Quantile metrics presents')
    end


    local tuples_fetched = find_metric('tnt_crud_tuples_fetched', metrics)
    t.assert_type(tuples_fetched, 'table', '`tnt_crud_tuples_fetched` metrics found')

    t.assert_items_equals(get_unique_label_values(tuples_fetched, 'operation'), { 'select' },
        'Metrics are labelled with operation')

    t.assert_items_equals(get_unique_label_values(tuples_fetched, 'name'), expected_names,
        'Metrics are labelled with space name')


    local tuples_lookup = find_metric('tnt_crud_tuples_lookup', metrics)
    t.assert_type(tuples_lookup, 'table', '`tnt_crud_tuples_lookup` metrics found')

    t.assert_items_equals(get_unique_label_values(tuples_lookup, 'operation'), { 'select' },
        'Metrics are labelled with operation')

    t.assert_items_equals(get_unique_label_values(tuples_lookup, 'name'), expected_names,
        'Metrics are labelled with space name')


    local map_reduces = find_metric('tnt_crud_map_reduces', metrics)
    t.assert_type(map_reduces, 'table', '`tnt_crud_map_reduces` metrics found')

    t.assert_items_equals(get_unique_label_values(map_reduces, 'operation'), { 'select' },
        'Metrics are labelled with operation')

    t.assert_items_equals(get_unique_label_values(map_reduces, 'name'), expected_names,
        'Metrics are labelled with space name')
end

local function check_updated_per_call(g)
    local metrics_before = get_metrics(g)
    local stats_labels = { operation = 'select', status = 'ok', name = space_name }
    local details_labels = { operation = 'select', name = space_name }

    local count_before = find_obs('tnt_crud_stats_count', stats_labels, metrics_before)
    local time_before = find_obs('tnt_crud_stats_sum', stats_labels, metrics_before)
    local tuples_lookup_before = find_obs('tnt_crud_tuples_lookup', details_labels, metrics_before)
    local tuples_fetched_before = find_obs('tnt_crud_tuples_fetched', details_labels, metrics_before)
    local map_reduces_before = find_obs('tnt_crud_map_reduces', details_labels, metrics_before)

    local case = select_cases['select_by_secondary_index']
    local _, err = g.router:call(case.func, { space_name, case.conditions, case.opts })
    t.assert_equals(err, nil)

    local metrics_after = get_metrics(g)
    local count_after = find_obs('tnt_crud_stats_count', stats_labels, metrics_after)
    local time_after = find_obs('tnt_crud_stats_sum', stats_labels, metrics_after)
    local tuples_lookup_after = find_obs('tnt_crud_tuples_lookup', details_labels, metrics_after)
    local tuples_fetched_after = find_obs('tnt_crud_tuples_fetched', details_labels, metrics_after)
    local map_reduces_after = find_obs('tnt_crud_map_reduces', details_labels, metrics_after)

    t.assert_equals(count_after.value - count_before.value, 1,
        '`select` metrics count increased')
    t.assert_ge(time_after.value - time_before.value, 0,
        '`select` total time increased')
    t.assert_ge(tuples_lookup_after.value - tuples_lookup_before.value, case.tuples_lookup,
        '`select` tuples lookup expected change')
    t.assert_ge(tuples_fetched_after.value - tuples_fetched_before.value, case.tuples_fetched,
        '`select` tuples feched expected change')
    t.assert_ge(map_reduces_after.value - map_reduces_before.value, case.tuples_lookup,
        '`select` map reduces expected change')
end


group_metrics.before_test(
    'test_stats_stored_in_global_metrics_registry',
    generate_stats)

group_metrics.test_stats_stored_in_global_metrics_registry = function(g)
    local metrics = get_metrics(g)
    validate_metrics(g, metrics)
end


group_metrics.before_test('test_metrics_updated_per_call', generate_stats)

group_metrics.test_metrics_updated_per_call = check_updated_per_call



group_metrics.before_test(
    'test_metrics_collectors_destroyed_if_stats_disabled',
    generate_stats)

group_metrics.test_metrics_collectors_destroyed_if_stats_disabled = function(g)
    disable_stats(g)

    local metrics = get_metrics(g)

    local stats = find_metric('tnt_crud_stats', metrics)
    t.assert_equals(stats, nil, '`tnt_crud_stats` summary metrics not found')

    local stats_count = find_metric('tnt_crud_stats_count', metrics)
    t.assert_equals(stats_count, nil, '`tnt_crud_stats` summary metrics not found')

    local stats_sum = find_metric('tnt_crud_stats_sum', metrics)
    t.assert_equals(stats_sum, nil, '`tnt_crud_stats` summary metrics not found')

    local tuples_fetched = find_metric('tnt_crud_tuples_fetched', metrics)
    t.assert_equals(tuples_fetched, nil, '`tnt_crud_tuples_fetched` metrics not found')

    local tuples_lookup = find_metric('tnt_crud_tuples_lookup', metrics)
    t.assert_equals(tuples_lookup, nil, '`tnt_crud_tuples_lookup` metrics not found')

    local map_reduces = find_metric('tnt_crud_map_reduces', metrics)
    t.assert_equals(map_reduces, nil, '`tnt_crud_map_reduces` metrics not found')
end


group_metrics.before_test(
    'test_stats_stored_in_metrics_registry_after_switch_to_metrics_driver',
    disable_stats)

group_metrics.test_stats_stored_in_metrics_registry_after_switch_to_metrics_driver = function(g)
    enable_stats(g, { driver = 'local', quantiles = false })
    -- Switch to metrics driver.
    enable_stats(g)

    generate_stats(g)
    local metrics = get_metrics(g)
    validate_metrics(g, metrics)
end

group_metrics.before_test(
    'test_role_reload_do_not_reset_metrics_observations',
    generate_stats)

group_metrics.test_role_reload_do_not_reset_metrics_observations = function(g)
    helpers.skip_not_cartridge_backend(g.params.backend)
    t.skip_if(not helpers.is_cartridge_hotreload_supported(),
        "Cartridge roles reload is not supported")
    t.skip_if(helpers.is_metrics_0_12_0_or_older(),
        "See https://github.com/tarantool/metrics/issues/334")
    helpers.skip_old_tarantool_cartridge_hotreload()

    helpers.reload_roles(g.router)
    g.router:eval("crud = require('crud')")
    local metrics = get_metrics(g)
    validate_metrics(g, metrics)
end


group_metrics.before_test(
    'test_module_reload_do_not_reset_metrics_observations',
    generate_stats)

group_metrics.test_module_reload_do_not_reset_metrics_observations = function(g)
    g.router:eval([[
        local function startswith(text, prefix)
            return text:find(prefix, 1, true) == 1
        end

        for k, _ in pairs(package.loaded) do
            if startswith(k, 'crud') then
                package.loaded[k] = nil
            end
        end

        crud = require('crud')
    ]])

    local metrics = get_metrics(g)
    validate_metrics(g, metrics)
end


group_metrics.before_test(
    'test_stats_changed_in_metrics_registry_after_role_reload',
    prepare_select_data)

group_metrics.test_stats_changed_in_metrics_registry_after_role_reload = function(g)
    helpers.skip_not_cartridge_backend(g.params.backend)
    t.skip_if(not helpers.is_cartridge_hotreload_supported(),
        "Cartridge roles reload is not supported")
    helpers.skip_old_tarantool_cartridge_hotreload()

    helpers.reload_roles(g.router)
    g.router:eval("crud = require('crud')")
    check_updated_per_call(g)
end


group_metrics.before_test(
    'test_stats_changed_in_metrics_registry_after_module_reload',
    prepare_select_data)

group_metrics.test_stats_changed_in_metrics_registry_after_module_reload = function(g)
    g.router:eval([[
        local function startswith(text, prefix)
            return text:find(prefix, 1, true) == 1
        end

        for k, _ in pairs(package.loaded) do
            if startswith(k, 'crud') then
                package.loaded[k] = nil
            end
        end

        crud = require('crud')
    ]])

    check_updated_per_call(g)
end
