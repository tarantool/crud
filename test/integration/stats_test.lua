local fio = require('fio')
local clock = require('clock')
local t = require('luatest')

local stats_registry_utils = require('crud.stats.registry_utils')

local g = t.group('stats_integration')
local helpers = require('test.helper')

local space_name = 'customers'
local non_existing_space_name = 'non_existing_space'
local new_space_name = 'newspace'

g.before_all(function(g)
    g.cluster = helpers.Cluster:new({
        datadir = fio.tempdir(),
        server_command = helpers.entrypoint('srv_stats'),
        use_vshard = true,
        replicasets = helpers.get_test_replicasets(),
    })
    g.cluster:start()
    g.router = g.cluster:server('router').net_box

    helpers.prepare_simple_functions(g.router)
    g.router:eval("require('crud').cfg{ stats = true }")
end)

g.after_all(function(g)
    helpers.stop_cluster(g.cluster)
end)

g.before_each(function(g)
    g.router:eval("crud = require('crud')")
    helpers.truncate_space_on_cluster(g.cluster, space_name)
    helpers.drop_space_on_cluster(g.cluster, new_space_name)
end)

function g:get_stats(space_name)
    return self.router:eval("return require('crud').stats(...)", { space_name })
end


local function create_new_space(g)
    helpers.call_on_storages(g.cluster, function(server)
        server.net_box:eval([[
            local space_name = ...
            if not box.cfg.read_only then
                local sp = box.schema.space.create(space_name, { format = {
                    {name = 'id', type = 'unsigned'},
                    {name = 'bucket_id', type = 'unsigned'},
                }})

                sp:create_index('pk', {
                    parts = { {field = 'id'} },
                })

                sp:create_index('bucket_id', {
                    parts = { {field = 'bucket_id'} },
                    unique = false,
                })
            end
        ]], { new_space_name })
    end)
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

        local result = {}
        for _, v in crud.pairs(space_name, conditions, { batch_size = 1 }) do
            table.insert(result, v)
        end

        return result
    ]],

    pairs_pcall = [[
        local space_name = select(1, ...)
        local conditions = select(2, ...)

        local _, err = pcall(crud.pairs, space_name, conditions, { batch_size = 1 })

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
    get = {
        func = 'crud.get',
        args = { space_name, { 12 } },
        op = 'get',
    },
    select = {
        func = 'crud.select',
        args = { space_name, {{ '==', 'id_index', 3 }} },
        op = 'select',
    },
    pairs = {
        eval = eval.pairs,
        args = { space_name, {{ '==', 'id_index', 3 }} },
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
        args = { space_name, {{ '==', 'id_index', 3 }} },
        op = 'count',
    },
    min = {
        func = 'crud.min',
        args = { space_name },
        op = 'borders',
    },
    max = {
        func = 'crud.max',
        args = { space_name },
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
    get_error = {
        func = 'crud.get',
        args = { space_name, { 'id' } },
        op = 'get',
        expect_error = true,
    },
    select_error = {
        func = 'crud.select',
        args = { space_name, {{ '==', 'id_index', 'sdf' }} },
        op = 'select',
        expect_error = true,
    },
    pairs_error = {
        eval = eval.pairs,
        args = { space_name, {{ '%=', 'id_index', 'sdf' }} },
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
    delete_error = {
        func = 'crud.delete',
        args = { space_name, { 'id' } },
        op = 'delete',
        expect_error = true,
    },
    count_error = {
        func = 'crud.count',
        args = { space_name, {{ '==', 'id_index', 'sdf' }} },
        op = 'count',
        expect_error = true,
    },
    min_error = {
        func = 'crud.min',
        args = { space_name, 'badindex' },
        op = 'borders',
        expect_error = true,
    },
    max_error = {
        func = 'crud.max',
        args = { space_name, 'badindex' },
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

local select_cases = {
    select_by_primary_index = {
        func = 'crud.select',
        conditions = {{ '==', 'id_index', 3 }},
        map_reduces = 0,
        tuples_fetched = 1,
        tuples_lookup = 1,
    },
    select_by_secondary_index = {
        func = 'crud.select',
        conditions = {{ '==', 'age_index', 46 }},
        map_reduces = 1,
        tuples_fetched = 1,
        tuples_lookup = 1,
    },
    select_full_scan = {
        func = 'crud.select',
        conditions = {{ '>', 'id_index', 0 }, { '==', 'city', 'Kyoto' }},
        map_reduces = 1,
        tuples_fetched = 0,
        tuples_lookup = 4,
    },
    pairs_by_primary_index = {
        eval = eval.pairs,
        conditions = {{ '==', 'id_index', 3 }},
        map_reduces = 0,
        tuples_fetched = 1,
        -- Since batch_size == 1, extra lookup is generated with
        -- after_tuple scroll for second batch.
        tuples_lookup = 2,
    },
    pairs_by_secondary_index = {
        eval = eval.pairs,
        conditions = {{ '==', 'age_index', 46 }},
        map_reduces = 1,
        tuples_fetched = 1,
        -- Since batch_size == 1, extra lookup is generated with
        -- after_tuple scroll for second batch.
        tuples_lookup = 2,
    },
    pairs_full_scan = {
        eval = eval.pairs,
        conditions = {{ '>', 'id_index', 0 }, { '==', 'city', 'Kyoto' }},
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
    end

    -- Generate non-null select details.
    prepare_select_data(g)
    for _, case in pairs(select_cases) do
        local _, err
        if case.eval ~= nil then
            _, err = g.router:eval(case.eval, { space_name, case.conditions })
        else
            _, err = g.router:call(case.func, { space_name, case.conditions })
        end

        t.assert_equals(err, nil)
    end
end


-- Call some operations for existing
-- spaces and ensure statistics is updated.
for name, case in pairs(simple_operation_cases) do
    local test_name = ('test_%s'):format(name)

    if case.prepare ~= nil then
        g.before_test(test_name, case.prepare)
    end

    g[test_name] = function(g)
        -- Collect stats before call.
        local stats_before = g:get_stats(space_name)
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
        local stats_after = g:get_stats(space_name)
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

        local time_diff = changed_after.time - changed_before.time

        t.assert_gt(time_diff, 0, 'Total time increase has appropriate value')
        t.assert_le(time_diff, after_finish - before_start,
            'Total time increase has appropriate value')

        local unchanged_before = op_before[unchanged]
        local unchanged_after = stats_after[case.op][unchanged]

        t.assert_equals(unchanged_before, unchanged_after, 'Other stats remained the same')
    end
end


-- Call some operation on non-existing
-- space and ensure statistics are updated.
g.before_test('test_non_existing_space', function(g)
    t.assert_equals(
        helpers.is_space_exist(g.router, non_existing_space_name),
        false,
        ('Space %s does not exist'):format(non_existing_space_name)
    )
end)

g.test_non_existing_space = function(g)
    local op = 'get'

    -- Collect stats before call.
    local stats_before = g:get_stats(non_existing_space_name)
    t.assert_type(stats_before, 'table')
    local op_before = set_defaults_if_empty(stats_before, op)

    -- Call operation.
    local _, err = g.router:call('crud.get', { non_existing_space_name, { 1 } })
    t.assert_not_equals(err, nil)

    -- Collect stats after call.
    local stats_after = g:get_stats(non_existing_space_name)
    t.assert_type(stats_after, 'table')
    local op_after = stats_after[op]
    t.assert_type(op_after, 'table', 'Section has been created if not existed')

    t.assert_equals(op_after.error.count - op_before.error.count, 1,
        'Error count for non-existing space incremented')
end


for name, case in pairs(select_cases) do
    local test_name = ('test_%s_details'):format(name)

    g.before_test(test_name, prepare_select_data)

    g[test_name] = function(g)
        local op = 'select'
        local space_name = space_name

        -- Collect stats before call.
        local stats_before = g:get_stats(space_name)
        t.assert_type(stats_before, 'table')

        -- Call operation.
        local _, err
        if case.eval ~= nil then
            _, err = g.router:eval(case.eval, { space_name, case.conditions })
        else
            _, err = g.router:call(case.func, { space_name, case.conditions })
        end

        t.assert_equals(err, nil)

        -- Collect stats after call.
        local stats_after = g:get_stats(space_name)
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


g.before_test(
    'test_role_reload_do_not_reset_observations',
    generate_stats)

g.test_role_reload_do_not_reset_observations = function(g)
    local stats_before = g:get_stats()

    helpers.reload_roles(g.cluster:server('router'))

    local stats_after = g:get_stats()
    t.assert_equals(stats_after, stats_before)
end


g.before_test(
    'test_module_reload_do_not_reset_observations',
    generate_stats)

g.test_module_reload_do_not_reset_observations = function(g)
    local stats_before = g:get_stats()

    helpers.reload_package(g.cluster:server('router'))

    local stats_after = g:get_stats()
    t.assert_equals(stats_after, stats_before)
end


g.test_spaces_created_in_runtime_supported_with_stats = function(g)
    local op = 'insert'
    local stats_before = g:get_stats(new_space_name)
    local op_before = set_defaults_if_empty(stats_before, op)

    create_new_space(g)

    local _, err = g.router:call('crud.insert', { new_space_name, { 1, box.NULL }})
    t.assert_equals(err, nil)

    local stats_after = g:get_stats(new_space_name)
    local op_after = stats_after[op]
    t.assert_type(op_after, 'table', "'insert' stats found for new space")
    t.assert_type(op_after.ok, 'table', "success 'insert' stats found for new space")
    t.assert_equals(op_after.ok.count - op_before.ok.count, 1,
        "Success requests count incremented for new space")
end


g.before_test(
    'test_spaces_dropped_in_runtime_supported_with_stats',
    function(g)
        create_new_space(g)

        local _, err = g.router:call('crud.insert', { new_space_name, { 1, box.NULL }})
        t.assert_equals(err, nil)
    end)

g.test_spaces_dropped_in_runtime_supported_with_stats = function(g)
    local op = 'insert'
    local stats_before = g:get_stats(new_space_name)
    local op_before = set_defaults_if_empty(stats_before, op)
    t.assert_type(op_before, 'table', "'insert' stats found for new space")

    helpers.drop_space_on_cluster(g.cluster, new_space_name)

    local _, err = g.router:call('crud.insert', { new_space_name, { 2, box.NULL }})
    t.assert_not_equals(err, nil, "Should trigger 'space not found' error")

    local stats_after = g:get_stats(new_space_name)
    local op_after = stats_after[op]
    t.assert_type(op_after, 'table', "'insert' stats found for dropped new space")
    t.assert_type(op_after.error, 'table', "error 'insert' stats found for dropped new space")
    t.assert_equals(op_after.error.count - op_before.error.count, 1,
        "Error requests count incremented since space was known to registry before drop")
end
