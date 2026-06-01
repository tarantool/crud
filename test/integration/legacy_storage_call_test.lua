local t = require('luatest')
local crud = require('crud')

local helpers = require('test.helper')

local pgroup = t.group('legacy_storage_call', helpers.backend_matrix({
    {engine = 'memtx'},
}, {skip_safe_mode = true}))

pgroup.before_all(function(g)
    helpers.start_default_cluster(g, 'srv_simple_operations')
end)

pgroup.after_all(function(g)
    helpers.stop_cluster(g.cluster, g.params.backend)
end)

pgroup.before_each(function(g)
    helpers.truncate_space_on_cluster(g.cluster, 'customers')
end)

local function disable_storage_yield_checks(cluster)
    helpers.call_on_storages(cluster, function(server)
        server.net_box:eval([[
            local yield_checks = require('crud.common.yield_checks')
            rawset(_G, '__legacy_storage_call_yield_checks_backup', {
                check_no_yields = yield_checks.check_no_yields,
                guard = yield_checks.guard,
            })

            yield_checks.check_no_yields = function() end
            yield_checks.guard = function(f, ...)
                return f(...)
            end
        ]])
    end)
end

local function restore_storage_yield_checks(cluster)
    helpers.call_on_storages(cluster, function(server)
        server.net_box:eval([[
            local backup = rawget(_G, '__legacy_storage_call_yield_checks_backup')
            if backup == nil then
                return
            end

            local yield_checks = require('crud.common.yield_checks')
            yield_checks.check_no_yields = backup.check_no_yields
            yield_checks.guard = backup.guard
            rawset(_G, '__legacy_storage_call_yield_checks_backup', nil)
        ]])
    end)
end

-- Simulate a pre-_crud.call_on_storage storage. Such storages accepted direct
-- calls to storage-side CRUD functions and did not run the newer yield-check
-- guard that is normally installed by _crud.call_on_storage.
local function without_legacy_storage_api(g, func)
    helpers.disable_call_on_storage(g.cluster, g.router)
    disable_storage_yield_checks(g.cluster)

    local ok, err = pcall(func)

    restore_storage_yield_checks(g.cluster)
    helpers.restore_call_on_storage(g.cluster, g.router)

    if not ok then
        error(err, 0)
    end
end

local function call_legacy(g, func_name, args)
    helpers.reset_storage_call_compat_cache(g.router)

    local result, err = g.router:call(func_name, args)
    t.assert_equals(err, nil)

    return result
end

local function assert_customers(result, expected)
    local actual = {}
    for _, customer in ipairs(crud.unflatten_rows(result.rows, result.metadata)) do
        customer.bucket_id = nil
        actual[customer.id] = customer
    end

    local expected_by_id = {}
    for _, customer in ipairs(expected) do
        expected_by_id[customer.id] = customer
    end

    t.assert_equals(actual, expected_by_id)
end

local function assert_customer(g, id, expected)
    local result = call_legacy(g, 'crud.get', {
        'customers', id, {mode = 'write'},
    })

    if expected == nil then
        t.assert_equals(result.rows, {})
    else
        assert_customers(result, {expected})
    end
end

pgroup.test_single_tuple_operations_work_without_call_on_storage = function(g)
    without_legacy_storage_api(g, function()
        local result = call_legacy(g, 'crud.insert', {
            'customers', {101, box.NULL, 'Legacy Insert', 20},
        })
        assert_customers(result, {
            {id = 101, name = 'Legacy Insert', age = 20},
        })

        assert_customer(g, 101, {
            id = 101, name = 'Legacy Insert', age = 20,
        })

        result = call_legacy(g, 'crud.update', {
            'customers', 101, {
                {'=', 'name', 'Legacy Update'},
                {'+', 'age', 1},
            },
        })
        assert_customers(result, {
            {id = 101, name = 'Legacy Update', age = 21},
        })

        result = call_legacy(g, 'crud.replace', {
            'customers', {101, box.NULL, 'Legacy Replace', 30},
        })
        assert_customers(result, {
            {id = 101, name = 'Legacy Replace', age = 30},
        })

        result = call_legacy(g, 'crud.upsert', {
            'customers', {101, box.NULL, 'Legacy Upsert Ignored', 1}, {
                {'=', 'name', 'Legacy Upsert'},
                {'=', 'age', 31},
            },
        })
        t.assert_equals(result.rows, {})
        assert_customer(g, 101, {
            id = 101, name = 'Legacy Upsert', age = 31,
        })

        result = call_legacy(g, 'crud.delete', {'customers', 101})
        assert_customers(result, {
            {id = 101, name = 'Legacy Upsert', age = 31},
        })

        assert_customer(g, 101, nil)
    end)
end

pgroup.test_single_object_operations_work_without_call_on_storage = function(g)
    without_legacy_storage_api(g, function()
        local result = call_legacy(g, 'crud.insert_object', {
            'customers', {id = 201, name = 'Legacy Object Insert', age = 20},
        })
        assert_customers(result, {
            {id = 201, name = 'Legacy Object Insert', age = 20},
        })

        result = call_legacy(g, 'crud.replace_object', {
            'customers', {id = 201, name = 'Legacy Object Replace', age = 30},
        })
        assert_customers(result, {
            {id = 201, name = 'Legacy Object Replace', age = 30},
        })

        result = call_legacy(g, 'crud.upsert_object', {
            'customers', {id = 201, name = 'Legacy Object Ignored', age = 1}, {
                {'=', 'name', 'Legacy Object Upsert'},
                {'=', 'age', 31},
            },
        })
        t.assert_equals(result.rows, {})
        assert_customer(g, 201, {
            id = 201, name = 'Legacy Object Upsert', age = 31,
        })
    end)
end

pgroup.test_batch_tuple_operations_work_without_call_on_storage = function(g)
    without_legacy_storage_api(g, function()
        local result = call_legacy(g, 'crud.insert_many', {
            'customers', {
                {301, box.NULL, 'Legacy Insert Many 1', 20},
                {302, box.NULL, 'Legacy Insert Many 2', 21},
            },
        })
        assert_customers(result, {
            {id = 301, name = 'Legacy Insert Many 1', age = 20},
            {id = 302, name = 'Legacy Insert Many 2', age = 21},
        })

        result = call_legacy(g, 'crud.replace_many', {
            'customers', {
                {301, box.NULL, 'Legacy Replace Many 1', 30},
                {302, box.NULL, 'Legacy Replace Many 2', 31},
            },
        })
        assert_customers(result, {
            {id = 301, name = 'Legacy Replace Many 1', age = 30},
            {id = 302, name = 'Legacy Replace Many 2', age = 31},
        })

        result = call_legacy(g, 'crud.upsert_many', {
            'customers', {
                {
                    {301, box.NULL, 'Legacy Upsert Many Ignored', 1},
                    {{'=', 'name', 'Legacy Upsert Many 1'}, {'=', 'age', 32}},
                },
                {
                    {303, box.NULL, 'Legacy Upsert Many 3', 33},
                    {{'+', 'age', 1}},
                },
            },
        })
        t.assert_equals(result.rows, nil)
        assert_customer(g, 301, {
            id = 301, name = 'Legacy Upsert Many 1', age = 32,
        })
        assert_customer(g, 303, {
            id = 303, name = 'Legacy Upsert Many 3', age = 33,
        })
    end)
end

pgroup.test_batch_object_operations_work_without_call_on_storage = function(g)
    without_legacy_storage_api(g, function()
        local result = call_legacy(g, 'crud.insert_object_many', {
            'customers', {
                {id = 401, name = 'Legacy Object Insert Many 1', age = 20},
                {id = 402, name = 'Legacy Object Insert Many 2', age = 21},
            },
        })
        assert_customers(result, {
            {id = 401, name = 'Legacy Object Insert Many 1', age = 20},
            {id = 402, name = 'Legacy Object Insert Many 2', age = 21},
        })

        result = call_legacy(g, 'crud.replace_object_many', {
            'customers', {
                {id = 401, name = 'Legacy Object Replace Many 1', age = 30},
                {id = 402, name = 'Legacy Object Replace Many 2', age = 31},
            },
        })
        assert_customers(result, {
            {id = 401, name = 'Legacy Object Replace Many 1', age = 30},
            {id = 402, name = 'Legacy Object Replace Many 2', age = 31},
        })

        result = call_legacy(g, 'crud.upsert_object_many', {
            'customers', {
                {
                    {id = 401, name = 'Legacy Object Upsert Many Ignored', age = 1},
                    {{'=', 'name', 'Legacy Object Upsert Many 1'}, {'=', 'age', 32}},
                },
                {
                    {id = 403, name = 'Legacy Object Upsert Many 3', age = 33},
                    {{'+', 'age', 1}},
                },
            },
        })
        t.assert_equals(result.rows, nil)
        assert_customer(g, 401, {
            id = 401, name = 'Legacy Object Upsert Many 1', age = 32,
        })
        assert_customer(g, 403, {
            id = 403, name = 'Legacy Object Upsert Many 3', age = 33,
        })
    end)
end

pgroup.test_map_operations_work_without_call_on_storage = function(g)
    without_legacy_storage_api(g, function()
        call_legacy(g, 'crud.insert_many', {
            'customers', {
                {501, box.NULL, 'Legacy Map 1', 20},
                {502, box.NULL, 'Legacy Map 2', 21},
            },
        })

        local result = call_legacy(g, 'crud.len', {'customers'})
        t.assert_equals(result, 2)

        result = call_legacy(g, 'crud.count', {'customers'})
        t.assert_equals(result, 2)

        result = call_legacy(g, 'crud.truncate', {'customers'})
        t.assert_equals(result, true)

        result = call_legacy(g, 'crud.len', {'customers'})
        t.assert_equals(result, 0)
    end)
end
