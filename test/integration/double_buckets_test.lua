local t = require('luatest')
local json = require('json')
local fiber = require('fiber')

local utils = require('crud.common.utils')

local helpers = require('test.helper')

local function wait_balance(g)
    t.helpers.retrying({timeout=30}, function()
        local buckets_count_s1 = g.cluster:server('s1-master').net_box:eval("return box.space._bucket:len()")
        local buckets_count_s2 = g.cluster:server('s2-master').net_box:eval("return box.space._bucket:len()")
        t.assert_equals(buckets_count_s1, 1500)
        t.assert_equals(buckets_count_s2, 1500)
    end)
end

local function balance_cluster(g)
    if g.params.backend == "config" then
        local cfg = g.cluster:cfg()
        cfg.groups.storages.replicasets["s-1"].sharding = {
            weight = 1,
        }
        cfg.groups.storages.replicasets["s-2"].sharding = {
            weight = 1,
        }
        g.cluster:cfg(cfg)
        wait_balance(g)
    end
end

local pgroup_duplicates = t.group('double_buckets_duplicates', helpers.backend_matrix({
    {engine = 'memtx', operation = 'replace'},
    {engine = 'memtx', operation = 'insert'},
    {engine = 'memtx', operation = 'upsert'},
    {engine = 'memtx', operation = 'insert_many'},
    {engine = 'memtx', operation = 'replace_many'},
    {engine = 'memtx', operation = 'upsert_many'},
}))

pgroup_duplicates.before_all(function(g)
    helpers.start_default_cluster(g, 'srv_simple_operations')
end)

pgroup_duplicates.after_all(function(g)
    helpers.stop_cluster(g.cluster, g.params.backend)
end)

pgroup_duplicates.before_each(function(g)
    helpers.truncate_space_on_cluster(g.cluster, 'customers')
end)

pgroup_duplicates.after_each(function(g)
    balance_cluster(g)
end)

--- Rebalance stalls if we move all buckets at once; use a small subset.
local test_tuples = {
    {22, box.NULL, 'Alex', 34},
    {92, box.NULL, 'Artur', 29},
    {3, box.NULL, 'Anastasia', 22},
    {5, box.NULL, 'Sergey', 25},
    {9, box.NULL, 'Anna', 30},
    {71, box.NULL, 'Oksana', 29},
}

local last_call = fiber.time()
local duplicate_operations = {
    insert = function(g)
        return g.router:call('crud.insert', {'customers', {45, box.NULL, 'John Fedor', 42}})
    end,
    replace = function(g)
        return g.router:call('crud.replace', {'customers', {45, box.NULL, 'John Fedor', 42}})
    end,
    upsert = function (g)
        return g.router:call('crud.upsert', {'customers', {45, box.NULL, 'John Fedor', 42}, {{'+', 'age', 1}}})
    end,
    insert_many = function(g)
        if fiber.time() - last_call < 1 then
            return
        end
        last_call = fiber.time()
        return g.router:call('crud.insert_many', {'customers', test_tuples})
    end,
    replace_many = function(g)
        if fiber.time() - last_call < 1 then
            return
        end
        last_call = fiber.time()
        return g.router:call('crud.replace_many', {'customers', test_tuples})
    end,
    upsert_many = function(g)
        if fiber.time() - last_call < 1 then
            return
        end
        last_call = fiber.time()
        local tuples = {}
        for i = 1, 2 do
            tuples[i] = {{i, box.NULL, 'John Fedor', 42}, {{'+', 'age', 1}}}
        end
        return g.router:call('crud.upsert_many', {'customers', tuples})
    end
}

local function check_duplicates(tuples)
    local ids = {}
    for _, tuple in pairs(tuples) do
        t.assert_equals(ids[tuple[1]], nil, ('duplicate to tuple: %s'):format(json.encode(tuple)))
        ids[tuple[1]] = true
    end
end


--- write requests cause duplicates by primary key in cluster
pgroup_duplicates.test_duplicates = function(g)
    t.skip_if(
        not (
            utils.tarantool_version_at_least(3, 1) and (g.params.backend == "config")
        ),
        'test implemented only for 3.1 and greater'
    )
    if g.params.backend == "config" then
        duplicate_operations[g.params.operation](g)

        local cfg = g.cluster:cfg()
        cfg.groups.storages.replicasets["s-1"].sharding = {
            weight = 0,
        }
        g.cluster:cfg(cfg)
        t.helpers.retrying({timeout=30}, function()
            local buckets_count = g.cluster:server('s1-master').net_box:eval("return box.space._bucket:len()")
            duplicate_operations[g.params.operation](g)
            t.assert_equals(buckets_count, 0)
        end)

        cfg.groups.storages.replicasets["s-2"].sharding = {
            weight = 0,
        }
        cfg.groups.storages.replicasets["s-1"].sharding = {
            weight = 1,
        }
        g.cluster:cfg(cfg)
        t.helpers.retrying({timeout=30}, function()
            local buckets_count = g.cluster:server('s2-master').net_box:eval("return box.space._bucket:len()")
            duplicate_operations[g.params.operation](g)
            t.assert_equals(buckets_count, 0)
        end)

        local res = g.router:call('crud.select', {'customers'})
        check_duplicates(res.rows)
    end
end

local pgroup_not_applied =  t.group('double_buckets_not_applied', helpers.backend_matrix({
    {engine = 'memtx', operation = 'delete'},
    {engine = 'memtx', operation = 'update'},
    {engine = 'memtx', operation = 'get'},
}))

pgroup_not_applied.before_all(function(g)
    helpers.start_default_cluster(g, 'srv_simple_operations')
end)

pgroup_not_applied.after_all(function(g)
    helpers.stop_cluster(g.cluster, g.params.backend)
end)

pgroup_not_applied.before_each(function(g)
    helpers.truncate_space_on_cluster(g.cluster, 'customers')
end)

pgroup_not_applied.after_each(function(g)
    balance_cluster(g)
end)

local not_applied_operations = {
    delete = {
        call = function(g, key)
            last_call = fiber.time()
            return g.router:call('crud.delete', { 'customers', {key} })
        end,
        check_applied = function(rows, applied_ids)
            for _, tuple in pairs(rows) do
                t.assert_equals(
                    applied_ids[tuple[1]],
                    nil,
                    ('tuples %s was marked as deleted, but exists'):format(json.encode(tuple))
                )
            end
        end,
        check_not_applied = function(not_applied_ids)
            t.assert_equals(
                next(not_applied_ids),
                nil,
                'tuples were inserted, but crud.delete returned 0 rows, as if there were no such tuples'
            )
        end
    },
    update = {
        call = function(g, key)
            return g.router:call('crud.update', { 'customers', key, {{'=', 'name', 'applied'}} })
        end,
        check_applied = function(rows, applied_ids)
            for _, tuple in pairs(rows) do
                if applied_ids[tuple[1]] then
                    t.assert_equals(
                        tuple[3],
                        'applied',
                        ('tuples %s was marked as updated, but was not updated'):format(json.encode(tuple))
                    )
                end
            end
        end,
        check_not_applied = function(not_applied_ids)
            t.assert_equals(
                next(not_applied_ids),
                nil,
                'tuples were created, but crud.update returned 0 rows, as if there were no such tuples'
            )
        end
    },
    get = {
        call = function (g, key)
            return g.router:call('crud.get', { 'customers', key, {mode = 'write'} })
        end,
        check_applied = function() end,
        check_not_applied = function(not_applied_ids)
            t.assert_equals(
                next(not_applied_ids),
                nil,
                'tuples were created, but crud.get returned 0 rows, as if there were no such tuples'
            )
        end
    }
}

--- Some requests do not create duplicates but return 0 rows as if there is no tuple
--- with this key. The tuple can still exist in cluster but be unavailable during
--- rebalance. CRUD should return an error in this case, not 0 rows as if there were
--- no tuples.
pgroup_not_applied.test_not_applied = function(g)
    t.skip_if(
        not (
            utils.tarantool_version_at_least(3, 1) and (g.params.backend == "config")
        ),
        'test implemented only for 3.1 and greater'
    )

    if g.params.backend == "config" then
        local tuples, tuples_count = {}, 1000
        for i = 1, tuples_count do
            tuples[i] = {i, box.NULL, 'John Fedor', 42}
        end

        local _, err = g.router:call('crud.replace_many', {'customers', tuples})
        t.assert_equals(err, nil)
        local cfg = g.cluster:cfg()
        cfg.groups.storages.replicasets["s-1"].sharding = {
            weight = 0,
        }
        g.cluster:cfg(cfg)
        local tuple_id = 1
        local not_applied_ids = {}
        local applied_ids = {}
        t.helpers.retrying({timeout=30}, function()
            if tuple_id > tuples_count then
                return
            end

            local buckets_count = g.cluster:server('s1-master').net_box:eval("return box.space._bucket:len()")
            local res, err = not_applied_operations[g.params.operation].call(g, tuple_id)
            if err == nil then
                if #res.rows == 0 then
                    not_applied_ids[tuple_id] = true
                else
                    applied_ids[tuple_id] = true
                end
                tuple_id = tuple_id + 1
            end

            t.assert_equals(buckets_count, 0)
        end)

        cfg.groups.storages.replicasets["s-2"].sharding = {
            weight = 0,
        }
        cfg.groups.storages.replicasets["s-1"].sharding = {
            weight = 1,
        }
        g.cluster:cfg(cfg)
        t.helpers.retrying({timeout=30}, function()
            if tuple_id > tuples_count then
                return
            end

            local buckets_count = g.cluster:server('s2-master').net_box:eval("return box.space._bucket:len()")
            local res, err = not_applied_operations[g.params.operation].call(g, tuple_id)

            if err == nil then
                if #res.rows == 0 then
                    not_applied_ids[tuple_id] = true
                else
                    applied_ids[tuple_id] = true
                end
                tuple_id = tuple_id + 1
            end

            t.assert_equals(buckets_count, 0)
        end)

        local res = g.router:call('crud.select', {'customers'})
        not_applied_operations[g.params.operation].check_applied(res.rows, applied_ids)
        not_applied_operations[g.params.operation].check_not_applied(not_applied_ids)
    end
end
