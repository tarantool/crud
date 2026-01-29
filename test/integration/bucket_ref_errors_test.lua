local t = require('luatest')

local utils = require('crud.common.utils')

local helpers = require('test.helper')

local function wait_balance(g, buckets_s1, buckets_s2)
    t.helpers.retrying({timeout=30}, function()
        local s1 = g.cluster:server('s1-master').net_box:eval("return vshard.storage.rebalancer_request_state()")
        t.assert_not_equals(s1, nil)
        local s2 = g.cluster:server('s2-master').net_box:eval("return vshard.storage.rebalancer_request_state()")
        t.assert_not_equals(s2, nil)

        t.assert_equals(s1.bucket_active_count, buckets_s1)
        t.assert_equals(s2.bucket_active_count, buckets_s2)
    end)
end

local function reset_weights(g)
    if g.params.backend == "config" then
        local cfg = g.cluster:cfg()
        cfg.groups.storages.replicasets["s-1"].sharding = {
            weight = 1,
        }
        cfg.groups.storages.replicasets["s-2"].sharding = {
            weight = 0,
        }
        g.cluster:cfg(cfg)
        wait_balance(g, 3000, 0)
    end
end

local pgroup_many = t.group('many_operations_errors', helpers.backend_matrix({
    {engine = 'memtx', operation = 'insert_many'},
    {engine = 'memtx', operation = 'replace_many'},
    {engine = 'memtx', operation = 'upsert_many'},
}))

pgroup_many.before_all(function(g)
    helpers.start_default_cluster(g, 'srv_simple_operations')
end)

pgroup_many.after_all(function(g)
    helpers.stop_cluster(g.cluster, g.params.backend)
end)

pgroup_many.before_each(function(g)
    helpers.truncate_space_on_cluster(g.cluster, 'customers')
end)

pgroup_many.after_each(function(g)
    reset_weights(g)
end)

local function test_tuples(start_id, number)
    local res = {}
    for i = start_id, start_id+number-1 do
        table.insert(res, {
            i,
            box.NULL,
            ('Customer %d'):format(i),
            math.random(18, 65)
        })
    end
    return res
end

local function test_ops(start_id, number)
    local res = {}
    for i = start_id, start_id+number-1 do
        table.insert(res, {
            {
                i,
                box.NULL,
                ('Customer %d'):format(i),
                math.random(18, 65)
            },
            {
                { '+', 'age', 1 },
            },
        })
    end
    return res
end

local duplicate_operations = {
    insert_many = function(g, start_id, number)
        return g.router:call('crud.insert_many', {'customers', test_tuples(start_id, number)})
    end,
    replace_many = function(g, start_id, number)
        return g.router:call('crud.replace_many', {'customers', test_tuples(start_id, number)})
    end,
    upsert_many = function(g, start_id, number)
        return g.router:call('crud.upsert_many', {'customers', test_ops(start_id, number)})
    end
}

pgroup_many.test_errors = function(g)
    t.skip_if(
        not (
            utils.tarantool_version_at_least(3, 1)
            and (g.params.backend == "config")
        ),
        'test implemented only for 3.1 and greater'
    )

    local cfg = g.cluster:cfg()
    cfg.groups.storages.replicasets["s-1"].sharding = {
        weight = 0,
    }
    cfg.groups.storages.replicasets["s-2"].sharding = {
        weight = 1,
    }
    g.cluster:cfg(cfg)

    local _, errs
    local start_id = 1
    t.helpers.retrying({timeout=30}, function()
        _, errs = duplicate_operations[g.params.operation](g, start_id, 100)
        if g.params.operation == 'insert_many' then
            start_id = start_id + 100
        end
        t.assert_not_equals(errs, nil)
    end)

    t.assert_type(errs, 'table')
    for _, err in ipairs(errs) do
        t.assert_type(err, 'table')
        t.assert_equals(err.class_name, 'CallError')
        t.assert_str_contains(err.err, 'Function returned an error: failed bucket_ref')
    end
end
