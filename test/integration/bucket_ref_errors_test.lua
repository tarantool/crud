local t = require('luatest')

local utils = require('crud.common.utils')

local helpers = require('test.helper')

local function wait_balance(g, buckets_s1, buckets_s2)
    helpers.wait_active_bucket_count(g.cluster:server('s1-master'), buckets_s1)
    helpers.wait_active_bucket_count(g.cluster:server('s2-master'), buckets_s2)
end

local function reset_weights(g)
    if g.params.backend == "config" then
        local cfg = g.cluster:cfg()
        cfg.sharding.rebalancer_max_sending = 15
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

local function replicaset_id_of(server)
    return server:exec(function()
        local name = box.info.replicaset.name
        if name == box.NULL then
            name = box.info.replicaset.uuid
        end
        return name
    end)
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
    cfg.sharding.rebalancer_max_sending = 15
    cfg.groups.storages.replicasets["s-1"].sharding = {
        weight = 0,
    }
    cfg.groups.storages.replicasets["s-2"].sharding = {
        weight = 1,
    }
    g.cluster:cfg(cfg)

    local _, errs
    local start_id = 1
    t.helpers.retrying({timeout = tonumber(os.getenv('LUATEST_SERVER_WAIT_TIMEOUT')) or 60}, function()
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

pgroup_many.test_bucket_refrw_many_aggregates_bucket_ref_errs = function(g)
    t.skip_if(g.params.operation ~= 'insert_many', 'run only for insert_many')

    t.skip_if(
        not (utils.tarantool_version_at_least(3, 1) and g.params.backend == "config"),
        'test implemented only for config backend on 3.1+'
    )

    local source = g.cluster:server('s1-master')
    local destination = g.cluster:server('s2-master')
    local destination_id = replicaset_id_of(destination)
    local bucket_ref_unref = require('crud.common.sharding.bucket_ref_unref')

    local bucket_ids = source:exec(function()
        local active = require('vshard.consts').BUCKET.ACTIVE
        local idx = box.space._bucket.index.status
        local res = {}
        for _, b in idx:pairs({active}) do
            res[#res + 1] = b.id
            if #res == 2 then
                break
            end
        end
        assert(#res == 2)
        return res
    end)
    local b1, b2 = bucket_ids[1], bucket_ids[2]

    -- Move buckets to destination, cache becomes stale.
    source:exec(function(bid1, bid2, dst)
        local vshard = require('vshard')
        assert(vshard.storage.bucket_send(bid1, dst))
        assert(vshard.storage.bucket_send(bid2, dst))
    end, {b1, b2, destination_id})

    -- Wait until destination sees both as active.
    helpers.retrying({timeout = 10}, function()
        local st = destination:exec(function(bid1, bid2)
            local b1 = box.space._bucket:get{bid1}
            local b2 = box.space._bucket:get{bid2}
            return {b1 and b1.status or nil, b2 and b2.status or nil}
        end, {b1, b2})

        t.assert_equals(st[1], 'active')
        t.assert_equals(st[2], 'active')
    end)

    -- Call internal aggregation directly on source and check how many errors it collected.
    local res = source:exec(function(bid1, bid2)
        local bucket_ref_unref = require('crud.common.sharding.bucket_ref_unref')

        local ok, err = bucket_ref_unref._bucket_refrw_many({[bid1] = true, [bid2] = true})
        if ok then
            return {ok = true}
        end

        return {
            ok = false,
            class_name = err.class_name,
            n = #err.bucket_ref_errs,
        }
    end, {b1, b2})

    t.assert_equals(res.ok, false)
    t.assert_equals(res.class_name, bucket_ref_unref.BucketRefError.name)
    t.assert_equals(res.n, 2)
end
