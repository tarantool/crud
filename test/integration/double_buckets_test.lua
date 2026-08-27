local t = require('luatest')
local json = require('json')
local fiber = require('fiber')

local utils = require('crud.common.utils')

local helpers = require('test.helper')

local TIMEOUT = 60

local function wait_balance(g, buckets_s1, buckets_s2)
    helpers.wait_active_bucket_count(g.cluster:server('s1-master'), buckets_s1)
    helpers.wait_active_bucket_count(g.cluster:server('s2-master'), buckets_s2)
end

local function balance_cluster(g)
    if g.params.backend == "config" then
        local cfg = g.cluster:cfg()
        cfg.sharding.rebalancer_max_sending = 15
        cfg.groups.storages.replicasets["s-1"].sharding = {
            weight = 1,
        }
        cfg.groups.storages.replicasets["s-2"].sharding = {
            weight = 1,
        }
        g.cluster:cfg(cfg)
        wait_balance(g, 1500, 1500)
    end
end

-- Returns a replicaset name if it is used as a sharding key, otherwise
-- returns a replicaset UUID.
local function get_replicaset_id(server)
    return server:exec(function()
        local name = box.info.replicaset.name
        if name == box.NULL then
            name = box.info.replicaset.uuid
        end
        return name
    end)
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
        cfg.sharding.rebalancer_max_sending = 15
        cfg.groups.storages.replicasets["s-1"].sharding = {
            weight = 0,
        }
        g.cluster:cfg(cfg)
        t.helpers.retrying({timeout=TIMEOUT}, function()
            local buckets_count = g.cluster:server('s1-master'):exec(function()
                return box.space._bucket:len()
            end)
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
        t.helpers.retrying({timeout=TIMEOUT}, function()
            local buckets_count = g.cluster:server('s2-master'):exec(function()
                return box.space._bucket:len()
            end)
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
        cfg.sharding.rebalancer_max_sending = 15
        cfg.groups.storages.replicasets["s-1"].sharding = {
            weight = 0,
        }
        g.cluster:cfg(cfg)
        local tuple_id = 1
        local not_applied_ids = {}
        local applied_ids = {}
        t.helpers.retrying({timeout=TIMEOUT}, function()
            if tuple_id > tuples_count then
                return
            end

            local buckets_count = g.cluster:server('s1-master'):exec(function()
                return box.space._bucket:len()
            end)
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
        t.helpers.retrying({timeout=TIMEOUT}, function()
            if tuple_id > tuples_count then
                return
            end

            local buckets_count = g.cluster:server('s2-master'):exec(function()
                return box.space._bucket:len()
            end)
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

local function restart_storage(g, server)
    server:stop()
    server:start()
    server:wait_for_rw()

    if g.params.backend == helpers.backend.VSHARD then
        local bootstrap_key = 'uuid'
        if type(g.params.backend_cfg) == 'table'
            and g.params.backend_cfg.identification_mode == 'name_as_key' then
            bootstrap_key = 'name'
        end

        server:exec(function(cfg, bootstrap_key)
            require('vshard.storage').cfg(cfg, box.info[bootstrap_key])
            require('crud').init_storage()
        end, {g.cfg, bootstrap_key})
    end
end

local pgroup_transfers = t.group('double_buckets_transfers', helpers.backend_matrix({
    {engine = 'memtx', safe_mode = false},
}, {skip_safe_mode = true}))

pgroup_transfers.before_all(function(g)
    helpers.start_default_cluster(g, 'srv_simple_operations')
    wait_balance(g, 1500, 1500)
end)

pgroup_transfers.after_all(function(g)
    helpers.stop_cluster(g.cluster, g.params.backend)
end)

pgroup_transfers.after_each(function(g)
    helpers.truncate_space_on_cluster(g.cluster, 'customers')

    helpers.set_safe_mode(g.cluster, false)
end)

--
-- Once the source became SENDING (READONLY since vshard 0.1.41),
-- bucket_refrw() reported its destination and CRUD retried there directly.
-- The destination must not accept that retry before bucket_recv() created
-- the bucket as READONLY/RECEIVING.
--
pgroup_transfers.test_delete_before_bucket_receive = function(g)
    t.skip_if(not utils.tarantool_version_at_least(3, 1),
        'test implemented only for 3.1 and greater'
    )

    local source = g.cluster:server('s1-master')
    local destination = g.cluster:server('s2-master')

    -- Create the tuple.
    local tuple_id = 8909
    local bucket_id = source:exec(function(id)
        local active = require('vshard.consts').BUCKET.ACTIVE
        local bucket = box.space._bucket.index.status:min({active})
        assert(bucket ~= nil)
        box.space.customers:replace({id, bucket.id, 'A', 42})
        return bucket.id
    end, {tuple_id})

    -- Populate the router cache with the source replicaset.
    local get_result, get_err = g.router:call('crud.get', {
        'customers', {tuple_id}, {bucket_id = bucket_id, mode = 'write'}})
    t.assert_equals(get_err, nil)
    t.assert_equals(#get_result.rows, 1)

    -- Start sending the bucket.
    destination:exec(function()
        require('vshard').storage.internal.errinj.ERRINJ_RECEIVE_DELAY = true
    end)
    local destination_id = get_replicaset_id(destination)
    -- Since vshard 0.1.41 a transfer starts in READONLY instead of SENDING,
    -- but the SENDING is kept for compatibility with older versions.
    local expected_status = require('vshard.consts').BUCKET.SENDING

    source:exec(function(bucket_id, destination_id, expected_status)
        local t = require('luatest')
        local fiber = require('fiber')
        local vshard = require('vshard')
        rawset(_G, 'send_fiber', fiber.new(function()
            return vshard.storage.bucket_send(bucket_id, destination_id)
        end))
        _G.send_fiber:set_joinable(true)
        t.helpers.retrying({timeout = 15}, function()
            t.assert_equals(box.space._bucket:get({bucket_id}).status,
                expected_status)
        end)
        -- Safe mode is enabled.
        t.assert(rawget(_G, '_crud').rebalance_safe_mode_status())
    end, {bucket_id, destination_id, expected_status})

    -- But destination doesn't have it yet.
    destination:exec(function(bucket_id) local t = require('luatest')
        t.assert_not(box.space._bucket:get({bucket_id}))
    end, {bucket_id})

    --
    -- The sender was in safe mode, it redirected the router to the node,
    -- which was not in safe mode yet: it hadn't received the bucket yet,
    -- but the request was done anyway, even without the bucket.
    --
    local delete_result, delete_err = g.router:call('crud.delete', {
        'customers', {tuple_id}, {bucket_id = bucket_id, timeout = 1},
    })

    -- delete should not succeed.
    t.assert(delete_err ~= nil, ('delete succeeded with result %s, but ' ..
        'tuple remained on destination'):format(json.encode(delete_result)))
    t.assert_str_contains(delete_err.err, "TRANSFER_IS_IN_PROGRESS")

    destination:exec(function()
        require('vshard').storage.internal.errinj.ERRINJ_RECEIVE_DELAY = false
    end)
    source:exec(function()
        local t = require('luatest')
        local fiber_ok, status, err = _G.send_fiber:join()
        t.assert(fiber_ok, status)
        t.assert(status, err)
    end)

    destination:exec(function(tuple_id)
        local t = require('luatest')
        t.assert(box.space.customers:get({tuple_id}))
    end, {tuple_id})
end

--
-- Safe mode is enabled by an on_replace trigger on _bucket. The
-- SENDING/RECEIVING rows replicate and fire the trigger on replicas too,
-- so the replica stays in safe mode as well as the master. It is also
-- persisted in the local settings space, so it stays enabled after a
-- restart.
--
pgroup_transfers.test_safe_mode_replicates_and_persists = function(g)
    t.skip_if(not utils.tarantool_version_at_least(3, 1),
        'test implemented only for 3.1 and greater'
    )
    local source = g.cluster:server('s1-master')
    local source_replica = g.cluster:server('s1-replica')
    local destination = g.cluster:server('s2-master')

    -- bucket_send requires both masters to be synced with their replicas
    -- (is_bucket_in_sync is set by a background vshard fiber).
    for _, server in ipairs({source, destination}) do
        t.helpers.retrying({timeout = TIMEOUT}, function()
            t.assert(server:exec(function()
                return require('vshard').storage.internal.is_bucket_in_sync
            end))
        end)
    end

    -- Insert data into a bucket and explicitly send it to the second
    -- replicaset.
    local destination_id = get_replicaset_id(destination)

    source:exec(function(destination_id)
        local vshard = require('vshard')
        local consts = require('vshard.consts')

        local bucket = box.space._bucket.index.status:min({consts.BUCKET.ACTIVE})
        assert(bucket ~= nil)

        box.space.customers:replace({1, bucket.id, 'Danila', 40})

        local ok, err = vshard.storage.bucket_send(bucket.id, destination_id)
        assert(ok, err)
    end, {destination_id})

    -- Safe mode is enabled on the master and, via replication, on the
    -- replica.
    t.assert(source:exec(function()
        return rawget(_G, '_crud').rebalance_safe_mode_status()
    end))
    t.helpers.retrying({timeout = TIMEOUT}, function()
        t.assert(source_replica:exec(function()
            return rawget(_G, '_crud').rebalance_safe_mode_status()
        end))
    end)

    -- Safe mode is persisted in the local settings space and stays enabled
    -- after the source is restarted.
    restart_storage(g, source)
    t.assert(source:exec(function()
        return rawget(_G, '_crud').rebalance_safe_mode_status()
    end))
end
