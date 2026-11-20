local t = require('luatest')
local helpers = require('test.helper')
local utils = require('crud.common.utils')

local g = t.group('map_route_reset', helpers.backend_matrix({
    {engine = 'memtx', safe_mode = false},
}, {skip_safe_mode = true}))

g.before_all(function(g)
    helpers.start_default_cluster(g, 'srv_simple_operations')
    helpers.wait_active_bucket_count(g.cluster:server('s1-master'), 1500)
    helpers.wait_active_bucket_count(g.cluster:server('s2-master'), 1500)
end)

g.after_all(function(g)
    helpers.stop_cluster(g.cluster, g.params.backend)
end)

g.before_each(function(g)
    helpers.truncate_space_on_cluster(g.cluster, 'customers')
end)

local function replicaset_id_of(server)
    return server:exec(function()
        local name = box.info.replicaset.name
        if name == box.NULL then
            name = box.info.replicaset.uuid
        end
        return name
    end)
end

local function cached_route(g, bucket_id)
    return g.router:exec(function(bid)
        local rs = require('vshard').router.static.route_map[bid]
        return rs and (rs.id or rs.name or rs.uuid) or nil
    end, {bucket_id})
end

g.test_insert_many_resets_stale_route_cache = function(g)
    t.skip_if(g.params.backend ~= helpers.backend.CONFIG, 'config backend only')
    t.skip_if(not utils.tarantool_version_at_least(3, 1), 'test implemented only for 3.1 and greater')

    local source = g.cluster:server('s1-master')
    local destination = g.cluster:server('s2-master')
    local source_id = replicaset_id_of(source)
    local destination_id = replicaset_id_of(destination)

    local bucket_id = source:exec(function()
        local active = require('vshard.consts').BUCKET.ACTIVE
        local bucket = box.space._bucket.index.status:min({active})
        assert(bucket ~= nil)
        return bucket.id
    end)

    -- Warm up router cache: bucket_id -> source.
    local _, err = g.router:call('crud.insert', {'customers', {1, bucket_id, 'A', 1}})
    t.assert_equals(err, nil)
    t.assert_equals(cached_route(g, bucket_id), source_id)

    -- Move bucket to destination, cache becomes stale.
    source:exec(function(bid, dst)
        local ok, e = require('vshard').storage.bucket_send(bid, dst)
        assert(ok, e)
    end, {bucket_id, destination_id})

    -- Wait until migration is visible on both sides.
    helpers.retrying({timeout = 10}, function()
        local src_status = source:exec(function(bid)
            local b = box.space._bucket:get{bid}
            return b and b.status or nil
        end, {bucket_id})

        local dst_status = destination:exec(function(bid)
            local b = box.space._bucket:get{bid}
            return b and b.status or nil
        end, {bucket_id})

        t.assert_equals(src_status, 'sent')
        t.assert_equals(dst_status, 'active')
    end)

    t.assert_equals(cached_route(g, bucket_id), source_id)

    -- insert_many hits stale route and returns bucket_ref error.
    local _, errs = g.router:call('crud.insert_many', {'customers', {{2, bucket_id, 'B', 2}}})
    t.assert_not_equals(errs, nil)
    t.assert_str_contains(errs[1].err, 'failed bucket_ref')

    -- Fix expectation: route cache must be reset after bucket_ref error.
    t.assert_equals(cached_route(g, bucket_id), nil)

    -- Optional: next call should not keep pointing to source.
    g.router:call('crud.insert_many', {'customers', {{3, bucket_id, 'C', 3}}})
    local cr = cached_route(g, bucket_id)
    t.assert(cr == nil or cr == destination_id)
end
