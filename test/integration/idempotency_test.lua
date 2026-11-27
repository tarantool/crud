local t = require('luatest')

local helpers = require('test.helper')

-- Integration tests for `opts.idempotency` in crud.insert / crud.replace.
-- Only external interfaces are exercised: crud calls over the net.box
-- protocol and operator-level box.space observations.

-- Calls the DML operation on the router. Returns result, err.
local function call_dml(gg, func_name, tuple, opts)
    return gg.router.net_box:call(func_name, {'customers', tuple, opts})
end

local function get_field(gg, id, fieldno)
    -- mode = 'write' routes the read to the master: right after a write
    -- the replica can lag behind and return no rows (vinyl).
    local res, err = gg.router.net_box:call(
        'crud.get', {'customers', {id}, {mode = 'write'}})
    t.assert_equals(err, nil)
    if res.rows[1] == nil then
        return nil
    end
    return res.rows[1][fieldno]
end

local function get_balance(gg, id)
    return get_field(gg, id, 3)
end

local function get_bucket(gg, id)
    return get_field(gg, id, 2)
end

-- Operator-level observation: in which marker engine the key marker is
-- present (replicas duplicate the master data, so presence is observed
-- instead of a count).
local function marker_presence(gg, key, bucket_id)
    local presence = {memtx = false, vinyl = false}
    for _, server in ipairs(gg.cluster.servers) do
        if server.net_box ~= nil then
            local found = server.net_box:eval([[
                local key, bucket_id = ...
                local function find(space_name)
                    local space = box.space[space_name]
                    if space == nil then
                        return false
                    end
                    return space:get({key, bucket_id}) ~= nil
                end
                return {find('_crud_idempotency'), find('_crud_idempotency_vinyl')}
            ]], {key, bucket_id})
            presence.memtx = presence.memtx or found[1]
            presence.vinyl = presence.vinyl or found[2]
        end
    end
    return presence
end

local function total_markers(gg)
    local total = 0
    for _, server in ipairs(gg.cluster.servers) do
        if server.net_box ~= nil then
            total = total + server.net_box:eval([[
                local function count(space_name)
                    local space = box.space[space_name]
                    if space == nil then
                        return 0
                    end
                    return space:count()
                end
                return count('_crud_idempotency') + count('_crud_idempotency_vinyl')
            ]])
        end
    end
    return total
end

-- Registers the symmetric scenario set on a group for one DML operation
-- (crud.insert or crud.replace). `bad_tuple` makes the storage-side DML
-- fail after the marker check: a duplicate PK for insert, a format
-- violation for replace.
local function setup_tests(g, func_name, bad_tuple, dml_error_class)
    g.test_duplicate = function(gg)
        local opts = {idempotency = {key = 'payments-7', seq = 1041, ttl = 3600}}
        local res, err = call_dml(gg, func_name, {2, box.NULL, 500}, opts)
        t.assert_equals(err, nil)
        t.assert_equals(res.rows[1][1], 2)
        local bucket_id = get_bucket(gg, 2)

        -- retry of the same (key, seq) with a different value
        res, err = call_dml(gg, func_name, {2, box.NULL, 777}, opts)
        t.assert_equals(res, nil)
        t.assert_equals(err.class_name, 'IdempotencyDuplicateError')
        t.assert_equals(err.key, 'payments-7')
        t.assert_equals(err.seq, 1041)
        t.assert_equals(err.applied_max, 1041)
        t.assert_equals(err.bucket_id, bucket_id)

        t.assert_equals(get_balance(gg, 2), 500) -- retry did not distort the DML
    end

    g.test_random_mode = function(gg)
        local idem = {key = 'order-42', ttl = 3600}
        local res, err = call_dml(gg, func_name, {2, box.NULL, 500}, {idempotency = idem})
        t.assert_equals(err, nil)
        t.assert_equals(res.rows[1][1], 2)

        res, err = call_dml(gg, func_name, {2, box.NULL, 777}, {idempotency = idem})
        t.assert_equals(res, nil)
        t.assert_equals(err.class_name, 'IdempotencyDuplicateError')
        t.assert_equals(err.seq, nil)
        t.assert_equals(err.applied_max, nil)

        t.assert_equals(get_balance(gg, 2), 500)
    end

    g.test_stale = function(gg)
        call_dml(gg, func_name, {2, box.NULL, 500},
            {idempotency = {key = 'payments-7', seq = 10, ttl = 3600}})
        local res, err = call_dml(gg, func_name, {2, box.NULL, 777},
            {idempotency = {key = 'payments-7', seq = 9, ttl = 3600}})
        t.assert_equals(res, nil)
        t.assert_equals(err.class_name, 'IdempotencyStaleError')
        t.assert_equals(err.seq, 9)
        t.assert_equals(err.applied_max, 10)
        t.assert_equals(get_balance(gg, 2), 500)
    end

    g.test_mode_mismatch = function(gg)
        -- monotone marker, call without seq
        call_dml(gg, func_name, {2, box.NULL, 500},
            {idempotency = {key = 'payments-7', seq = 10, ttl = 3600}})
        local res, err = call_dml(gg, func_name, {2, box.NULL, 777},
            {idempotency = {key = 'payments-7', ttl = 3600}})
        t.assert_equals(res, nil)
        t.assert_equals(err.class_name, 'IdempotencyModeMismatchError')
        t.assert_equals(err.mode, 'monotone')
        t.assert_equals(err.applied_max, 10)

        -- random marker, call with seq
        call_dml(gg, func_name, {3, box.NULL, 600},
            {idempotency = {key = 'order-42', ttl = 3600}})
        res, err = call_dml(gg, func_name, {3, box.NULL, 888},
            {idempotency = {key = 'order-42', seq = 1, ttl = 3600}})
        t.assert_equals(res, nil)
        t.assert_equals(err.class_name, 'IdempotencyModeMismatchError')
        t.assert_equals(err.mode, 'random')
        t.assert_equals(err.applied_max, nil)
    end

    g.test_ttl_expiry = function(gg)
        local res, err = call_dml(gg, func_name, {2, box.NULL, 500},
            {idempotency = {key = 'payments-7', seq = 7, ttl = 2}})
        t.assert_equals(err, nil)
        t.assert_equals(res.rows[1][1], 2)
        gg.router.net_box:eval("require('fiber').sleep(3)")

        -- marker expired: the same (key, seq) applies anew
        res, err = call_dml(gg, func_name, {3, box.NULL, 600},
            {idempotency = {key = 'payments-7', seq = 7, ttl = 3600}})
        t.assert_equals(err, nil)
        t.assert_equals(res.rows[1][1], 3)
        t.assert_equals(get_balance(gg, 3), 600)
    end

    g.test_marker_rolls_back_with_dml = function(gg)
        local opts = {idempotency = {key = 'broken-1', seq = 5, ttl = 3600}}
        local bucket_id = get_bucket(gg, 1)

        -- the DML fails: the marker must be rolled back along with it
        local res, err = call_dml(gg, func_name, bad_tuple, opts)
        t.assert_equals(res, nil)
        t.assert_equals(err.class_name, dml_error_class)

        local counts = marker_presence(gg, 'broken-1', bucket_id)
        t.assert_equals(counts, {memtx = false, vinyl = false})

        -- the marker is absent, so the retry applies
        res, err = call_dml(gg, func_name, {2, box.NULL, 500}, opts)
        t.assert_equals(err, nil)
        t.assert_equals(res.rows[1][1], 2)
    end

    g.test_marker_engine_matches_data_engine = function(gg)
        local _, err = call_dml(gg, func_name, {2, box.NULL, 500},
            {idempotency = {key = 'eng-1', seq = 3, ttl = 3600}})
        t.assert_equals(err, nil)
        local bucket_id = get_bucket(gg, 2)

        local counts = marker_presence(gg, 'eng-1', bucket_id)
        if gg.params.engine == 'memtx' then
            t.assert_equals(counts, {memtx = true, vinyl = false})
        else
            t.assert_equals(counts, {memtx = false, vinyl = true})
        end
    end

    g.test_without_idempotency_unchanged = function(gg)
        -- regression: without the option the behavior is the same as before
        local res, err = call_dml(gg, func_name, {2, box.NULL, 500}, nil)
        t.assert_equals(err, nil)
        t.assert_equals(res.rows[1][1], 2)

        res, err = call_dml(gg, func_name, {2, box.NULL, 777}, {})
        if func_name == 'crud.insert' then
            t.assert_equals(res, nil)
            t.assert_equals(err.class_name, dml_error_class)
        else
            t.assert_equals(err, nil)
            t.assert_equals(get_balance(gg, 2), 777)
        end

        -- no idempotency markers were committed
        t.assert_equals(total_markers(gg), 0)
    end

    g.test_bad_opts = function(gg)
        local bad = {
            {key = ''},
            {key = string.rep('a', 256)},
            {key = 'k', seq = -1},
            {key = 'k', seq = 1.5},
            {key = 'k', ttl = 0},
            {key = 'k', extra = 1},
        }
        for _, idem in ipairs(bad) do
            local res, err = call_dml(gg, func_name, {2, box.NULL, 500},
                {idempotency = idem})
            t.assert_equals(res, nil, idem)
            t.assert_equals(err.class_name, 'IdempotencyError', idem)
        end

        -- validation happens before any storage call: no markers, no rows
        t.assert_equals(total_markers(gg), 0)
        t.assert_equals(get_balance(gg, 2), nil)
    end
end

local function make_group(group_name)
    local g = t.group(group_name, helpers.backend_matrix({
        {engine = 'memtx'},
        {engine = 'vinyl'},
    }))

    g.before_all(function(gg)
        helpers.start_default_cluster(gg, 'srv_idempotency')
    end)

    g.after_all(function(gg)
        helpers.stop_cluster(gg.cluster, gg.params.backend)
    end)

    g.before_each(function(gg)
        helpers.truncate_space_on_cluster(gg.cluster, 'customers')
        helpers.truncate_space_on_cluster(gg.cluster, '_crud_idempotency')
        helpers.truncate_space_on_cluster(gg.cluster, '_crud_idempotency_vinyl')

        local res, err = gg.router.net_box:call(
            'crud.insert', {'customers', {1, box.NULL, 1000}})
        t.assert_equals(err, nil)
        t.assert_equals(res.rows[1][1], 1)

        -- The router discovers buckets in the background: wait until the
        -- buckets used by the tests are routed to avoid the WRONG_BUCKET
        -- race on the first calls.
        t.helpers.retrying({timeout = 10, delay = 0.1}, function()
            for _, id in ipairs({2, 3}) do
                local _, get_err = gg.router.net_box:call(
                    'crud.get', {'customers', {id}})
                t.assert_equals(get_err, nil)
            end
        end)
    end)

    return g
end

-- A duplicate PK makes the insert fail after the marker check.
setup_tests(make_group('idempotency_insert'), 'crud.insert',
    {1, box.NULL, 999}, 'InsertError')

-- A format violation (balance must be a number) makes the replace fail
-- after the marker check.
setup_tests(make_group('idempotency_replace'), 'crud.replace',
    {1, box.NULL, 'not-a-number'}, 'ReplaceError')
