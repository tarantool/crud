local t = require('luatest')

local helpers = require('test.helper')

local g = t.group('idempotency_mark', helpers.backend_matrix({
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
end)

-- Calls the process_payment stored procedure via the external crud.storage_call
-- API (the user path from ADR 4.5). On success returns the new balance, on an
-- idempotency rejection returns the error object returned by the procedure
-- as data (the transaction was rolled back).
local function call_process(gg, id, amount, idem)
    local res, err = gg.router.net_box:call('crud.storage_call', {
        'process_payment', {id, amount, idem},
        {space_name = 'customers', key = {id}},
    })
    t.assert_equals(err, nil)
    return res.returns[1]
end

local function get_balance(gg)
    local res, err = gg.router.net_box:call(
        'crud.get', {'customers', {1}})
    t.assert_equals(err, nil)
    return res.rows[1][3]
end

local function get_account_bucket(gg)
    local res, err = gg.router.net_box:call(
        'crud.get', {'customers', {1}})
    t.assert_equals(err, nil)
    return res.rows[1][2]
end

-- Operator-level observation: fetch a marker tuple from any cluster server.
local function get_marker(gg, key, bucket_id)
    for _, server in ipairs(gg.cluster.servers) do
        if server.net_box ~= nil then
            local marker = server.net_box:eval([[
                local key, bucket_id = ...
                local function find(space_name)
                    local space = box.space[space_name]
                    if space == nil then
                        return nil
                    end
                    local tuple = space:get({key, bucket_id})
                    if tuple == nil then
                        return nil
                    end
                    return {
                        applied_max = tuple.applied_max,
                        expires_at = tuple.expires_at,
                    }
                end
                return find('_crud_idempotency')
                    or find('_crud_idempotency_vinyl')
            ]], {key, bucket_id})
            if marker ~= nil then
                return marker
            end
        end
    end
    return nil
end

g.test_monotone_apply_and_duplicate = function(gg)
    local idem = {key = 'payments-7', seq = 7, ttl = 3600}
    local balance = call_process(gg, 1, 100, idem)
    t.assert_equals(balance, 1100)

    local err = call_process(gg, 1, 100, idem) -- retry of the same (key, seq)
    t.assert_equals(err.class_name, 'IdempotencyDuplicateError')
    t.assert_equals(err.key, 'payments-7')
    t.assert_equals(err.seq, 7)
    t.assert_equals(err.applied_max, 7)

    t.assert_equals(get_balance(gg), 1100) -- retry did not distort the DML

    local marker = get_marker(gg, 'payments-7', get_account_bucket(gg))
    t.assert_equals(marker.applied_max, 7)
end

g.test_stale = function(gg)
    call_process(gg, 1, 100, {key = 'payments-7', seq = 7, ttl = 3600})
    local err = call_process(gg, 1, 100, {key = 'payments-7', seq = 6, ttl = 3600})
    t.assert_equals(err.class_name, 'IdempotencyStaleError')
    t.assert_equals(err.seq, 6)
    t.assert_equals(err.applied_max, 7)
    t.assert_equals(get_balance(gg), 1100)
end

g.test_mode_mismatch = function(gg)
    -- monotone marker, call without seq
    call_process(gg, 1, 100, {key = 'payments-7', seq = 7, ttl = 3600})
    local err = call_process(gg, 1, 100, {key = 'payments-7', ttl = 3600})
    t.assert_equals(err.class_name, 'IdempotencyModeMismatchError')
    t.assert_equals(err.mode, 'monotone')
    t.assert_equals(err.applied_max, 7)

    -- random marker, call with seq
    local ok = call_process(gg, 1, 100, {key = 'order-42', ttl = 3600})
    t.assert_equals(ok, 1200) -- 1100 after the monotone call + 100
    err = call_process(gg, 1, 100, {key = 'order-42', seq = 1, ttl = 3600})
    t.assert_equals(err.class_name, 'IdempotencyModeMismatchError')
    t.assert_equals(err.mode, 'random')
    t.assert_equals(err.applied_max, nil)
end

g.test_random_mode_duplicate = function(gg)
    local idem = {key = 'order-42', ttl = 3600}
    local ok = call_process(gg, 1, 100, idem)
    t.assert_equals(ok, 1100)
    local err = call_process(gg, 1, 100, idem)
    t.assert_equals(err.class_name, 'IdempotencyDuplicateError')
    t.assert_equals(err.seq, nil)
    t.assert_equals(err.applied_max, nil)

    local marker = get_marker(gg, 'order-42', get_account_bucket(gg))
    t.assert_equals(marker.applied_max, nil)
end

g.test_ttl_expiry = function(gg)
    call_process(gg, 1, 100, {key = 'payments-7', seq = 7, ttl = 2})
    gg.router.net_box:eval("require('fiber').sleep(3)")
    -- marker expired: the same (key, seq) applies anew
    local balance = call_process(gg, 1, 100, {key = 'payments-7', seq = 7, ttl = 3600})
    t.assert_equals(balance, 1200)
end

g.test_mark_outside_transaction = function(gg)
    helpers.call_on_storages(gg.cluster, function(server)
        local class_name = server.net_box:eval([[
            local _, err = require('crud').idempotency.mark{
                key = 'k', bucket_id = 1, engine = 'memtx'}
            if err == nil then
                return nil
            end
            return err.class_name
        ]])
        t.assert_equals(class_name, 'IdempotencyError')
    end)
end

g.test_mark_on_router = function(gg)
    local class_name = gg.router.net_box:eval([[
        local _, err = require('crud').idempotency.mark{
            key = 'k', bucket_id = 1, engine = 'memtx'}
        if err == nil then
            return nil
        end
        return err.class_name
    ]])
    t.assert_equals(class_name, 'IdempotencyError')
end

g.test_bad_opts = function(gg)
    local bad = {
        {key = ''},
        {key = string.rep('a', 256)},
        {key = 'k', seq = -1},
        {key = 'k', seq = 1.5},
        {key = 'k', ttl = 0},
        {key = 'k', extra = 1},
        42,
    }
    for _, idem in ipairs(bad) do
        local err = call_process(gg, 1, 100, idem)
        t.assert_equals(err.class_name, 'IdempotencyError', idem)
    end
end

g.test_marker_spaces_hidden_from_crud_schema = function(gg)
    local schema, err = gg.router.net_box:call('crud.schema')
    t.assert_equals(err, nil)
    t.assert_equals(schema['_crud_idempotency'], nil)
    t.assert_equals(schema['_crud_idempotency_vinyl'], nil)
end

g.test_marker_spaces_exist = function(gg)
    for _, server in ipairs(gg.cluster.servers) do
        if server.net_box ~= nil then
            -- Marker spaces live on storages only, routers have no _bucket.
            local is_storage = server.net_box:eval(
                'return box.space._bucket ~= nil')
            if is_storage then
                local ok, spaces = server.net_box:eval([[
                    local memtx = box.space['_crud_idempotency']
                    local vinyl = box.space['_crud_idempotency_vinyl']
                    if memtx == nil or vinyl == nil then
                        return false
                    end
                    return true, {memtx.engine, vinyl.engine}
                ]])
                t.assert(ok, 'marker spaces exist on ' .. server.alias)
                t.assert_equals(spaces[1], 'memtx')
                t.assert_equals(spaces[2], 'vinyl')
            end
        end
    end
end
