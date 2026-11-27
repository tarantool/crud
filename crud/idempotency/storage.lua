local fiber = require('fiber')

local idem_errors = require('crud.idempotency.errors')
local idempotency = require('crud.idempotency')
local spaces = require('crud.idempotency.spaces')

local storage = {}

storage.initialized = false

-- Fields of mark() opts on top of the validated key/seq/ttl: the routing
-- fields are provided by the stored procedure itself.
local MARK_ALLOWED_FIELDS = {key = true, seq = true, ttl = true,
                             bucket_id = true, engine = true}

local function attach(err, fields)
    for field, value in pairs(fields) do
        err[field] = value
    end
    return err
end

local function mode_of(marker)
    if marker.applied_max == nil then
        return 'random'
    end
    return 'monotone'
end

local function duplicate_err(key, bucket_id, seq, applied_max)
    local msg
    if applied_max == nil then
        msg = ("operation for key '%s' in bucket %s is already applied")
            :format(key, bucket_id)
    else
        msg = ("operation for key '%s' in bucket %s is already applied (applied_max=%s)")
            :format(key, bucket_id, applied_max)
    end
    return attach(idem_errors.IdempotencyDuplicateError:new(msg), {
        key = key, bucket_id = bucket_id, seq = seq, applied_max = applied_max})
end

-- An expired marker counts as absent (ADR 4.6): the sweeper is async.
local function get_live_marker(space, key, bucket_id)
    local marker = space:get({key, bucket_id})
    if marker ~= nil and marker.expires_at <= fiber.time() then
        return nil
    end
    return marker
end

--- Idempotency decision (ADR 4.2). Returns true when the operation should
--- be applied, or nil, an idempotency error.
function storage.check(space, key, bucket_id, seq)
    local marker = get_live_marker(space, key, bucket_id)

    -- The mode is fixed by a live marker: applied_max = nil means random.
    if marker ~= nil and (marker.applied_max == nil) ~= (seq == nil) then
        local mode = mode_of(marker)
        return nil, attach(idem_errors.IdempotencyModeMismatchError:new(
            "key '%s' in bucket %s is already used in %s mode", key, bucket_id, mode), {
            key = key, bucket_id = bucket_id, mode = mode,
            seq = seq, applied_max = marker.applied_max})
    end

    if seq == nil then
        if marker ~= nil then
            return nil, duplicate_err(key, bucket_id, nil, nil)
        end
    elseif marker ~= nil and seq <= marker.applied_max then
        if seq == marker.applied_max then
            return nil, duplicate_err(key, bucket_id, seq, marker.applied_max)
        end
        return nil, attach(idem_errors.IdempotencyStaleError:new(
            "value %s for key '%s' in bucket %s is stale (applied_max=%s)",
            seq, key, bucket_id, marker.applied_max), {
            key = key, bucket_id = bucket_id,
            seq = seq, applied_max = marker.applied_max})
    end

    return true
end

--- Marker commit: upsert with a sliding TTL prolongation (ADR 4.6).
function storage.commit_marker(space, key, bucket_id, seq, ttl)
    local expires_at = fiber.time() + (ttl or idempotency.DEFAULT_TTL)
    space:upsert(
        {key, bucket_id, seq or box.NULL, expires_at},
        {{'=', 3, seq or box.NULL}, {'=', 4, expires_at}}
    )
    return true
end

local function engine_space(engine)
    local space = spaces.get(engine)
    if space == nil then
        return nil, idem_errors.IdempotencyError:new(
            'idempotency marker space for engine %q is not initialized', engine)
    end
    return space
end

--- Public primitive for stored procedures (ADR 4.1).
function storage.mark(opts)
    local idem, err = idempotency.validate(opts, MARK_ALLOWED_FIELDS)
    if err ~= nil then
        return nil, err
    end

    local bucket_id = opts.bucket_id
    if type(bucket_id) ~= 'number' then
        return nil, idem_errors.IdempotencyError:new(
            'bucket_id must be a number, got %s', type(bucket_id))
    end

    local engine = opts.engine or 'memtx'
    if engine ~= 'memtx' and engine ~= 'vinyl' then
        return nil, idem_errors.IdempotencyError:new(
            "engine must be 'memtx' or 'vinyl', got %s", tostring(engine))
    end

    if not storage.initialized then
        return nil, idem_errors.IdempotencyError:new(
            'crud.idempotency.mark is available on a crud storage only')
    end
    if box.info.ro then
        return nil, idem_errors.IdempotencyError:new(
            'crud.idempotency.mark cannot be called on a read-only replica')
    end
    if not box.is_in_txn() then
        return nil, idem_errors.IdempotencyError:new(
            'crud.idempotency.mark must be called inside a transaction')
    end

    local space, space_err = engine_space(engine)
    if space_err ~= nil then
        return nil, space_err
    end

    local ok, check_err = storage.check(space, idem.key, bucket_id, idem.seq)
    if not ok then
        return nil, check_err
    end

    return storage.commit_marker(space, idem.key, bucket_id, idem.seq, idem.ttl)
end

--- For DML handlers: the options are already validated on the router.
--- Must be called inside the DML transaction, so the marker is committed
--- or rolled back together with the DML (ADR 4.1).
--- `_space_name` and `_operation` label the call site for the storage
--- metrics (passed since the first DML integration so the signature does
--- not change when metrics land).
--- Returns true or nil, an idempotency error.
function storage.mark_dml(idem, bucket_id, engine, _space_name, _operation) --luacheck: ignore
    if not storage.initialized then
        return nil, idem_errors.IdempotencyError:new(
            'crud storage is not initialized: crud.idempotency is unavailable')
    end

    local space, err = engine_space(engine)
    if err ~= nil then
        return nil, err
    end

    local ok, check_err = storage.check(space, idem.key, bucket_id, idem.seq)
    if not ok then
        return nil, check_err
    end

    return storage.commit_marker(space, idem.key, bucket_id, idem.seq, idem.ttl)
end

function storage.init()
    storage.initialized = true
end

function storage.deinit()
    storage.initialized = false
end

return storage
