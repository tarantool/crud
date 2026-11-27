local idem_errors = require('crud.idempotency.errors')

local MAX_KEY_LEN = 255
local DEFAULT_TTL = 86400

-- Fields of `opts.idempotency` passed by a crud client.
local ALLOWED_FIELDS = {key = true, seq = true, ttl = true}

local idempotency = {}

idempotency.MAX_KEY_LEN = MAX_KEY_LEN
idempotency.DEFAULT_TTL = DEFAULT_TTL

local function is_unsigned_integer(value)
    if type(value) == 'number' then
        return value >= 0 and value == math.floor(value) and value < 2 ^ 53
    end
    if type(value) == 'cdata' then
        -- uint64 after a msgpack round-trip
        local number = tonumber(value)
        return number ~= nil and number >= 0 and math.floor(number) == number
    end
    return false
end

--- Validate and normalize `opts.idempotency`.
---
--- `allowed_fields` extends the set of accepted option fields: `mark()`
--- additionally accepts the storage-side bucket_id and engine.
---
--- Returns a normalized `{key, seq, ttl}` map or nil, IdempotencyError.
function idempotency.validate(opts, allowed_fields)
    if type(opts) ~= 'table' then
        return nil, idem_errors.IdempotencyError:new(
            'idempotency must be a table, got %s', type(opts))
    end

    local allowed = allowed_fields or ALLOWED_FIELDS
    for field in pairs(opts) do
        if not allowed[field] then
            return nil, idem_errors.IdempotencyError:new(
                'idempotency.%s is not supported', tostring(field))
        end
    end

    local key = opts.key
    if type(key) ~= 'string' or #key == 0 or #key > MAX_KEY_LEN then
        return nil, idem_errors.IdempotencyError:new(
            'idempotency.key must be a non-empty string up to %d bytes', MAX_KEY_LEN)
    end

    local seq = opts.seq
    if seq ~= nil and not is_unsigned_integer(seq) then
        return nil, idem_errors.IdempotencyError:new(
            'idempotency.seq must be an unsigned integer')
    end

    local ttl = opts.ttl
    if ttl ~= nil and (type(ttl) ~= 'number' or ttl <= 0) then
        return nil, idem_errors.IdempotencyError:new(
            'idempotency.ttl must be a positive number of seconds')
    end

    return {key = key, seq = seq, ttl = ttl}
end

--- Primitive for stored procedures: validates the options, checks the
--- marker and commits it within the current transaction (ADR 4.1).
---
--- Must be called on a crud storage inside a transaction. Returns true
--- or nil, an idempotency error.
function idempotency.mark(opts)
    -- Required lazily: crud.idempotency.storage requires this module back.
    return require('crud.idempotency.storage').mark(opts)
end

return idempotency
