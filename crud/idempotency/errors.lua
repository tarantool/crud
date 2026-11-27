local errors = require('errors')

local CLASS_NAMES = {
    IdempotencyError = true,
    IdempotencyDuplicateError = true,
    IdempotencyStaleError = true,
    IdempotencyModeMismatchError = true,
}

local mod = {
    IdempotencyError = errors.new_class('IdempotencyError', {capture_stack = false}),
    IdempotencyDuplicateError = errors.new_class('IdempotencyDuplicateError', {capture_stack = false}),
    IdempotencyStaleError = errors.new_class('IdempotencyStaleError', {capture_stack = false}),
    IdempotencyModeMismatchError = errors.new_class(
        'IdempotencyModeMismatchError', {capture_stack = false}),
}

--- Returns true when the error belongs to one of the idempotency classes.
--- Used on the router to pass a storage-side idempotency error to the
--- client as is, without wrapping, so that the error fields (key, seq,
--- applied_max, mode) stay accessible (as atomic_batch does for
--- AtomicBatchError).
function mod.is_idempotency_error(err)
    return type(err) == 'table' and err.class_name ~= nil
        and CLASS_NAMES[err.class_name] == true
end

return mod
