local errors = require('errors')

return {
    IdempotencyError = errors.new_class('IdempotencyError', {capture_stack = false}),
    IdempotencyDuplicateError = errors.new_class('IdempotencyDuplicateError', {capture_stack = false}),
    IdempotencyStaleError = errors.new_class('IdempotencyStaleError', {capture_stack = false}),
    IdempotencyModeMismatchError = errors.new_class(
        'IdempotencyModeMismatchError', {capture_stack = false}),
}
