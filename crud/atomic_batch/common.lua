---- Shared definitions for `crud.atomic_batch`.
-- @module crud.atomic_batch.common
--

local errors = require('errors')

local AtomicBatchError = errors.new_class('AtomicBatchError', {capture_stack = false})

return {
    AtomicBatchError = AtomicBatchError,
}
