---- Shared constants and error classes for `crud.atomic_batch`.
-- @module crud.atomic_batch.common
--

local errors = require('errors')

local utils = require('crud.common.utils')

local AtomicBatchExecutionError = errors.new_class('AtomicBatchExecutionError', {capture_stack = false})
local AtomicBatchValidationError = errors.new_class('AtomicBatchValidationError', {capture_stack = false})

local ATOMIC_BATCH_FUNC_NAME = 'atomic_batch_on_storage'
local CRUD_ATOMIC_BATCH_FUNC_NAME = utils.get_storage_call(ATOMIC_BATCH_FUNC_NAME)

-- Cross-engine transactions (mixing memtx and vinyl spaces in a single
-- transaction) are supported only since Tarantool 3.4.0.
local CROSS_ENGINE_TXNS_SUPPORTED = utils.tarantool_version_at_least(3, 4, 0)

-- Supported operation types.
local SUPPORTED_OPERATIONS = {
    get = true, insert = true, replace = true,
    update = true, upsert = true, delete = true,
}
-- Operations that carry a tuple/object.
local TUPLE_OPERATIONS = { insert = true, replace = true, upsert = true }
-- Operations that use a primary-key lookup.
local KEY_OPERATIONS = { get = true, update = true, delete = true }

return {
    AtomicBatchExecutionError = AtomicBatchExecutionError,
    AtomicBatchValidationError = AtomicBatchValidationError,
    ATOMIC_BATCH_FUNC_NAME = ATOMIC_BATCH_FUNC_NAME,
    CRUD_ATOMIC_BATCH_FUNC_NAME = CRUD_ATOMIC_BATCH_FUNC_NAME,
    CROSS_ENGINE_TXNS_SUPPORTED = CROSS_ENGINE_TXNS_SUPPORTED,
    SUPPORTED_OPERATIONS = SUPPORTED_OPERATIONS,
    TUPLE_OPERATIONS = TUPLE_OPERATIONS,
    KEY_OPERATIONS = KEY_OPERATIONS,
}
