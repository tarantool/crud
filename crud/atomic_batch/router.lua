---- Router-side implementation of `crud.atomic_batch`.
-- @module crud.atomic_batch.router
--

local call = require('crud.common.call')
local const = require('crud.common.const')
local dev_checks = require('crud.common.dev_checks')
local schema = require('crud.common.schema')
local sharding = require('crud.common.sharding')
local utils = require('crud.common.utils')
local stats = require('crud.stats')

local common = require('crud.atomic_batch.common')

local router = {}

local AtomicBatchError = common.AtomicBatchError

local CRUD_ATOMIC_BATCH_FUNC_NAME = utils.get_storage_call('atomic_batch_on_storage')

-- Supported operation types.
local SUPPORTED_OPERATIONS = {
    get = true, insert = true, replace = true,
    update = true, upsert = true, delete = true,
}
-- Operations that carry a tuple/object.
local TUPLE_OPERATIONS = { insert = true, replace = true, upsert = true }
-- Operations that use a primary-key lookup.
local KEY_OPERATIONS = { get = true, update = true, delete = true }

local STORAGE_OP_TO_STATS_OP = {
    get = stats.op.GET,
    insert = stats.op.INSERT,
    replace = stats.op.REPLACE,
    update = stats.op.UPDATE,
    upsert = stats.op.UPSERT,
    delete = stats.op.DELETE,
}

------------------------------------------------------------------------
-- Router-side helpers
------------------------------------------------------------------------

local function collect_unique_spaces(operations)
    local unique_spaces = {}
    for _, op in ipairs(operations) do
        if type(op) == 'table' and type(op.space) == 'string' then
            unique_spaces[op.space] = true
        end
    end

    return unique_spaces
end

-- Observe storage-side execution latency of executed sub-operations.
-- `op_latencies` contains entries only for sub-operations that actually ran.
local function observe_atomic_batch_sub_op_stats(op_latencies, operations, status)
    if not stats.is_enabled() or op_latencies == nil then
        return
    end

    for i, latency in ipairs(op_latencies) do
        local op = operations[i]
        if type(op) == 'table' and type(op.space) == 'string' then
            local op_name = STORAGE_OP_TO_STATS_OP[op.type]
            if op_name ~= nil then
                stats.observe_atomic_batch_sub_op(latency, op.space, op_name, status)
            end
        end
    end
end

-- Validate a single operation descriptor, returns op or nil, err.
local function validate_op(op, op_index)
    if type(op) ~= 'table' then
        return nil, AtomicBatchError:new(
            "Operation #%d must be a table, got %s", op_index, type(op))
    end
    if type(op.type) ~= 'string' or not SUPPORTED_OPERATIONS[op.type] then
        return nil, AtomicBatchError:new(
            "Operation #%d has unsupported type %q " ..
            "(allowed: get, insert, replace, update, upsert, delete)",
            op_index, tostring(op.type))
    end
    if type(op.space) ~= 'string' then
        return nil, AtomicBatchError:new(
            "Operation #%d: 'space' must be a string", op_index)
    end
    if TUPLE_OPERATIONS[op.type] then
        if op.tuple == nil and op.object == nil then
            return nil, AtomicBatchError:new(
                "Operation #%d (%s): 'tuple' or 'object' is required", op_index, op.type)
        end
        if op.type == 'upsert' and type(op.operations) ~= 'table' then
            return nil, AtomicBatchError:new(
                "Operation #%d (upsert): 'operations' table is required", op_index)
        end
    end
    if KEY_OPERATIONS[op.type] then
        if op.key == nil then
            return nil, AtomicBatchError:new(
                "Operation #%d (%s): 'key' is required", op_index, op.type)
        end
        if op.type == 'update' and type(op.operations) ~= 'table' then
            return nil, AtomicBatchError:new(
                "Operation #%d (update): 'operations' table is required", op_index)
        end
    end
    return op
end

-- Flatten an operation's `.object` into a `.tuple` using the router schema.
-- Returns a (possibly new) op table, or nil, err.
local function flatten_op_object(vshard_router, op)
    if op.object == nil then
        return op
    end

    local tuple, err = utils.flatten_obj_reload(vshard_router, op.space, op.object)
    if err ~= nil then
        return nil, AtomicBatchError:new(
            "Failed to flatten object for operation on space %q: %s", op.space, err)
    end

    local flat_op = table.copy(op)
    flat_op.tuple = tuple
    flat_op.object = nil
    -- include schema hash on storage to detect a schema mismatch on the router
    flat_op._add_schema_hash = true
    return flat_op
end

-- Compute bucket_id for a single router-side operation (after flattening).
-- Returns { bucket_id = ..., sharding_data = ... } or nil, err, need_reload.
local function op_get_bucket_id(vshard_router, op, space)
    if TUPLE_OPERATIONS[op.type] then
        -- insert / replace / upsert: bucket_id is embedded in the tuple
        local sharding_data, err = sharding.tuple_set_and_return_bucket_id(
            vshard_router, op.tuple, space, nil)
        if err ~= nil then
            return nil, err
        end

        return {
            bucket_id = sharding_data.bucket_id,
            sharding_data = sharding_data,
        }
    end

    -- get / update / delete: derive bucket_id from the primary key
    local sharding_data, err, need_reload = sharding.key_set_and_return_bucket_id(
        vshard_router, space, op.key, nil)
    if err ~= nil then
        return nil, err, need_reload
    end

    return {
        bucket_id = sharding_data.bucket_id,
        sharding_data = sharding_data,
    }
end

local function load_prepare_context(vshard_router, timeout)
    local spaces, spaces_err = utils.get_spaces(vshard_router, {timeout = timeout})
    if spaces_err ~= nil then
        return nil,
            AtomicBatchError:new("Failed to load spaces metadata: %s", spaces_err),
            const.NEED_SCHEMA_RELOAD
    end

    return spaces
end

local function merge_sharding_meta(meta_by_space, space_name, sharding_data)
    local meta = meta_by_space[space_name]
    if meta == nil then
        meta = {
            sharding_func_hash = nil,
            sharding_key_hash = nil,
            skip_sharding_hash_check = true,
        }
        meta_by_space[space_name] = meta
    end

    if sharding_data.sharding_func_hash ~= nil then
        meta.sharding_func_hash = sharding_data.sharding_func_hash
    end
    if sharding_data.sharding_key_hash ~= nil then
        meta.sharding_key_hash = sharding_data.sharding_key_hash
    end
    if sharding_data.skip_sharding_hash_check ~= true then
        meta.skip_sharding_hash_check = false
    end
end

local function build_result_metadata(prepared_ops, spaces_cache, fields)
    local metadata = {}

    for _, op in ipairs(prepared_ops) do
        if metadata[op.space] == nil then
            local space = spaces_cache[op.space]
            if space ~= nil then
                local space_fields = fields and fields[op.space] or nil
                local fields_format, err = utils.get_fields_format(space:format(), space_fields)
                if err ~= nil then
                    return nil, err
                end

                metadata[op.space] = fields_format
            end
        end
    end

    return metadata
end

local function build_ops(operations)
    local ops = {}
    for i, op in ipairs(operations) do
        ops[i] = {type = op.type, space = op.space}
    end
    return ops
end

local function prepare_operations_on_router(vshard_router, operations, opts)
    local prepared_ops = {}
    local single_bucket_id = nil
    local sharding_meta_by_space = {}

    local spaces_cache, ctx_err, need_reload = load_prepare_context(vshard_router, opts.timeout)
    if ctx_err ~= nil then
        return nil, ctx_err, need_reload
    end

    for i, op in ipairs(operations) do
        local _, err = validate_op(op, i)
        if err ~= nil then
            return nil, err
        end

        local space = spaces_cache[op.space]
        if space == nil then
            return nil,
                AtomicBatchError:new("Space %q doesn't exist", op.space),
                const.NEED_SCHEMA_RELOAD
        end

        local flat_op, err = flatten_op_object(vshard_router, op)
        if err ~= nil then
            return nil, err
        end

        local bucket_id_data, err, need_reload = op_get_bucket_id(vshard_router, flat_op, space)
        if err ~= nil then
            return nil,
                AtomicBatchError:new("Op #%d (%s on %q): %s", i, op.type, op.space, err),
                need_reload
        end

        local bucket_id = bucket_id_data.bucket_id
        local sharding_data = bucket_id_data.sharding_data

        if single_bucket_id == nil then
            single_bucket_id = bucket_id
        elseif single_bucket_id ~= bucket_id then
            local err = AtomicBatchError:new(
                "Op #%d (%s on %q): bucket_id %d does not match bucket_id %d " ..
                "of previous operations. All ops in atomic_batch must target the same bucket.",
                i, op.type, op.space, bucket_id, single_bucket_id
            )
            err.operation_index = i
            err.operation_data = op

            return nil, err
        end

        merge_sharding_meta(sharding_meta_by_space, op.space, sharding_data)

        flat_op.bucket_id = bucket_id
        table.insert(prepared_ops, flat_op)
    end

    return {
        spaces_cache = spaces_cache,
        prepared_ops = prepared_ops,
        bucket_id = single_bucket_id,
        sharding_meta_by_space = sharding_meta_by_space,
    }
end

------------------------------------------------------------------------
-- Router-side main function
------------------------------------------------------------------------

-- Returns result, err, need_reload.
-- `need_reload` is either const.NEED_SCHEMA_RELOAD or const.NEED_SHARDING_RELOAD.
local function call_atomic_batch_on_router(vshard_router, operations, opts)
    dev_checks('table', 'table', {
        timeout = '?number',
        noreturn = '?boolean',
        fields = '?table',
    })

    if #operations == 0 then
        return {metadata = {}, data = {}, ops = {}}
    end

    local prepared, err, need_reload = prepare_operations_on_router(vshard_router, operations, opts)
    if err ~= nil then
        return nil, err, need_reload
    end

    local on_storage_opts = {
        noreturn = opts.noreturn,
        fields = opts.fields,
        sharding_meta = prepared.sharding_meta_by_space,
    }

    local storage_result, call_err = call.single(
        vshard_router, prepared.bucket_id,
        CRUD_ATOMIC_BATCH_FUNC_NAME,
        {prepared.prepared_ops, on_storage_opts},
        {mode = 'write', timeout = opts.timeout}
    )

    if call_err ~= nil then
        local err_wrapped = AtomicBatchError:new(
            "Failed to call atomic_batch on storage-side: %s", call_err)
        if sharding.result_needs_sharding_reload(call_err) then
            return nil, err_wrapped, const.NEED_SHARDING_RELOAD
        end
        return nil, err_wrapped
    end

    -- Observe sub-operation latency with the storage-side result status
    -- (`error` on rollback, `ok` otherwise).
    local op_latencies = storage_result ~= nil and storage_result.op_latencies or nil
    local sub_op_status = (storage_result ~= nil and storage_result.err ~= nil) and 'error' or 'ok'
    observe_atomic_batch_sub_op_stats(op_latencies, operations, sub_op_status)

    if storage_result ~= nil and storage_result.err ~= nil then
        local err = storage_result.err
        local failed_op = err.operation_data
        local failed_space = failed_op ~= nil and prepared.spaces_cache[failed_op.space] or nil
        if failed_space ~= nil and schema.result_needs_reload(failed_space, storage_result) then
            return nil, err, const.NEED_SCHEMA_RELOAD
        end
        return nil, err
    end

    if opts.noreturn == true then
        return nil
    end

    local metadata, metadata_err = build_result_metadata(
        prepared.prepared_ops,
        prepared.spaces_cache,
        opts.fields
    )
    if metadata_err ~= nil then
        return nil, AtomicBatchError:new("Failed to format result metadata: %s", metadata_err)
    end

    return {
        metadata = metadata,
        ops = build_ops(operations),
        data = storage_result ~= nil and storage_result.data or nil,
    }
end

router.call_on_router = call_atomic_batch_on_router
router.collect_unique_spaces = collect_unique_spaces

return router
