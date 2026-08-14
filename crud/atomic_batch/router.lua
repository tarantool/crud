---- Router-side implementation of `crud.atomic_batch`.
-- @module crud.atomic_batch.router
--

local call = require('crud.common.call')
local const = require('crud.common.const')
local dev_checks = require('crud.common.dev_checks')
local sharding = require('crud.common.sharding')
local sharding_key_module = require('crud.common.sharding.sharding_key')
local sharding_metadata_module = require('crud.common.sharding.sharding_metadata')
local utils = require('crud.common.utils')
local stats = require('crud.stats')

local common = require('crud.atomic_batch.common')

local router = {}

local AtomicBatchExecutionError = common.AtomicBatchExecutionError
local AtomicBatchValidationError = common.AtomicBatchValidationError
local CRUD_ATOMIC_BATCH_FUNC_NAME = common.CRUD_ATOMIC_BATCH_FUNC_NAME
local SUPPORTED_OPERATIONS = common.SUPPORTED_OPERATIONS
local TUPLE_OPERATIONS = common.TUPLE_OPERATIONS
local KEY_OPERATIONS = common.KEY_OPERATIONS

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

-- Observe storage-side execution latency of each sub-operation.
-- Sub-operations that were never executed are observed with zero latency.
local function observe_atomic_batch_sub_op_stats(op_latencies, operations, status)
    if not stats.is_enabled() then
        return
    end

    for i, op in ipairs(operations) do
        if type(op) == 'table' and type(op.space) == 'string' then
            local op_name = STORAGE_OP_TO_STATS_OP[op.type]
            if op_name ~= nil then
                local latency = op_latencies ~= nil and op_latencies[i] or 0
                stats.observe_atomic_batch_sub_op(latency, op.space, op_name, status)
            end
        end
    end
end

-- Validate a single operation descriptor, returns op or nil, err.
local function validate_op(op, op_index)
    if type(op) ~= 'table' then
        return nil, AtomicBatchValidationError:new(
            "Operation #%d must be a table, got %s", op_index, type(op))
    end
    if type(op.type) ~= 'string' or not SUPPORTED_OPERATIONS[op.type] then
        return nil, AtomicBatchValidationError:new(
            "Operation #%d has unsupported type %q " ..
            "(allowed: get, insert, replace, update, upsert, delete)",
            op_index, tostring(op.type))
    end
    if type(op.space) ~= 'string' then
        return nil, AtomicBatchValidationError:new(
            "Operation #%d: 'space' must be a string", op_index)
    end
    if TUPLE_OPERATIONS[op.type] then
        if op.tuple == nil and op.object == nil then
            return nil, AtomicBatchValidationError:new(
                "Operation #%d (%s): 'tuple' or 'object' is required", op_index, op.type)
        end
        if op.type == 'upsert' and type(op.operations) ~= 'table' then
            return nil, AtomicBatchValidationError:new(
                "Operation #%d (upsert): 'operations' table is required", op_index)
        end
    end
    if KEY_OPERATIONS[op.type] then
        if op.key == nil then
            return nil, AtomicBatchValidationError:new(
                "Operation #%d (%s): 'key' is required", op_index, op.type)
        end
        if op.type == 'update' and type(op.operations) ~= 'table' then
            return nil, AtomicBatchValidationError:new(
                "Operation #%d (update): 'operations' table is required", op_index)
        end
    end
    return op
end

-- Flatten an operation's .object field into a .tuple using the router schema.
-- Returns a (possibly new) op table, or nil, err.
-- The returned op has _add_schema_hash=true if flattening was done,
-- which tells the storage side to include schema hash in the result
-- so the router can detect a schema mismatch and retry.
local function flatten_op_object(vshard_router, op)
    if op.object == nil then
        return op
    end

    local tuple, err = utils.flatten_obj_reload(vshard_router, op.space, op.object)
    if err ~= nil then
        return nil, AtomicBatchExecutionError:new(
            "Failed to flatten object for operation on space %q: %s", op.space, err)
    end

    local flat_op = table.copy(op)
    flat_op.tuple = tuple
    flat_op.object = nil
    flat_op._add_schema_hash = true  -- detect schema mismatch on storage
    return flat_op
end

-- Compute bucket_id for a single router-side operation (after flattening).
-- Returns bucket_id, sharding_data, err
local function op_get_bucket_id(vshard_router, op, space)
    if TUPLE_OPERATIONS[op.type] then
        -- insert / replace / upsert: bucket_id is embedded in the tuple
        local sharding_data, err = sharding.tuple_set_and_return_bucket_id(
            vshard_router, op.tuple, space, nil)
        if err ~= nil then return nil, nil, err end
        return sharding_data.bucket_id, sharding_data, nil
    else
        -- get / update / delete: derive bucket_id from the primary key
        local skip_sharding_hash_check = nil

        if space.index[0] == nil then
            return nil, nil, AtomicBatchExecutionError:new("Cannot fetch primary index parts for space %q", op.space)
        end
        local primary_index_parts = space.index[0].parts

        local sharding_key_data, err = sharding_metadata_module.fetch_sharding_key_on_router(
            vshard_router, op.space)
        if err ~= nil then
            return nil, nil, err
        end

        local sharding_key, err = sharding_key_module.extract_from_pk(
            vshard_router, op.space, sharding_key_data.value,
            primary_index_parts, op.key)
        if err ~= nil then
            return nil, nil, err
        end

        local sharding_key_hash = sharding_key_data.hash

        local bucket_id_data, err = sharding.key_get_bucket_id(
            vshard_router, op.space, sharding_key, nil)
        if err ~= nil then
            return nil, nil, err
        end

        -- When sharding index is the primary index, bucket_id may be part of the key.
        sharding.fill_bucket_id_pk(space, op.key, bucket_id_data.bucket_id)

        return bucket_id_data.bucket_id, {
            bucket_id = bucket_id_data.bucket_id,
            sharding_func_hash = bucket_id_data.sharding_func_hash,
            sharding_key_hash = sharding_key_hash,
            skip_sharding_hash_check = skip_sharding_hash_check,
        }, nil
    end
end

local function load_prepare_context(vshard_router, timeout)
    local spaces, spaces_err = utils.get_spaces(vshard_router, {timeout = timeout})
    if spaces_err ~= nil then
        return nil, nil,
            AtomicBatchExecutionError:new("Failed to load spaces metadata: %s", spaces_err),
            const.NEED_SCHEMA_RELOAD
    end

    local known_replicasets, routeall_err = vshard_router:routeall()
    if known_replicasets == nil then
        return nil, nil,
            AtomicBatchExecutionError:new("Failed to get router replicasets: %s", tostring(routeall_err)),
            const.NEED_SHARDING_RELOAD
    end

    local replicaset_id_by_obj = {}
    for replicaset_id, replicaset in pairs(known_replicasets) do
        replicaset_id_by_obj[replicaset] = replicaset_id
    end

    return spaces, replicaset_id_by_obj
end

local function resolve_replicaset_id(vshard_router, bucket_id, replicaset_id_by_obj, i, op)
    local replicaset, route_err = vshard_router:route(bucket_id)
    if route_err ~= nil or replicaset == nil then
        return nil,
            AtomicBatchExecutionError:new(
                "Op #%d (%s on %q): failed to route bucket_id %d: %s",
                i, op.type, op.space, bucket_id, tostring(route_err)
            ),
            const.NEED_SHARDING_RELOAD
    end

    local replicaset_id = replicaset_id_by_obj[replicaset]
    if replicaset_id == nil then
        return nil,
            AtomicBatchExecutionError:new(
                "Op #%d (%s on %q): failed to determine replicaset id for bucket_id %d",
                i, op.type, op.space, bucket_id
            ),
            const.NEED_SHARDING_RELOAD
    end

    return replicaset_id
end

local function merge_sharding_meta(meta, sharding_data)
    if sharding_data.sharding_func_hash ~= nil then
        meta.sharding_func_hash = sharding_data.sharding_func_hash
    end
    if sharding_data.sharding_key_hash ~= nil then
        meta.sharding_key_hash = sharding_data.sharding_key_hash
    end
    if sharding_data.skip_sharding_hash_check ~= nil then
        meta.skip_sharding_hash_check = sharding_data.skip_sharding_hash_check
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

local function prepare_operations_on_router(vshard_router, operations, opts)
    local prepared_ops = {}
    local single_bucket_id = nil
    local target_replicaset_id = nil
    local sharding_meta = {
        sharding_func_hash = nil,
        sharding_key_hash = nil,
        skip_sharding_hash_check = nil,
    }

    local spaces_cache, replicaset_id_by_obj, ctx_err, need_reload =
        load_prepare_context(vshard_router, opts.timeout)
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
                AtomicBatchExecutionError:new("Space %q doesn't exist", op.space),
                const.NEED_SCHEMA_RELOAD
        end

        local flat_op, err = flatten_op_object(vshard_router, op)
        if err ~= nil then
            return nil, err, const.NEED_SCHEMA_RELOAD
        end

        local bucket_id, sharding_data, err = op_get_bucket_id(vshard_router, flat_op, space)
        if err ~= nil then
            return nil,
                AtomicBatchExecutionError:new("Op #%d (%s on %q): %s", i, op.type, op.space, err),
                const.NEED_SHARDING_RELOAD
        end

        local replicaset_id
        replicaset_id, err = resolve_replicaset_id(vshard_router, bucket_id, replicaset_id_by_obj, i, op)
        if err ~= nil then
            return nil, err, const.NEED_SHARDING_RELOAD
        end

        if target_replicaset_id == nil then
            target_replicaset_id = replicaset_id
        elseif target_replicaset_id ~= replicaset_id then
            local err = AtomicBatchExecutionError:new(
                "Op #%d (%s on %q): bucket_id %d belongs to replicaset %s, " ..
                "while previous operations target replicaset %s. " ..
                "All ops in atomic_batch must target the same replicaset.",
                i, op.type, op.space, bucket_id,
                tostring(replicaset_id), tostring(target_replicaset_id)
            )
            err.operation_index = i
            err.operation_data = op

            return nil, err
        end

        if single_bucket_id == nil then
            single_bucket_id = bucket_id
        end

        merge_sharding_meta(sharding_meta, sharding_data)

        local prepared_op = table.copy(flat_op)
        prepared_op.bucket_id = bucket_id
        table.insert(prepared_ops, prepared_op)
    end

    return {
        spaces_cache = spaces_cache,
        prepared_ops = prepared_ops,
        bucket_id = single_bucket_id,
        sharding_meta = sharding_meta,
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
        return {metadata = {}, data = {}}
    end

    local prepared, err, need_reload = prepare_operations_on_router(vshard_router, operations, opts)
    if err ~= nil then
        return nil, err, need_reload
    end

    local on_storage_opts = {
        noreturn = opts.noreturn,
        fields = opts.fields,
        sharding_func_hash = prepared.sharding_meta.sharding_func_hash,
        sharding_key_hash = prepared.sharding_meta.sharding_key_hash,
        skip_sharding_hash_check = prepared.sharding_meta.skip_sharding_hash_check,
    }

    local storage_result, call_err = call.single(
        vshard_router, prepared.bucket_id,
        CRUD_ATOMIC_BATCH_FUNC_NAME,
        {prepared.prepared_ops, on_storage_opts},
        {mode = 'write', timeout = opts.timeout}
    )

    if call_err ~= nil then
        local err_wrapped = AtomicBatchExecutionError:new(
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
        return nil, storage_result.err
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
        return nil, AtomicBatchExecutionError:new("Failed to format result metadata: %s", metadata_err)
    end

    return {
        metadata = metadata,
        data = storage_result ~= nil and storage_result.data or nil,
    }
end

router.call_on_router = call_atomic_batch_on_router
router.collect_unique_spaces = collect_unique_spaces

return router
