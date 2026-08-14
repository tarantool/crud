local checks = require('checks')
local errors = require('errors')

local call = require('crud.common.call')
local const = require('crud.common.const')
local utils = require('crud.common.utils')
local sharding = require('crud.common.sharding')
local sharding_key_module = require('crud.common.sharding.sharding_key')
local sharding_metadata_module = require('crud.common.sharding.sharding_metadata')
local dev_checks = require('crud.common.dev_checks')
local schema = require('crud.common.schema')
local bucket_ref_unref = require('crud.common.sharding.bucket_ref_unref')
local clock = require('clock')
local stats = require('crud.stats')

local AtomicBatchError = errors.new_class('AtomicBatchError', {capture_stack = false})
local AtomicBatchValidationError = errors.new_class('AtomicBatchValidationError', {capture_stack = false})

local atomic_batch = {}

local ATOMIC_BATCH_FUNC_NAME = 'atomic_batch_on_storage'
local CRUD_ATOMIC_BATCH_FUNC_NAME = utils.get_storage_call(ATOMIC_BATCH_FUNC_NAME)

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

local function unref_buckets(unref_fn)
    if unref_fn == nil then
        return true
    end

    local ok, unref_ok, unref_err = pcall(unref_fn)
    if not ok then
        return nil, AtomicBatchError:new('Failed to unref buckets: %s', tostring(unref_ok))
    end

    if not unref_ok then
        return nil, AtomicBatchError:new('Failed to unref buckets: %s', tostring(unref_err))
    end

    return true
end

local function observe_atomic_batch_stats(latency, operations, status)
    if not stats.is_enabled() then
        return
    end

    for _, op in ipairs(operations) do
        if type(op) == 'table' and type(op.space) == 'string' then
            local op_name = STORAGE_OP_TO_STATS_OP[op.type]
            if op_name ~= nil then
                stats.observe(latency, op.space, op_name, status)
            end
        end
    end
end

local function collect_unique_spaces(operations)
    local unique_spaces = {}
    for _, op in ipairs(operations) do
        if type(op) == 'table' and type(op.space) == 'string' then
            unique_spaces[op.space] = true
        end
    end

    return unique_spaces
end

local function observe_atomic_batch_stats_with_clock(started_at, operations, err)
    local status = err == nil and 'ok' or 'error'
    observe_atomic_batch_stats(clock.monotonic() - started_at, operations, status)
end

------------------------------------------------------------------------
-- Storage-side implementation
------------------------------------------------------------------------

-- Execute a single CRUD operation inside an open box transaction.
local function execute_single_op_on_storage(op, noreturn, space_fields)
    local space = box.space[op.space]
    if space == nil then
        return nil, AtomicBatchError:new("Space %q doesn't exist", op.space)
    end

    local field_names = space_fields and space_fields[op.space] or nil
    local wrap_opts = {
        add_space_schema_hash = op._add_schema_hash or false,
        field_names = field_names,
        noreturn = noreturn,
    }

    if op.type == 'insert' then
        return schema.wrap_func_result(space, space.insert, wrap_opts, space, op.tuple)
    elseif op.type == 'replace' then
        return schema.wrap_func_result(space, space.replace, wrap_opts, space, op.tuple)
    elseif op.type == 'upsert' then
        return schema.wrap_func_result(space, space.upsert, wrap_opts, space, op.tuple, op.operations)
    elseif op.type == 'update' then
        return schema.wrap_func_result(space, space.update, wrap_opts, space, op.key, op.operations)
    elseif op.type == 'delete' then
        return schema.wrap_func_result(space, space.delete, wrap_opts, space, op.key)
    elseif op.type == 'get' then
        return schema.wrap_func_result(space, space.get, wrap_opts, space, op.key)
    end
    return nil, AtomicBatchError:new("Unsupported operation type: %s", op.type)
end

-- Build bucket_id -> engine map; operations must already carry .bucket_id.
local function get_bucket_ids_engine(operations)
    local bucket_ids_engine = {}
    for _, op in ipairs(operations) do
        local space = box.space[op.space]
        if space ~= nil and op.bucket_id ~= nil then
            bucket_ids_engine[op.bucket_id] = space.engine
        end
    end
    return bucket_ids_engine
end

-- Return error if mixed memtx/vinyl without MVCC, nil otherwise.
local function check_mvcc_for_mixed_engines(operations)
    local has_memtx = false
    local has_vinyl = false
    for _, op in ipairs(operations) do
        local space = box.space[op.space]
        if space ~= nil then
            if space.engine == 'vinyl' then has_vinyl = true
            else has_memtx = true end
        end
    end
    if has_memtx and has_vinyl and not box.cfg.memtx_use_mvcc_engine then
        return AtomicBatchError:new(
            "atomic_batch over mixed memtx and vinyl spaces requires MVCC " ..
            "(box.cfg.memtx_use_mvcc_engine = true)")
    end
    return nil
end

local function atomic_batch_on_storage(operations, opts)
    dev_checks('table', {
        noreturn = '?boolean',
        fields = '?table',
        sharding_key_hash = '?number',
        sharding_func_hash = '?number',
        skip_sharding_hash_check = '?boolean',
    })

    opts = opts or {}

    -- Validate sharding hash for each unique space.
    if opts.skip_sharding_hash_check ~= true then
        local checked_spaces = {}
        for _, op in ipairs(operations) do
            if not checked_spaces[op.space] then
                local _, err = sharding.check_sharding_hash(
                    op.space, opts.sharding_func_hash, opts.sharding_key_hash,
                    opts.skip_sharding_hash_check)
                if err ~= nil then
                    return nil, err
                end
                checked_spaces[op.space] = true
            end
        end
    end

    local err = check_mvcc_for_mixed_engines(operations)
    if err ~= nil then
        return { err = err }
    end

    local bucket_ids_engine = get_bucket_ids_engine(operations)
    local ref_ok, ref_err, unref_fn = bucket_ref_unref.bucket_refrw_batch(bucket_ids_engine)
    if not ref_ok then
        return nil, ref_err
    end

    local results = {}
    local execution_err, failed_index, failed_op

    box.begin()

    for i, op in ipairs(operations) do
        local res, op_err = execute_single_op_on_storage(op, opts.noreturn, opts.fields)
        if op_err ~= nil or (res ~= nil and res.err ~= nil) then
            execution_err = op_err or AtomicBatchError:new('%s', res.err)
            failed_index = i
            failed_op = op
            break
        end
        if opts.noreturn ~= true then
            table.insert(results, res ~= nil and res.res or nil)
        end
    end

    if execution_err ~= nil then
        box.rollback()
        local _, unref_err = unref_buckets(unref_fn)

        local err = AtomicBatchError:new(
            "Operation #%d (%s on %q) failed: %s",
            failed_index, failed_op.type, failed_op.space, execution_err
        )
        err.operation_index = failed_index
        err.operation_data = failed_op
        err.unref_error = unref_err

        return { err = err }
    end

    box.commit()

    local unref_ok, unref_err = unref_buckets(unref_fn)
    if not unref_ok then
        return nil, unref_err
    end

    return { data = results }
end

atomic_batch.storage_api = {[ATOMIC_BATCH_FUNC_NAME] = atomic_batch_on_storage}

------------------------------------------------------------------------
-- Router-side helpers
------------------------------------------------------------------------

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
        return nil, AtomicBatchError:new(
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
            return nil, nil, AtomicBatchError:new("Cannot fetch primary index parts for space %q", op.space)
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

------------------------------------------------------------------------
-- Router-side main function
------------------------------------------------------------------------

local function load_prepare_context(vshard_router, timeout)
    local spaces, spaces_err = utils.get_spaces(vshard_router, {timeout = timeout})
    if spaces_err ~= nil then
        return nil, nil,
            AtomicBatchError:new("Failed to load spaces metadata: %s", spaces_err),
            const.NEED_SCHEMA_RELOAD
    end

    local known_replicasets, routeall_err = vshard_router:routeall()
    if known_replicasets == nil then
        return nil, nil,
            AtomicBatchError:new("Failed to get router replicasets: %s", tostring(routeall_err)),
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
            AtomicBatchError:new(
                "Op #%d (%s on %q): failed to route bucket_id %d: %s",
                i, op.type, op.space, bucket_id, tostring(route_err)
            ),
            const.NEED_SHARDING_RELOAD
    end

    local replicaset_id = replicaset_id_by_obj[replicaset]
    if replicaset_id == nil then
        return nil,
            AtomicBatchError:new(
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
                AtomicBatchError:new("Space %q doesn't exist", op.space),
                const.NEED_SCHEMA_RELOAD
        end

        local flat_op, err = flatten_op_object(vshard_router, op)
        if err ~= nil then
            return nil, err, const.NEED_SCHEMA_RELOAD
        end

        local bucket_id, sharding_data, err = op_get_bucket_id(vshard_router, flat_op, space)
        if err ~= nil then
            return nil,
                AtomicBatchError:new("Op #%d (%s on %q): %s", i, op.type, op.space, err),
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
            local err = AtomicBatchError:new(
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
        local err_wrapped = AtomicBatchError:new(
            "Failed to call atomic_batch on storage-side: %s", call_err)
        if sharding.result_needs_sharding_reload(call_err) then
            return nil, err_wrapped, const.NEED_SHARDING_RELOAD
        end
        return nil, err_wrapped
    end

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
        return nil, AtomicBatchError:new("Failed to format result metadata: %s", metadata_err)
    end

    return {
        metadata = metadata,
        data = storage_result ~= nil and storage_result.data or nil,
    }
end

--- Execute a batch of CRUD operations atomically on a single replicaset.
--
-- @function call
--
-- @param table operations
--  Array of operations to execute in a single transaction.
--  Every operation is a table with fields:
--
--  - `type`: `'get'`, `'insert'`, `'replace'`, `'update'`, `'upsert'` or `'delete'`.
--  - `space`: target space name.
--  - `tuple` or `object`: required for `insert`, `replace`, `upsert`.
--  - `key`: required for `get`, `update`, `delete`.
--  - `operations`: required for `update` and `upsert`.
--
-- @tparam ?number opts.timeout
--  Function call timeout.
-- @tparam ?boolean opts.noreturn
--  Suppress returning successfully processed tuples.
-- @tparam ?table opts.fields    per-space field projection
--  Output field names by space, same format as `{[space_name] = {field1, field2, ...}}`.
--
-- @return[1] table
--  `{metadata = {[space_name] = format}, data = {op_results...}}`.
--  `data[i]` matches the i-th operation in `operations` when `opts.noreturn ~= true`.
-- @treturn[2] nil
-- @treturn[2] table Error
--
function atomic_batch.call(operations, opts)
    checks('table', {
        timeout = '?number',
        noreturn = '?boolean',
        fields = '?table',
    })

    opts = opts or {}

    local vshard_router, err = utils.get_vshard_router_instance()
    if err ~= nil then
        return nil, AtomicBatchError:new(err)
    end

    local started_at = clock.monotonic()
    local unique_spaces = collect_unique_spaces(operations)
    local res, res_err = schema.wrap_func_reload(
        vshard_router,
        sharding.wrap_method_for_spaces,
        call_atomic_batch_on_router,
        unique_spaces,
        operations,
        opts
    )
    observe_atomic_batch_stats_with_clock(started_at, operations, res_err)

    return res, res_err
end

function atomic_batch.extract_space_names(operations)
    return collect_unique_spaces(operations)
end

return atomic_batch
