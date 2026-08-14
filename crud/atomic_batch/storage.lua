---- Storage-side implementation of `crud.atomic_batch`.
-- @module crud.atomic_batch.storage
--

local clock = require('clock')

local bucket_ref_unref = require('crud.common.sharding.bucket_ref_unref')
local dev_checks = require('crud.common.dev_checks')
local schema = require('crud.common.schema')
local sharding = require('crud.common.sharding')

local common = require('crud.atomic_batch.common')

local storage = {}

local AtomicBatchExecutionError = common.AtomicBatchExecutionError
local CROSS_ENGINE_TXNS_SUPPORTED = common.CROSS_ENGINE_TXNS_SUPPORTED

local function unref_buckets(unref_fn)
    if unref_fn == nil then
        return true
    end

    local ok, unref_ok, unref_err = pcall(unref_fn)
    if not ok then
        return nil, AtomicBatchExecutionError:new('Failed to unref buckets: %s', tostring(unref_ok))
    end

    if not unref_ok then
        return nil, AtomicBatchExecutionError:new('Failed to unref buckets: %s', tostring(unref_err))
    end

    return true
end

-- Execute a single CRUD operation inside an open box transaction.
local function execute_single_op_on_storage(op, noreturn, space_fields)
    local space = box.space[op.space]
    if space == nil then
        return nil, AtomicBatchExecutionError:new("Space %q doesn't exist", op.space)
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
    return nil, AtomicBatchExecutionError:new("Unsupported operation type: %s", op.type)
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

-- Return error if mixed memtx/vinyl spaces are not supported, nil otherwise.
local function check_mvcc_for_mixed_engines(operations)
    local has_memtx = false
    local has_vinyl = false
    for _, op in ipairs(operations) do
        local space = box.space[op.space]
        if space ~= nil then
            if space.engine == 'vinyl' then
                has_vinyl = true
            else
                has_memtx = true
            end
        end
    end

    if not (has_memtx and has_vinyl) then
        return nil
    end

    if not box.cfg.memtx_use_mvcc_engine then
        return AtomicBatchExecutionError:new(
            "atomic_batch over mixed memtx and vinyl spaces requires MVCC " ..
            "(box.cfg.memtx_use_mvcc_engine = true)")
    end

    if not CROSS_ENGINE_TXNS_SUPPORTED then
        return AtomicBatchExecutionError:new(
            "atomic_batch over mixed memtx and vinyl spaces requires " ..
            "Tarantool 3.4.0 or newer (cross-engine transactions)")
    end

    return nil
end

------------------------------------------------------------------------
-- Storage-side main function
------------------------------------------------------------------------

local function atomic_batch_on_storage(operations, opts)
    dev_checks('table', {
        noreturn = '?boolean',
        fields = '?table',
        sharding_key_hash = '?number',
        sharding_func_hash = '?number',
        skip_sharding_hash_check = '?boolean',
    })

    opts = opts or {}

    local op_latencies = {}

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
        local op_started_at = clock.monotonic()
        local res, op_err = execute_single_op_on_storage(op, opts.noreturn, opts.fields)
        op_latencies[i] = clock.monotonic() - op_started_at

        if op_err ~= nil or (res ~= nil and res.err ~= nil) then
            execution_err = op_err or AtomicBatchExecutionError:new('%s', res.err)
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

        local err = AtomicBatchExecutionError:new(
            "Operation #%d (%s on %q) failed: %s",
            failed_index, failed_op.type, failed_op.space, execution_err
        )
        err.operation_index = failed_index
        err.operation_data = failed_op
        err.unref_error = unref_err

        return { err = err, op_latencies = op_latencies }
    end

    box.commit()

    local unref_ok, unref_err = unref_buckets(unref_fn)
    if not unref_ok then
        return nil, unref_err
    end

    return { data = results, op_latencies = op_latencies }
end

storage.storage_api = { [common.ATOMIC_BATCH_FUNC_NAME] = atomic_batch_on_storage }

return storage
