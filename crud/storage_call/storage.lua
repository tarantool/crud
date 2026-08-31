local msgpack = require('msgpack')
local vshard = require('vshard')

local sharding = require('crud.common.sharding')
local utils = require('crud.common.utils')
local storage_call_errors = require('crud.storage_call.errors')

local storage = {}

local STORAGE_FUNC_NAME = 'storage_call_on_storage'
local STORAGE_MANY_FUNC_NAME = 'storage_call_many_on_storage'

local function capture_returns(ok, ...)
    if not ok then
        return nil, (...)
    end

    local returns = {}
    for i = 1, select('#', ...) do
        local value = select(i, ...)
        if value == nil then
            value = box.NULL
        end
        returns[i] = value
    end

    return returns
end

local function invoke_box_func(func, args)
    return func:call(args)
end

local function try_rollback_open_transaction()
    if not box.is_in_txn() then
        return false
    end

    local ok, err = pcall(box.rollback)
    if not ok then
        return true, err
    end

    return true
end

local function append_cleanup_errors(message, cleanup_errors)
    if cleanup_errors.transaction_rollback == nil then
        return message
    end

    return ('%s; cleanup errors: transaction rollback: %s'):format(
        message,
        cleanup_errors.transaction_rollback
    )
end

local function new_execution_error(message, call_data,
                                   may_have_side_effects, cleanup_errors)
    local err = storage_call_errors.new(
        append_cleanup_errors(message, cleanup_errors),
        call_data,
        may_have_side_effects
    )
    if next(cleanup_errors) ~= nil then
        err.cleanup_errors = cleanup_errors
    end

    return {error = err}
end

local function is_target_execute_access_denied(err, func_name)
    local error_type = storage_call_errors.get_field(err, 'type')
    if error_type ~= 'AccessDeniedError' and error_type ~= 'ClientError' then
        return false
    end

    local expected = ("Execute access to function '%s' is denied"):format(
        func_name
    )
    local message = storage_call_errors.message(err)
    return message:sub(1, #expected) == expected
end

local function execute(run_as_user, call_data)
    local func = box.func[call_data.func_name]
    if func == nil then
        return {
            error = storage_call_errors.new(
                ('Function %q is not registered'):format(call_data.func_name),
                call_data,
                false
            ),
        }
    end

    if func.body == nil then
        return {
            error = storage_call_errors.new(
                ('Function %q is not persistent; its body must be stored in '
                    .. 'box.func'):format(call_data.func_name),
                call_data,
                false
            ),
        }
    end

    if func.setuid then
        return {
            error = storage_call_errors.new(
                ('Function %q has setuid enabled; storage_call does not allow '
                    .. 'privilege elevation'):format(call_data.func_name),
                call_data,
                false
            ),
        }
    end

    if call_data.skip_sharding_hash_check ~= true then
        local _, err = sharding.check_sharding_hash(
            call_data.space_name,
            call_data.sharding_func_hash,
            call_data.sharding_key_hash,
            false
        )
        if err ~= nil then
            local result_err = storage_call_errors.new(
                storage_call_errors.message(err),
                call_data,
                false
            )
            result_err.sharding_hash_mismatch = true
            return {error = result_err}
        end
    end

    local returns, call_err = capture_returns(pcall(
        box.session.su,
        run_as_user,
        invoke_box_func,
        func,
        call_data.args
    ))
    local transaction_left_open, rollback_err =
        try_rollback_open_transaction()

    local cleanup_errors = {}
    if rollback_err ~= nil then
        cleanup_errors.transaction_rollback =
            storage_call_errors.message(rollback_err)
    end

    if call_err ~= nil then
        local may_have_side_effects = not is_target_execute_access_denied(
            call_err,
            call_data.func_name
        )
        return new_execution_error(
            ('Failed to execute function %q: %s'):format(
                call_data.func_name,
                storage_call_errors.message(call_err)
            ),
            call_data,
            may_have_side_effects,
            cleanup_errors
        )
    end

    if transaction_left_open then
        local message = ('Function %q returned with an open transaction')
            :format(call_data.func_name)
        if rollback_err == nil then
            message = message .. '; the transaction was rolled back'
        end
        return new_execution_error(
            message,
            call_data,
            true,
            cleanup_errors
        )
    end

    if next(cleanup_errors) ~= nil then
        return new_execution_error(
            ('Function %q completed, but cleanup failed'):format(
                call_data.func_name
            ),
            call_data,
            true,
            cleanup_errors
        )
    end

    local serializable, serialization_err = pcall(msgpack.encode, returns)
    if not serializable then
        return {
            error = storage_call_errors.new(
                ('Function %q returned values that cannot be serialized to '
                    .. 'MessagePack: %s'):format(
                        call_data.func_name,
                        storage_call_errors.message(serialization_err)
                    ),
                call_data,
                true
            ),
        }
    end

    return {returns = returns}
end

local function append_result(results, result, call_data)
    result.operation_index = call_data.operation_index
    table.insert(results, result)
end

local function execute_safely(run_as_user, call_data)
    local ok, result = pcall(execute, run_as_user, call_data)
    if ok then
        return result
    end

    local _, rollback_err = try_rollback_open_transaction()
    local cleanup_errors = {}
    if rollback_err ~= nil then
        cleanup_errors.transaction_rollback =
            storage_call_errors.message(rollback_err)
    end

    if box.is_in_txn() then
        error(('%s; failed to clean up the open transaction: %s'):format(
            storage_call_errors.message(result),
            storage_call_errors.message(rollback_err)
        ))
    end

    return new_execution_error(
        ('Unexpected error while processing function %q: %s'):format(
            call_data.func_name,
            storage_call_errors.message(result)
        ),
        call_data,
        true,
        cleanup_errors
    )
end

local function execute_bucket_calls(results, run_as_user, bucket_calls)
    for _, call_data in ipairs(bucket_calls) do
        append_result(
            results,
            execute_safely(run_as_user, call_data),
            call_data
        )
    end
end

local function unref_bucket(bucket_id)
    local status, ok, err = pcall(
        vshard.storage.bucket_unrefrw,
        bucket_id
    )
    if not status then
        return nil, ok
    end

    return ok, err
end

local function append_bucket_ref_errors(results, bucket_calls, ref_err)
    for _, call_data in ipairs(bucket_calls) do
        append_result(results, {
            error = storage_call_errors.new(
                ('Failed to acquire a write reference for bucket %s: %s')
                    :format(
                        call_data.bucket_id,
                        storage_call_errors.message(ref_err)
                    ),
                call_data,
                false
            ),
        }, call_data)
    end
end

local function execute_many(run_as_user, calls_by_bucket)
    local results = {}

    for bucket_id, bucket_calls in pairs(calls_by_bucket) do
        local ref_ok, ref_err = vshard.storage.bucket_refrw(bucket_id)
        if not ref_ok then
            append_bucket_ref_errors(results, bucket_calls, ref_err)
            goto continue
        end

        local execute_ok, execute_err = pcall(
            execute_bucket_calls,
            results,
            run_as_user,
            bucket_calls
        )
        local unref_ok, unref_err = unref_bucket(bucket_id)

        if not execute_ok then
            local message = storage_call_errors.message(execute_err)
            if not unref_ok then
                message = ('%s; failed to release the write reference for '
                    .. 'bucket %s: %s'):format(
                        message,
                        bucket_id,
                        storage_call_errors.message(unref_err)
                    )
            end
            error(message)
        end

        if not unref_ok then
            error(('Failed to release the write reference for bucket %s: %s')
                :format(
                    bucket_id,
                    storage_call_errors.message(unref_err)
                ))
        end

        ::continue::
    end

    return results
end

local function assert_service_user()
    local service_user = utils.get_this_replica_user() or 'guest'
    -- user() identifies the IPROTO caller and is not changed by su() or
    -- setuid. effective_user() cannot be used for this trust boundary.
    local caller = box.session.user()

    storage_call_errors.class:assert(
        caller == service_user,
        'Access to the internal storage_call dispatcher is denied for user %q',
        caller
    )
end

local function storage_call_on_storage(run_as_user, call_data)
    assert_service_user()
    return execute_safely(run_as_user, call_data)
end

local function storage_call_many_on_storage(run_as_user, calls_by_bucket)
    assert_service_user()
    return execute_many(run_as_user, calls_by_bucket)
end

storage.func_name = STORAGE_FUNC_NAME
storage.func_many_name = STORAGE_MANY_FUNC_NAME
storage.storage_api = {
    [STORAGE_FUNC_NAME] = storage_call_on_storage,
    [STORAGE_MANY_FUNC_NAME] = storage_call_many_on_storage,
}

return storage
