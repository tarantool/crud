local msgpack = require('msgpack')

local sharding = require('crud.common.sharding')
local storage_call_errors = require('crud.storage_call.errors')

local executor = {}

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

function executor.execute(run_as_user, call_data)
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

function executor.execute_many(run_as_user, calls_by_bucket)
    local results = {}

    for _, bucket_calls in pairs(calls_by_bucket) do
        for _, call_data in ipairs(bucket_calls) do
            local result = executor.execute(run_as_user, call_data)
            result.operation_index = call_data.operation_index
            table.insert(results, result)
        end
    end

    return results
end

return executor
