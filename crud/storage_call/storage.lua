local sharding = require('crud.common.sharding')
local utils = require('crud.common.utils')
local storage_call_errors = require('crud.storage_call.errors')

local storage = {}

local STORAGE_FUNC_NAME = 'storage_call_on_storage'
local STORAGE_MANY_FUNC_NAME = 'storage_call_many_on_storage'

-- This function runs under the caller's identity. Both comparisons and error
-- formatting can invoke metamethods on values returned by a target function.
local function capture_returns(ok, ...)
    if not ok then
        local err = (...)
        local message_ok, message = pcall(storage_call_errors.message, err)
        if not message_ok then
            return nil, 'Target function raised an error that could not be formatted'
        end
        if type(message) ~= 'string' then
            message_ok, message = pcall(tostring, message)
            if not message_ok then
                return nil, 'Target function raised an error that could not be formatted'
            end
        end
        return nil, message
    end

    local returns = {}
    for i = 1, select('#', ...) do
        local value = select(i, ...)
        if type(value) == 'nil' then
            value = box.NULL
        end
        returns[i] = value
    end

    return returns
end

local function invoke_box_func(func, args)
    return func:call(args)
end

local function invoke_and_capture(func, args)
    return capture_returns(pcall(invoke_box_func, func, args))
end

--- Validates and executes one persistent function as the original user.
local function execute(call_data)
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

    local _, err = sharding.check_sharding_hash(
        call_data.space_name,
        call_data.sharding_func_hash,
        call_data.sharding_key_hash,
        call_data.skip_sharding_hash_check
    )
    if err ~= nil then
        return {error = err}
    end

    local returns, call_err = invoke_and_capture(func, call_data.args)
    if call_err ~= nil then
        return {
            error = storage_call_errors.new(
                ('Failed to execute function %q: %s'):format(
                    call_data.func_name,
                    call_err
                ),
                call_data,
                -- The target may have committed changes before failing.
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

local function rollback_open_transaction(call_data)
    if not box.is_in_txn() then
        return false
    end

    local rollback_ok, rollback_err = pcall(box.rollback)
    if not rollback_ok or box.is_in_txn() then
        error(('Failed to roll back an open transaction after function %q: %s')
            :format(call_data.func_name, storage_call_errors.message(rollback_err)))
    end
    return true
end

--- Converts unexpected executor failures to item errors.
local function execute_safely(call_data)
    local ok, result = pcall(execute, call_data)

    -- Unlike a separate IPROTO request, batch items share a fiber. Never let
    -- an unfinished transaction become part of the next item's execution.
    if rollback_open_transaction(call_data) then
        return {
            error = storage_call_errors.new(
                ('Function %q returned with an open transaction; it was rolled back')
                    :format(call_data.func_name),
                call_data,
                true
            ),
        }
    end

    if ok then
        return result
    end
    return {
        error = storage_call_errors.new(
            ('Unexpected error while processing function %q: %s'):format(
                call_data.func_name,
                storage_call_errors.message(result)
            ),
            call_data,
            true
        ),
    }
end

--- Executes calls under the storage-wide reference held by vshard Map.
local function execute_many(calls_by_bucket)
    local results = {}

    for _, bucket_calls in pairs(calls_by_bucket) do
        for _, call_data in ipairs(bucket_calls) do
            append_result(results, execute_safely(call_data), call_data)
        end
    end

    return results
end

--- Allows internal dispatchers to be called only by the vshard service user.
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
    return box.session.su(run_as_user, execute_safely, call_data)
end

local function storage_call_many_on_storage(run_as_user, calls_by_bucket)
    assert_service_user()
    return box.session.su(run_as_user, execute_many, calls_by_bucket)
end

storage.func_name = STORAGE_FUNC_NAME
storage.func_many_name = STORAGE_MANY_FUNC_NAME
storage.storage_api = {
    [STORAGE_FUNC_NAME] = storage_call_on_storage,
    [STORAGE_MANY_FUNC_NAME] = storage_call_many_on_storage,
}

return storage
