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

local function snapshot_returns(returns)
    -- Keep a separate representation before executing the next function.
    return msgpack.decode(msgpack.encode(returns))
end

--- Validates and executes one persistent function as the original user.
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
    if call_err ~= nil then
        return {
            error = storage_call_errors.new(
                ('Failed to execute function %q: %s'):format(
                    call_data.func_name,
                    storage_call_errors.message(call_err)
                ),
                call_data,
                -- The target may have committed changes before failing.
                true
            ),
        }
    end

    -- Serialization hooks belong to the target and must keep its privileges.
    local serializable, snapshot = pcall(
        box.session.su, run_as_user, snapshot_returns, returns
    )
    if not serializable then
        return {
            error = storage_call_errors.new(
                ('Function %q returned values that cannot be serialized to '
                    .. 'MessagePack: %s'):format(
                        call_data.func_name,
                        storage_call_errors.message(snapshot)
                    ),
                call_data,
                true
            ),
        }
    end

    return {returns = snapshot}
end

local function append_result(results, result, call_data)
    result.operation_index = call_data.operation_index
    table.insert(results, result)
end

--- Converts unexpected executor failures to item errors.
local function execute_safely(run_as_user, call_data)
    local ok, result = pcall(execute, run_as_user, call_data)
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

--- Executes calls bucket by bucket while holding write references.
---
--- A reference acquired for a bucket is released after all its calls, even
--- when execution raises an error.
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
