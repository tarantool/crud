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
            return {
                error = storage_call_errors.new(
                    storage_call_errors.message(err),
                    call_data,
                    false
                ),
            }
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
                true
            ),
        }
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

return executor
