local errors = require('errors')

local StorageCallError = errors.new_class('StorageCallError', {
    capture_stack = false,
})

local storage_call_errors = {
    class = StorageCallError,
}

function storage_call_errors.get_field(err, field)
    if err == nil then
        return nil
    end

    local ok, value = pcall(function()
        return err[field]
    end)
    if ok then
        return value
    end

    return nil
end

function storage_call_errors.message(err)
    return storage_call_errors.get_field(err, 'err')
        or storage_call_errors.get_field(err, 'str')
        or storage_call_errors.get_field(err, 'message')
        or tostring(err)
end

function storage_call_errors.new(message, call_data, may_have_side_effects,
                                 replicaset_id)
    local err = StorageCallError:new('%s', message)
    err.may_have_side_effects = may_have_side_effects == true

    if call_data ~= nil then
        err.func_name = call_data.func_name
        err.bucket_id = call_data.bucket_id
        err.operation_index = call_data.operation_index
        err.operation_data = call_data.operation_data
    end

    if replicaset_id ~= nil then
        err.replicaset_id = replicaset_id
    end

    return err
end

function storage_call_errors.timeout_before_send(call_data)
    return storage_call_errors.new(
        'Timeout exceeded before the operation was sent to storage',
        call_data,
        false
    )
end

function storage_call_errors.invalid_storage_response(replicaset_id)
    return storage_call_errors.new(
        'Storage returned an invalid response',
        nil,
        true,
        replicaset_id
    )
end

return storage_call_errors
