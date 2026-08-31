local sharding_metadata = require(
    'crud.common.sharding.sharding_metadata'
)
local storage_call_errors = require('crud.storage_call.errors')

local batch = {}

--- Adds a routed call to the payload of its bucket.
function batch.add_call(calls_by_bucket, call_data)
    local bucket_calls = calls_by_bucket[call_data.bucket_id]
    if bucket_calls == nil then
        bucket_calls = {}
        calls_by_bucket[call_data.bucket_id] = bucket_calls
    end
    table.insert(bucket_calls, call_data)
end

--- Marks routed calls as not started when the deadline expires before Map.
function batch.mark_not_sent(results, expected_calls, original_calls)
    for operation_index, call_data in pairs(expected_calls) do
        results[operation_index] = {
            error = storage_call_errors.timeout_before_send({
                func_name = call_data.func_name,
                bucket_id = call_data.bucket_id,
                operation_index = operation_index,
                operation_data = original_calls[operation_index],
            }),
        }
    end

    return {results = results}
end

--- Validates Map responses and restores results to their input positions.
function batch.collect(vshard_router, map_results, original_calls, results,
                       expected_calls)
    for replicaset_id, response in pairs(map_results) do
        local storage_results = type(response) == 'table' and response[1]
            or nil
        if type(storage_results) ~= 'table' then
            return nil, storage_call_errors.invalid_storage_response(
                replicaset_id
            )
        end

        for _, storage_result in ipairs(storage_results) do
            local operation_index = type(storage_result) == 'table'
                and storage_result.operation_index or nil
            if not expected_calls[operation_index]
            or results[operation_index] ~= nil then
                return nil, storage_call_errors.invalid_storage_response(
                    replicaset_id
                )
            end

            if storage_result.error ~= nil then
                storage_result.error.operation_data =
                    original_calls[operation_index]
                storage_result.error.replicaset_id = replicaset_id
                results[operation_index] = {error = storage_result.error}

                if storage_call_errors.get_field(
                    storage_result.error,
                    'sharding_hash_mismatch'
                ) == true then
                    sharding_metadata.reload_sharding_cache(
                        vshard_router,
                        original_calls[operation_index].space_name
                    )
                end
            else
                results[operation_index] = {
                    returns = storage_result.returns,
                }
            end
        end
    end

    for operation_index, call_data in pairs(expected_calls) do
        if results[operation_index] == nil then
            results[operation_index] = {
                error = storage_call_errors.new(
                    'No result was returned for the operation',
                    {
                        func_name = call_data.func_name,
                        bucket_id = call_data.bucket_id,
                        operation_index = operation_index,
                        operation_data = original_calls[operation_index],
                    },
                    true
                ),
            }
        end
    end

    return {results = results}
end

return batch
