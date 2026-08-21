local fiber = require('fiber')

local const = require('crud.common.const')
local utils = require('crud.common.utils')
local batch = require('crud.storage_call.batch')
local routing = require('crud.storage_call.routing')
local storage_call_errors = require('crud.storage_call.errors')
local storage = require('crud.storage_call.storage')

local router = {}

local CRUD_STORAGE_FUNC_NAME = utils.get_storage_call(storage.func_name)
local CRUD_STORAGE_MANY_FUNC_NAME = utils.get_storage_call(
    storage.func_many_name
)

local function remaining_timeout(deadline)
    return math.max(deadline - fiber.clock(), 0)
end

local function get_router(router_option)
    local vshard_router, err = utils.get_vshard_router_instance(router_option)
    if err ~= nil then
        return nil, storage_call_errors.class:new(
            '%s',
            storage_call_errors.message(err)
        )
    end

    return vshard_router
end

function router.call(func_name, args, opts)
    local vshard_router, err = get_router(opts.vshard_router)
    if err ~= nil then
        return nil, err
    end

    local timeout = opts.timeout or const.DEFAULT_VSHARD_CALL_TIMEOUT
    local deadline = fiber.clock() + timeout
    local route_data
    route_data, err = routing.single(vshard_router, opts, timeout)
    if err ~= nil then
        return nil, err
    end

    local call_data = {
        func_name = func_name,
        args = args,
        bucket_id = route_data.bucket_id,
        space_name = route_data.space_name,
        sharding_key_hash = route_data.sharding_key_hash,
        sharding_func_hash = route_data.sharding_func_hash,
        skip_sharding_hash_check = route_data.skip_sharding_hash_check,
    }

    local call_timeout = remaining_timeout(deadline)
    if call_timeout == 0 then
        return nil, storage_call_errors.timeout_before_send(call_data)
    end

    local result
    result, err = vshard_router:callrw(
        call_data.bucket_id,
        CRUD_STORAGE_FUNC_NAME,
        {box.session.effective_user(), call_data},
        {timeout = call_timeout}
    )
    if err ~= nil then
        return nil, storage_call_errors.new(
            storage_call_errors.message(err),
            call_data,
            true
        )
    end

    if result.error ~= nil then
        return nil, result.error
    end

    return result
end

function router.call_many(calls, opts)
    local calls_count, err = routing.array_length(calls)
    if err ~= nil then
        return nil, err
    end
    if calls_count == 0 then
        return {results = {}}
    end

    local vshard_router
    vshard_router, err = get_router(opts.vshard_router)
    if err ~= nil then
        return nil, err
    end

    local timeout = opts.timeout or const.DEFAULT_VSHARD_CALL_TIMEOUT
    local deadline = fiber.clock() + timeout
    local results = {}
    local expected_calls = {}
    local calls_by_bucket = {}

    for operation_index = 1, calls_count do
        local routed_call
        routed_call, err = routing.call(
            vshard_router,
            calls[operation_index],
            operation_index,
            remaining_timeout(deadline)
        )

        if err ~= nil then
            results[operation_index] = {error = err}
        else
            expected_calls[operation_index] = routed_call
            batch.add_call(calls_by_bucket, routed_call)
        end
    end

    if next(calls_by_bucket) == nil then
        return {results = results}
    end

    local map_timeout = remaining_timeout(deadline)
    if map_timeout == 0 then
        return batch.mark_not_sent(results, expected_calls, calls)
    end

    local map_results, map_err, replicaset_id = vshard_router:map_callrw(
        CRUD_STORAGE_MANY_FUNC_NAME,
        {box.session.effective_user()},
        {
            timeout = map_timeout,
            bucket_ids = calls_by_bucket,
        }
    )
    if map_err ~= nil then
        return nil, storage_call_errors.new(
            storage_call_errors.message(map_err),
            nil,
            true,
            replicaset_id
        )
    end

    return batch.collect(
        vshard_router,
        map_results,
        calls,
        results,
        expected_calls
    )
end

return router
