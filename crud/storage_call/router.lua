local fiber = require('fiber')
local checks = require('checks')

local const = require('crud.common.const')
local schema = require('crud.common.schema')
local sharding = require('crud.common.sharding')
local router_cache = require('crud.common.sharding.router_metadata_cache')
local utils = require('crud.common.utils')
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

local function array_length(value, where)
    if type(value) ~= 'table' then
        return nil, storage_call_errors.class:new(
            '%s must be an array', where
        )
    end

    local count = 0
    local max_index = 0

    for key in pairs(value) do
        if type(key) ~= 'number' or key < 1 or key % 1 ~= 0 then
            return nil, storage_call_errors.class:new(
                '%s must be an array', where
            )
        end
        count = count + 1
        max_index = math.max(max_index, key)
    end

    if count ~= max_index then
        return nil, storage_call_errors.class:new(
            '%s must not contain gaps', where
        )
    end

    return count
end

local function validate_route(route_data, bucket_count, where)
    local has_bucket_id = route_data.bucket_id ~= nil
    local has_space_name = route_data.space_name ~= nil
    local has_key = route_data.key ~= nil

    if has_bucket_id and (has_space_name or has_key) then
        return storage_call_errors.class:new(
            '%s must specify either bucket_id or space_name and key', where
        )
    end
    if not has_bucket_id and not (has_space_name and has_key) then
        return storage_call_errors.class:new(
            '%s must specify bucket_id or both space_name and key', where
        )
    end
    if has_space_name and type(route_data.space_name) ~= 'string' then
        return storage_call_errors.class:new(
            '%s.space_name must be a string', where
        )
    end
    if not has_bucket_id then
        return nil
    end

    local err = sharding.validate_bucket_id(route_data.bucket_id, where)
    if err ~= nil then
        return err
    end
    if tonumber(route_data.bucket_id) > bucket_count then
        return storage_call_errors.class:new(
            'Invalid bucket_id in %s: expected value in range [1, %d]',
            where, bucket_count
        )
    end
end

local function validate_call(call_data, operation_index, bucket_count)
    local where = ('calls[%d]'):format(operation_index)
    if type(call_data) ~= 'table' then
        return storage_call_errors.new(
            where .. ' must be a table', {operation_data = call_data}, false
        )
    end

    local err
    if type(call_data.func_name) ~= 'string' then
        err = where .. '.func_name must be a string'
    else
        if call_data.args ~= nil then
            local _, args_err = array_length(call_data.args, where .. '.args')
            if args_err ~= nil then
                err = storage_call_errors.message(args_err)
            end
        end
        if err == nil then
            local route_err = validate_route(call_data, bucket_count, where)
            if route_err == nil then
                return nil
            end
            err = storage_call_errors.message(route_err)
        end
    end

    return storage_call_errors.new(err, {
        func_name = call_data.func_name,
        bucket_id = call_data.bucket_id,
        operation_data = call_data,
    }, false)
end

local function add_call(calls_by_bucket, call_data)
    local bucket_calls = calls_by_bucket[call_data.bucket_id]
    if bucket_calls == nil then
        bucket_calls = {}
        calls_by_bucket[call_data.bucket_id] = bucket_calls
    end
    table.insert(bucket_calls, call_data)
end

local function mark_not_sent(results, expected_calls, original_calls)
    for operation_index, call_data in pairs(expected_calls) do
        results[operation_index] = {
            error = storage_call_errors.timeout_before_send({
                func_name = call_data.func_name,
                bucket_id = call_data.bucket_id,
                operation_data = original_calls[operation_index],
            }),
        }
    end

    return {results = results}
end

local function collect(map_results, original_calls, results, expected_calls)
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
                if sharding.result_needs_sharding_reload(
                    storage_result.error
                ) then
                    storage_result.error.may_have_side_effects = false
                end
                results[operation_index] = {error = storage_result.error}
            else
                results[operation_index] = {
                    returns = storage_result.returns,
                }
            end
        end
    end

    for operation_index in pairs(expected_calls) do
        if results[operation_index] == nil then
            return nil, storage_call_errors.invalid_storage_response()
        end
    end

    return {results = results}
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

local function call_once(vshard_router, func_name, args, opts, deadline,
                         bucket_count, context)
    local err, need_reload
    local route_data
    route_data, err, need_reload = routing.single(
        vshard_router,
        opts,
        deadline,
        bucket_count,
        context
    )
    if err ~= nil then
        return nil, err, need_reload
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
        if sharding.result_needs_sharding_reload(result.error) then
            result.error.may_have_side_effects = false
            result.error.func_name = func_name
            result.error.bucket_id = call_data.bucket_id
            if not context.sharding_reload_requested then
                context.sharding_reload_requested = true
                return nil, result.error, const.NEED_SHARDING_RELOAD
            end
        end
        return nil, result.error
    end

    return result
end

--- Routes and executes one persistent function on a storage.
function router.call(func_name, args, opts)
    checks('?', '?', {
        bucket_id = '?',
        space_name = '?string',
        key = '?',
        timeout = '?number',
        vshard_router = '?string|table',
    })

    if type(func_name) ~= 'string' then
        return nil, storage_call_errors.new(
            'func_name must be a string', nil, false
        )
    end

    if args == nil then
        args = {}
    end
    local _, args_err = array_length(args, 'args')
    if args_err ~= nil then
        return nil, storage_call_errors.new(
            storage_call_errors.message(args_err), nil, false
        )
    end

    local vshard_router, err = get_router(opts.vshard_router)
    if err ~= nil then
        return nil, err
    end

    local timeout = opts.timeout or const.DEFAULT_VSHARD_CALL_TIMEOUT
    local deadline = fiber.clock() + timeout
    local bucket_count = vshard_router:bucket_count()
    err = validate_route(opts, bucket_count, 'opts')
    if err ~= nil then
        return nil, storage_call_errors.new(
            storage_call_errors.message(err), nil, false
        )
    end

    local space_names = {}
    if opts.space_name ~= nil then
        space_names[opts.space_name] = true
    end
    return schema.wrap_func_reload(vshard_router, sharding.wrap_method_for_spaces,
        call_once, space_names, func_name, args, opts, deadline, bucket_count, {})
end

local function has_sharding_mismatch(results)
    local all_mismatch = true
    local any_mismatch = false
    for _, item in ipairs(results) do
        local mismatch = item.error ~= nil and
            sharding.result_needs_sharding_reload(item.error)
        any_mismatch = any_mismatch or mismatch
        all_mismatch = all_mismatch and mismatch
    end
    return any_mismatch, all_mismatch
end

local function call_many_once(vshard_router, calls, calls_count, deadline,
                              bucket_count, context)
    local err
    local results = {}
    local expected_calls = {}
    local calls_by_bucket = {}

    -- Validate the whole request before fetching schema or routing any item.
    for operation_index = 1, calls_count do
        err = validate_call(calls[operation_index], operation_index, bucket_count)
        if err ~= nil then
            results[operation_index] = {error = err}
        end
    end

    for operation_index = 1, calls_count do
        if results[operation_index] == nil then
            local routed_call, need_reload
            routed_call, err, need_reload = routing.call(
                vshard_router,
                calls[operation_index],
                operation_index,
                deadline,
                bucket_count,
                context
            )

            if err ~= nil then
                if need_reload == const.NEED_SCHEMA_RELOAD then
                    return nil, err, need_reload
                end
                results[operation_index] = {error = err}
            else
                expected_calls[operation_index] = routed_call
                add_call(calls_by_bucket, routed_call)
                if routed_call.space_name ~= nil then
                    context.space_names[routed_call.space_name] = true
                end
            end
        end
    end

    if next(calls_by_bucket) == nil then
        return {results = results}
    end

    local map_timeout = remaining_timeout(deadline)
    if map_timeout == 0 then
        return mark_not_sent(results, expected_calls, calls)
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

    local result
    result, err = collect(
        map_results,
        calls,
        results,
        expected_calls
    )
    if err ~= nil then
        return nil, err
    end

    local any_mismatch, all_mismatch = has_sharding_mismatch(result.results)
    if any_mismatch then
        if all_mismatch and not context.sharding_reload_requested then
            context.sharding_reload_requested = true
            return nil, result.results[1].error, const.NEED_SHARDING_RELOAD
        end
        router_cache.drop_instance(vshard_router)
    end

    return result
end

--- Routes calls and sends one Map request to all affected replicasets.
function router.call_many(calls, opts)
    checks('?', {
        timeout = '?number',
        vshard_router = '?string|table',
    })

    local calls_count, err = array_length(calls, 'calls')
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
    local bucket_count = vshard_router:bucket_count()

    local context = {space_names = {}}
    return schema.wrap_func_reload(vshard_router, sharding.wrap_method_for_spaces,
        call_many_once, context.space_names, calls, calls_count, deadline,
        bucket_count, context)
end

return router
