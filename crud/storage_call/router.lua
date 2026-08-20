local fiber = require('fiber')

local const = require('crud.common.const')
local utils = require('crud.common.utils')
local routing = require('crud.storage_call.routing')
local storage_call_errors = require('crud.storage_call.errors')
local storage = require('crud.storage_call.storage')

local router = {}

local CRUD_STORAGE_FUNC_NAME = utils.get_storage_call(storage.func_name)

local function remaining_timeout(deadline)
    return math.max(deadline - fiber.clock(), 0)
end

function router.call(func_name, args, opts)
    local vshard_router, err = utils.get_vshard_router_instance(
        opts.vshard_router
    )
    if err ~= nil then
        return nil, storage_call_errors.class:new(
            '%s',
            storage_call_errors.message(err)
        )
    end

    local timeout = opts.timeout or const.DEFAULT_VSHARD_CALL_TIMEOUT
    local deadline = fiber.clock() + timeout
    local route_data
    route_data, err = routing.single(vshard_router, opts, timeout)
    if err ~= nil then
        return nil, storage_call_errors.class:new(
            '%s',
            storage_call_errors.message(err)
        )
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


return router
