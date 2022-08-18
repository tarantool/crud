local errors = require('errors')
local log = require('log')

local dev_checks = require('crud.common.dev_checks')
local router_cache = require('crud.common.sharding.router_metadata_cache')
local utils = require('crud.common.utils')

local ShardingFuncError = errors.new_class('ShardingFuncError',  {capture_stack = false})

local sharding_func_module = {}

local sharding_module_names = {
    ['vshard'] = true,
}

local function is_callable(object)
    if type(object) == 'function' then
        return true
    end

    -- all objects with type `cdata` are allowed
    -- because there is no easy way to get
    -- metatable.__call of object with type `cdata`
    if type(object) == 'cdata' then
        return true
    end

    local object_metatable = getmetatable(object)
    if (type(object) == 'table' or type(object) == 'userdata') then
        -- if metatable type is not `table` -> metatable is protected ->
        -- cannot detect metamethod `__call` exists
        if object_metatable and type(object_metatable) ~= 'table' then
            return true
        end

        -- `__call` metamethod can be only the `function`
        -- and cannot be a `table` | `userdata` | `cdata`
        -- with `__call` methamethod on its own
        if object_metatable and object_metatable.__call then
            return type(object_metatable.__call) == 'function'
        end
    end

    return false
end

local function get_function_from_G(func_name)
    local chunks = string.split(func_name, '.')
    local sharding_func = _G
    local sharding_module = false
    local ok

    if sharding_module_names[chunks[1]] then
        ok, sharding_func = pcall(require, chunks[1])
        if not ok then
            return nil
        end

        sharding_module = true
        table.remove(chunks, 1)
    end

    -- check is the each chunk an identifier
    for _, chunk in pairs(chunks) do
        if not utils.check_name_isident(chunk) or sharding_func == nil then
            return nil
        end

        -- `vshard` store sharding functions in metatable,
        -- this metatable is common for all `vshard` routers.
        -- That's why for `vshard` case we can't use rawget.
        if sharding_module then
            sharding_func = sharding_func[chunk]
        else
            sharding_func = rawget(sharding_func, chunk)
        end
    end

    return sharding_func
end

local function as_callable_object(sharding_func_def, space_name)
    if type(sharding_func_def) == 'string' then
        local sharding_func = get_function_from_G(sharding_func_def)
        if sharding_func ~= nil and is_callable(sharding_func) == true then
            return sharding_func
        end
    end

    if type(sharding_func_def) == 'table' then
        local sharding_func, err = loadstring('return ' .. sharding_func_def.body)
        if sharding_func == nil then
            return nil, ShardingFuncError:new(
                    "Body is incorrect in sharding_func for space (%s): %s", space_name, err)
        end
        return sharding_func()
    end

    return nil, ShardingFuncError:new(
            "Wrong sharding function specified in _ddl_sharding_func space for (%s) space", space_name
    )
end

function sharding_func_module.construct_as_callable_obj_cache(vshard_router, metadata_map, specified_space_name)
    dev_checks('table', 'table', 'string')

    local result_err

    local cache = router_cache.get_instance(vshard_router)
    cache[router_cache.SHARDING_FUNC_MAP_NAME] = {}
    local func_cache = cache[router_cache.SHARDING_FUNC_MAP_NAME]

    cache[router_cache.META_HASH_MAP_NAME][router_cache.SHARDING_FUNC_MAP_NAME] = {}
    local func_hash_cache = cache[router_cache.META_HASH_MAP_NAME][router_cache.SHARDING_FUNC_MAP_NAME]

    for space_name, metadata in pairs(metadata_map) do
        if metadata.sharding_func_def ~= nil then
            local sharding_func, err = as_callable_object(metadata.sharding_func_def,
                                                          space_name)
            if err ~= nil then
                if specified_space_name == space_name then
                    result_err = err
                    log.error(err)
                else
                    log.warn(err)
                end
            end

            func_cache[space_name] = sharding_func
            func_hash_cache[space_name] = metadata.sharding_func_hash
        end
    end

    return result_err
end

sharding_func_module.internal = {
    as_callable_object = as_callable_object,
    is_callable = is_callable,
}

return sharding_func_module
