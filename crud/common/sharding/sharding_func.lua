local errors = require('errors')

local utils = require('crud.common.utils')

local ShardingFuncError = errors.new_class('ShardingFuncError',  {capture_stack = false})

local sharding_func_module = {}

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

    -- check is the each chunk an identifier
    for _, chunk in pairs(chunks) do
        if not utils.check_name_isident(chunk) or sharding_func == nil then
            return nil
        end
        sharding_func = rawget(sharding_func, chunk)
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

sharding_func_module.internal = {
    as_callable_object = as_callable_object,
    is_callable = is_callable,
}

return sharding_func_module
