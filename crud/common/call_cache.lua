local func_name_to_func_cache = {}

local function func_name_to_func(func_name)
    if func_name_to_func_cache[func_name] then
        return func_name_to_func_cache[func_name]
    end

    local current = _G
    for part in string.gmatch(func_name, "[^%.]+") do
        current = rawget(current, part)
        if current == nil then
            error(("Function '%s' is not registered"):format(func_name))
        end
    end

    if type(current) ~= "function" then
        error(func_name .. " is not a function")
    end

    func_name_to_func_cache[func_name] = current
    return current
end

local function reset()
    func_name_to_func_cache = {}
end

return {
    func_name_to_func = func_name_to_func,
    reset = reset,
}