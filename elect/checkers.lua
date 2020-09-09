require('checks')

function _G.checkers.funcs_map(p)
    if type(p) ~= 'table' then
        return false
    end

    for func_name, func in pairs(p) do
        if type(func_name) ~= 'string' then return false end
        if type(func) ~= 'function' then return false end
    end

    return true
end

function _G.checkers.strings_array(p)
    if type(p) ~= 'table' then
        return false
    end

    for i, str in pairs(p) do
        if type(i) ~= 'number' then return false end
        if type(str) ~= 'string' then return false end
    end

    return true
end

function _G.checkers.update_operations(p)
    if type(p) ~= 'table' then
        return false
    end

    for i, operation in pairs(p) do
        if type(i) ~= 'number' then return false end
        if type(operation) ~= 'table' then return false end
        if #operation ~= 3 then return false end

        if type(operation[1]) ~= 'string' then return false end
        if type(operation[2]) ~= 'number' and type(operation[2]) ~= 'string' then
            return false
        end
    end

    return true
end
