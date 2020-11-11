local types = {}

local lt_by_key_type = {
    uuid = function (lhs, rhs)
        if lhs == nil and rhs ~= nil then return lhs end
        if lhs ~= nil and rhs == nil then return rhs end
        return lhs and rhs and lhs:str() < rhs:str()
    end
}

local eq_by_key_type = {
    uuid = function (lhs, rhs)
        return lhs == rhs
    end
}

function types.lt(key_part)
    return key_part and key_part.type and lt_by_key_type[key_part.type] or nil
end

function types.eq(key_part)
    return key_part and key_part.type and eq_by_key_type[key_part.type] or nil
end

return types
