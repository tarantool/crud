local types = {}

local lt_by_key_type = {
    uuid = function (lhs, rhs)
        return lhs:str() < rhs:str()
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