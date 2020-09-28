local json = require('json')

local COLLATION_NAME_FN = 2

local collations = {}

collations.NONE = 'none'
collations.UNICODE = 'unicode'
collations.UNICODE_CI = 'unicode_ci'

function collations.get(index_part)
    if index_part.collation ~= nil then
        return index_part.collation
    end

    if index_part.collation_id == nil then
        return collations.NONE
    end

    local collation_tuple = box.space._collation:get(index_part.collation_id)
    assert(collation_tuple ~= nil, "Unknown collation_id: " .. json.encode(index_part.collation_id))

    local collation = collation_tuple[COLLATION_NAME_FN]
    return collation
end

return collations
