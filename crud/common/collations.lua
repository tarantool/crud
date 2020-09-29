local json = require('json')

local COLLATION_NAME_FN = 2

local collations = {}

collations.NONE = 'none'
collations.BINARY = 'binary'
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

function collations.is_default(collation)
    if collation == nil then
        return true
    end

    if collation == collations.NONE or collation == collations.BINARY then
        return true
    end

    return false
end

function collations.is_unicode(collation)
    if collation == nil then
        return false
    end

    return string.startswith(collation, 'unicode_')
end

return collations
