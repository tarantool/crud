local errors = require('errors')

local collations = require('crud.common.collations')
local TypeMismatchError = errors.new_class('TypeMismatchError')
local UnsupportedCollationError = errors.new_class('UnsupportedCollationError')

local types = {}

local function lt_nullable(cmp)
    return function(lhs, rhs)
        if lhs == nil and rhs ~= nil then
            return true
        elseif rhs == nil then
            return false
        end

        return cmp(lhs, rhs)
    end
end

local function lt_default(lhs, rhs)
    return lhs < rhs
end

local lt = lt_nullable(lt_default)

local function eq(lhs, rhs)
    return lhs == rhs
end

local function lt_boolean_default(lhs, rhs)
    local lhs_is_boolean = type(lhs) == 'boolean'
    local rhs_is_boolean = type(rhs) == 'boolean'

    if lhs_is_boolean and rhs_is_boolean then
        return (not lhs) and rhs
    elseif lhs_is_boolean or rhs_is_boolean then
        TypeMismatchError:assert(false, 'Could not compare boolean and not boolean')
    end
end

local lt_boolean = lt_nullable(lt_boolean_default)

local function lt_unicode(lhs, rhs)
    if type(lhs) == 'string' and type(rhs) == 'string' then
        return utf8.cmp(lhs, rhs) == -1
    end

    return lt(lhs, rhs)
end

local function lt_unicode_ci(lhs, rhs)
    if type(lhs) == 'string' and type(rhs) == 'string' then
        return utf8.casecmp(lhs, rhs) == -1
    end

    return lt(lhs, rhs)
end

local function eq_unicode(lhs, rhs)
    if type(lhs) == 'string' and type(rhs) == 'string' then
        return utf8.cmp(lhs, rhs) == 0
    end

    return lhs == rhs
end

local function eq_unicode_ci(lhs, rhs)
    if type(lhs) == 'string' and type(rhs) == 'string' then
        return utf8.casecmp(lhs, rhs) == 0
    end

    return lhs == rhs
end

local comparators_by_type = {
    boolean = function ()
        return lt_boolean, eq
    end,
    string = function (key_part)
        local collation = collations.get(key_part)
        if collations.is_default(collation) then
            return lt, eq
        elseif collation == collations.UNICODE then
            return lt_unicode, eq_unicode
        elseif collation == collations.UNICODE_CI then
            return lt_unicode_ci, eq_unicode_ci
        else
            UnsupportedCollationError:assert(false, 'Unsupported Tarantool collation %q', collation)
        end
    end,
}

function types.get_comparators_by_type(key_part)
    if comparators_by_type[key_part.type] then
        return comparators_by_type[key_part.type](key_part)
    else
        return lt, eq
    end
end

return types
