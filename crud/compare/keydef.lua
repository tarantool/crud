local comparators = require('crud.compare.comparators')
local collations = require('crud.common.collations')

local compat = require('crud.common.compat')
local keydef_lib = compat.require('tuple.keydef', 'key_def')

-- As "tuple.key_def" doesn't support collation_id
-- we manually change it to collation
local function normalize_parts(index_parts)
    local result = {}

    for _, part in ipairs(index_parts) do
        if part.collation_id == nil then
            table.insert(result, part)
        else
            local part_copy = table.copy(part)
            part_copy.collation = collations.get(part)
            part_copy.collation_id = nil
            table.insert(result, part_copy)
        end
    end

    return result
end

local keydef_cache = {}
setmetatable(keydef_cache, {__mode = 'k'})

local function new(space, field_names, index_id)
    -- Get requested and primary index metainfo.
    local index = space.index[index_id]

    -- We use "index" as key here (not some string or something else)
    -- since cache should be invalidated on schema update.
    -- It will be done automatically because fetch_schema
    -- rewrites "index" table in space object.
    -- Later lua garbage collector will drop old
    -- value from "keydef_cache" table. Since it's a weak table with "k" mode.
    if field_names == nil and keydef_cache[index] ~= nil then
        return keydef_cache[index]
    end

    -- Create a key def
    local primary_index = space.index[0]
    local space_format = space:format()
    local updated_parts = comparators.update_key_parts_by_field_names(
            space_format, field_names, index.parts
    )

    local keydef = keydef_lib.new(normalize_parts(updated_parts))
    if not index.unique then
        updated_parts = comparators.update_key_parts_by_field_names(
                space_format, field_names, primary_index.parts
        )
        keydef = keydef:merge(keydef_lib.new(normalize_parts(updated_parts)))
    end

    if field_names == nil then
        keydef_cache[index] = keydef
    end

    return keydef
end

return {
    new = new,
}
