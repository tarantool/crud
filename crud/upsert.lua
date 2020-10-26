local checks = require('checks')
local errors = require('errors')
local vshard = require('vshard')

local call = require('crud.common.call')
local utils = require('crud.common.utils')
local dev_checks = require('crud.common.dev_checks')

local UpsertError = errors.new_class('UpsertError',  { capture_stack = false})

local upsert = {}

local UPSERT_FUNC_NAME = '_crud.upsert_on_storage'

local function upsert_on_storage(space_name, tuple, operations)
    dev_checks('string', 'table', 'table')

    local space = box.space[space_name]
    if space == nil then
        return nil, UpsertError:new("Space %q doesn't exist", space_name)
    end

    return space:upsert(tuple, operations)
end

function upsert.init()
   _G._crud.upsert_on_storage = upsert_on_storage
end

--- Update or insert a tuple in the specified space
--
-- @function tuple
--
-- @param string space_name
--  A space name
--
-- @param table tuple
--  Tuple
--
-- @param table user_operations
--  user_operations to be performed.
--  See `space:upsert()` operations in Tarantool doc
--
-- @tparam ?number opts.timeout
--  Function call timeout
--
-- @return[1] tuple
-- @treturn[2] nil
-- @treturn[2] table Error description
--
function upsert.tuple(space_name, tuple, user_operations, opts)
    checks('string', '?', 'table', {
        timeout = '?number',
    })

    opts = opts or {}

    local space = utils.get_space(space_name, vshard.router.routeall())
    if space == nil then
        return nil, UpsertError:new("Space %q doesn't exist", space_name)
    end

    local space_format = space:format()
    local operations, err = utils.convert_operations(user_operations, space_format)
    if err ~= nil then
        return nil, UpsertError:new("Wrong operations are specified: %s", err)
    end

    local key = utils.extract_key(tuple, space.index[0].parts)

    local bucket_id = vshard.router.bucket_id_strcrc32(key)
    local bucket_id_fieldno, err = utils.get_bucket_id_fieldno(space)
    if err ~= nil then
        return nil, err
    end

    if tuple[bucket_id_fieldno] ~= nil then
        return nil, UpsertError:new("Unexpected value (%s) at field %s (bucket_id)",
                tuple[bucket_id_fieldno], bucket_id_fieldno)
    end

    tuple[bucket_id_fieldno] = bucket_id
    local _, err = call.rw_single(bucket_id, UPSERT_FUNC_NAME, {space_name, tuple, operations}, {
         timeout = opts.timeout
    })

    if err ~= nil then
        return nil, UpsertError:new("Failed to upsert: %s", err)
    end

    -- upsert always returns nil
    return {
        metadata = table.copy(space_format),
        rows = {},
    }
end

--- Update or insert an object in the specified space
--
-- @function object
--
-- @param string space_name
--  A space name
--
-- @param table obj
--  Object
--
-- @param table user_operations
--  user_operations to be performed.
--  See `space:upsert()` operations in Tarantool doc
--
-- @tparam ?number opts.timeout
--  Function call timeout
--
-- @return[1] object
-- @treturn[2] nil
-- @treturn[2] table Error description
--
function upsert.object(space_name, obj, user_operations, opts)
    checks('string', '?', 'table', {
        timeout = '?number',
    })

    opts = opts or {}

    local space = utils.get_space(space_name, vshard.router.routeall())
    if space == nil then
        return nil, UpsertError:new("Space %q doesn't exist", space_name)
    end

    local space_format = space:format()

    local tuple, err = utils.flatten(obj, space_format)
    if err ~= nil then
        return nil, UpsertError:new("Object is specified in bad format: %s", err)
    end

    return upsert.tuple(space_name, tuple, user_operations, opts)
end

return upsert
