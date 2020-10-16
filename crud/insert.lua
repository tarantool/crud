local checks = require('checks')
local errors = require('errors')
local vshard = require('vshard')

local call = require('crud.common.call')
local utils = require('crud.common.utils')
local sharding = require('crud.common.sharding')
local dev_checks = require('crud.common.dev_checks')

local InsertError = errors.new_class('Insert',  {capture_stack = false})

local insert = {}

local INSERT_FUNC_NAME = '_crud.insert_on_storage'

local function insert_on_storage(space_name, tuple)
    dev_checks('string', 'table')

    local space = box.space[space_name]
    if space == nil then
        return nil, InsertError:new("Space %q doesn't exist", space_name)
    end

    return space:insert(tuple)
end

function insert.init()
   _G._crud.insert_on_storage = insert_on_storage
end

--- Inserts a tuple to the specified space
--
-- @function tuple
--
-- @param string space_name
--  A space name
--
-- @param table tuple
--  Tuple
--
-- @tparam ?number opts.timeout
--  Function call timeout
--
-- @tparam ?number opts.bucket_id
--  Bucket ID
--  (by default, it's vshard.router.bucket_id_strcrc32 of primary key)
--
-- @return[1] tuple
-- @treturn[2] nil
-- @treturn[2] table Error description
--
function insert.tuple(space_name, tuple, opts)
    checks('string', 'table', {
        timeout = '?number',
        bucket_id = '?number|cdata',
    })

    opts = opts or {}

    local space = utils.get_space(space_name, vshard.router.routeall())
    if space == nil then
        return nil, InsertError:new("Space %q doesn't exist", space_name)
    end
    local space_format = space:format()

    local bucket_id, err = sharding.tuple_set_and_return_bucket_id(tuple, space, opts.bucket_id)
    if err ~= nil then
        return nil, InsertError:new("Failed to get bucket ID: %s", err)
    end

    local result, err = call.rw_single(
        bucket_id, INSERT_FUNC_NAME,
        {space_name, tuple}, {timeout=opts.timeout})

    if err ~= nil then
        return nil, InsertError:new("Failed to insert: %s", err)
    end

    return {
        metadata = table.copy(space_format),
        rows = {result},
    }
end

--- Inserts an object to the specified space
--
-- @function object
--
-- @param string space_name
--  A space name
--
-- @param table obj
--  Object
--
-- @tparam ?table opts
--  Options of insert.tuple
--
-- @return[1] object
-- @treturn[2] nil
-- @treturn[2] table Error description
--
function insert.object(space_name, obj, opts)
    checks('string', 'table', '?table')

    opts = opts or {}

    local space = utils.get_space(space_name, vshard.router.routeall())
    if space == nil then
        return nil, InsertError:new("Space %q doesn't exist", space_name)
    end
    local space_format = space:format()

    local tuple, err = utils.flatten(obj, space_format)
    if err ~= nil then
        return nil, InsertError:new("Object is specified in bad format: %s", err)
    end

    return insert.tuple(space_name, tuple, opts)
end

return insert
