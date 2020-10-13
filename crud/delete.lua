local checks = require('checks')
local errors = require('errors')
local vshard = require('vshard')

local call = require('crud.common.call')
local utils = require('crud.common.utils')
local dev_checks = require('crud.common.dev_checks')

local DeleteError = errors.new_class('Delete',  {capture_stack = false})

local delete = {}

local DELETE_FUNC_NAME = '_crud.delete_on_storage'

local function delete_on_storage(space_name, key)
    dev_checks('string', '?')

    local space = box.space[space_name]
    if space == nil then
        return nil, DeleteError:new("Space %q doesn't exist", space_name)
    end

    local tuple = space:delete(key)
    return tuple
end

function delete.init()
   _G._crud.delete_on_storage = delete_on_storage
end

--- Deletes tuple from the specified space by key
--
-- @function call
--
-- @param string space_name
--  A space name
--
-- @param key
--  Primary key value
--
-- @tparam ?number opts.timeout
--  Function call timeout
--
-- @return[1] object
-- @treturn[2] nil
-- @treturn[2] table Error description
--
function delete.call(space_name, key, opts)
    checks('string', '?', {
        timeout = '?number',
    })

    opts = opts or {}


    local space = utils.get_space(space_name, vshard.router.routeall())
    if space == nil then
        return nil, DeleteError:new("Space %q doesn't exist", space_name)
    end

    if box.tuple.is(key) then
        key = key:totable()
    end

    local bucket_id = vshard.router.bucket_id_strcrc32(key)
    local results, err = call.rw_single(
        bucket_id, DELETE_FUNC_NAME,
        {space_name, key}, {timeout=opts.timeout})

    if err ~= nil then
        return nil, DeleteError:new("Failed to delete: %s", err)
    end

    return {
        metadata = table.copy(space:format()),
        rows = {results},
    }
end

return delete
