local checks = require('checks')
local errors = require('errors')
local vshard = require('vshard')

local call = require('crud.common.call')
local utils = require('crud.common.utils')
local sharding = require('crud.common.sharding')
local dev_checks = require('crud.common.dev_checks')

local UpdateError = errors.new_class('Update',  {capture_stack = false})

local update = {}

local UPDATE_FUNC_NAME = '_crud.update_on_storage'

local function update_on_storage(space_name, key, operations)
    dev_checks('string', '?', 'table')

    local space = box.space[space_name]
    if space == nil then
        return nil, UpdateError:new("Space %q doesn't exist", space_name)
    end

    local tuple = space:update(key, operations)
    return tuple
end

function update.init()
   _G._crud.update_on_storage = update_on_storage
end

--- Updates tuple in the specified space
--
-- @function call
--
-- @param string space_name
--  A space name
--
-- @param key
--  Primary key value
--
-- @param table user_operations
--  Operations to be performed.
--  See `space:update` operations in Tarantool doc
--
-- @tparam ?number opts.timeout
--  Function call timeout
--
-- @tparam ?number opts.bucket_id
--  Bucket ID
--  (by default, it's vshard.router.bucket_id_strcrc32 of primary key)
--
-- @return[1] object
-- @treturn[2] nil
-- @treturn[2] table Error description
--
function update.call(space_name, key, user_operations, opts)
    checks('string', '?', 'table', {
        timeout = '?number',
        bucket_id = '?number|cdata',
    })

    opts = opts or {}

    local space = utils.get_space(space_name, vshard.router.routeall())
    if space == nil then
        return nil, UpdateError:new("Space %q doesn't exist", space_name)
    end
    local space_format = space:format()

    if box.tuple.is(key) then
        key = key:totable()
    end

    local operations, err = utils.convert_operations(user_operations, space_format)
    if err ~= nil then
        return nil, UpdateError:new("Wrong operations are specified: %s", err)
    end

    local bucket_id = sharding.key_get_bucket_id(key, opts.bucket_id)
    local result, err = call.rw_single(
        bucket_id, UPDATE_FUNC_NAME, {space_name, key, operations},
        {timeout=opts.timeout})

    if err ~= nil then
        return nil, UpdateError:new("Failed to update: %s", err)
    end

    return {
        metadata = table.copy(space_format),
        rows = {result},
    }
end

return update
