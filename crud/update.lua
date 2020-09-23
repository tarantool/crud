local checks = require('checks')
local errors = require('errors')
local vshard = require('vshard')

local call = require('crud.common.call')
local registry = require('crud.common.registry')
local utils = require('crud.common.utils')

require('crud.common.checkers')

local UpdateError = errors.new_class('Update',  {capture_stack = false})

local update = {}

local UPDATE_FUNC_NAME = '__update'

local function call_update_on_storage(space_name, key, operations)
    checks('string', '?', 'update_operations')

    local space = box.space[space_name]
    if space == nil then
        return nil, UpdateError:new("Space %q doesn't exists", space_name)
    end

    local tuple = space:update(key, operations)
    return tuple
end

function update.init()
    registry.add({
        [UPDATE_FUNC_NAME] = call_update_on_storage,
    })
end

--- Updates tuple in the specifed space
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
--  See `spaceect:update` operations in Tarantool doc
--
-- @tparam ?number opts.timeout
--  Function call timeout
--
-- @return[1] object
-- @treturn[2] nil
-- @treturn[2] table Error description
--
function update.call(space_name, key, user_operations, opts)
    checks('string', '?', 'update_operations', {
        timeout = '?number',
    })

    opts = opts or {}

    local space = utils.get_space(space_name, vshard.router.routeall())
    if space == nil then
        return nil, UpdateError:new("Space %q doesn't exists", space_name)
    end

    if box.tuple.is(key) then
        key = key:totable()
    end

    local operations, err = utils.convert_operations(user_operations, space:format())
    if err ~= nil then
        return nil, UpdateError:new("Wrong operations are specified: %s", err)
    end

    local bucket_id = vshard.router.bucket_id_strcrc32(key)
    local replicaset, err = vshard.router.route(bucket_id)
    if replicaset == nil then
        return nil, UpdateError:new("Failed to get replicaset for bucket_id %s: %s", bucket_id, err.err)
    end

    local results, err = call.rw(UPDATE_FUNC_NAME, {space_name, key, operations}, {
        replicasets = {replicaset},
        timeout = opts.timeout,
    })

    if err ~= nil then
        return nil, UpdateError:new("Failed to update: %s", err)
    end

    local tuple = results[replicaset.uuid]
    local object, err = utils.unflatten(tuple, space:format())
    if err ~= nil then
        return nil, UpdateError:new("Received tuple that doesn't match space format: %s", err)
    end

    return object
end

return update
