local checks = require('checks')
local errors = require('errors')
local vshard = require('vshard')

local call = require('elect.call')
local registry = require('elect.registry')

require('elect.checkers')

local UpdateError = errors.new_class('Update',  {capture_stack = false})

local update = {}

local UPDATE_FUNC_NAME = '__update'

local function call_update_on_storage(space_name, key, operations)
    checks('string', '?', 'update_operations')

    local space = box.space[space_name]
    if space == nil then
        return nil, UpdateError:new("Space %s doesn't exists", space_name)
    end

    local tuple = space:update(key, operations)
    return tuple:tomap({names_only = true})
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
-- @param table operations
--  Operations to be performed.
--  See `space_object:update` operations in Tarantool doc
--
-- @tparam ?number opts.timeout
--  Function call timeout
--
-- @return[1] object
-- @treturn[2] nil
-- @treturn[2] table Error description
--
function update.call(space_name, key, operations, opts)
    checks('string', '?', 'update_operations', {
        timeout = '?number',
    })

    opts = opts or {}

    local bucket_id = vshard.router.bucket_id(key)
    local replicaset, err = vshard.router.route(bucket_id)
    if replicaset == nil then
        return nil, UpdateError:new("Failed to get replicaset for bucket_id %s: %s", bucket_id, err.err)
    end

    local results, err = call.rw({
        func_name = UPDATE_FUNC_NAME,
        func_args = {space_name, key, operations},
        replicasets = {replicaset},
        timeout = opts.timeout,
    })

    if err ~= nil then
        return nil, UpdateError:new("Failed to update: %s", err)
    end

    return results[replicaset.uuid]
end

return update
