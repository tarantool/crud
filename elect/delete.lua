local checks = require('checks')
local errors = require('errors')
local vshard = require('vshard')

local call = require('elect.common.call')
local registry = require('elect.common.registry')

local DeleteError = errors.new_class('Delete',  {capture_stack = false})

local delete = {}

local DELETE_FUNC_NAME = '__delete'

local function call_delete_on_storage(space_name, key)
    checks('string', '?')

    local space = box.space[space_name]
    if space == nil then
        return nil, DeleteError:new("Space %s doesn't exists", space_name)
    end

    local tuple = space:delete(key)
    return tuple:tomap({names_only = true})
end

function delete.init()
    registry.add({
        [DELETE_FUNC_NAME] = call_delete_on_storage,
    })
end

--- Deletes tuple from the specifed space by key
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

    local bucket_id = vshard.router.bucket_id(key)
    local replicaset, err = vshard.router.route(bucket_id)
    if replicaset == nil then
        return nil, DeleteError:new("Failed to get replicaset for bucket_id %s: %s", bucket_id, err.err)
    end

    local results, err = call.rw({
        func_name = DELETE_FUNC_NAME,
        func_args = {space_name, key},
        replicasets = {replicaset},
        timeout = opts.timeout,
    })

    if err ~= nil then
        return nil, DeleteError:new("Failed to delete: %s", err)
    end

    return results[replicaset.uuid]
end

return delete
