local checks = require('checks')
local errors = require('errors')
local vshard = require('vshard')

local call = require('elect.common.call')
local registry = require('elect.common.registry')

require('elect.common.checkers')

local InsertError = errors.new_class('Insert',  {capture_stack = false})

local insert = {}

local INSERT_FUNC_NAME = '__insert'

local function call_insert_on_storage(space_name, obj)
    checks('string', 'table')

    local space = box.space[space_name]
    if space == nil then
        return nil, InsertError:new("Space %s doesn't exists", space_name)
    end

    local tuple, err = space:frommap(obj)
    if tuple == nil then
        return nil, InsertError:new("Object specified in wrong format: %s", err)
    end

    return space:insert(tuple):tomap({names_only = true})
end

function insert.init()
    registry.add({
        [INSERT_FUNC_NAME] = call_insert_on_storage,
    })
end

--- Inserts tuple to the specifed space
--
-- @function call
--
-- @param string space_name
--  A space name
--
-- @param table key_parts
--  Primary key fields' names array
--
-- @param table obj
--  Tuple object (according to space format)
--
-- @tparam ?number opts.timeout
--  Function call timeout
--
-- @return[1] object
-- @treturn[2] nil
-- @treturn[2] table Error description
--
function insert.call(space_name, key_parts, obj, opts)
    checks('string', 'strings_array', 'table', {
        timeout = '?number',
    })

    opts = opts or {}

    local key = {}
    for _, key_part in ipairs(key_parts) do
        table.insert(key, obj[key_part])
    end

    local bucket_id = vshard.router.bucket_id(key)
    local replicaset, err = vshard.router.route(bucket_id)
    if replicaset == nil then
        return nil, InsertError:new("Failed to get replicaset for bucket_id %s: %s", bucket_id, err.err)
    end

    obj = table.copy(obj)
    obj.bucket_id = bucket_id

    local results, err = call.rw({
        func_name = INSERT_FUNC_NAME,
        func_args = {space_name, obj},
        replicasets = {replicaset},
        timeout = opts.timeout,
    })

    if err ~= nil then
        return nil, InsertError:new("Failed to insert: %s", err)
    end

    return results[replicaset.uuid]
end

return insert
