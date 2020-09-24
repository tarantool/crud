local checks = require('checks')
local errors = require('errors')
local vshard = require('vshard')

local call = require('crud.common.call')
local registry = require('crud.common.registry')
local utils = require('crud.common.utils')

require('crud.common.checkers')

local InsertError = errors.new_class('Insert',  {capture_stack = false})

local insert = {}

local INSERT_FUNC_NAME = '__insert'

local function call_insert_on_storage(space_name, tuple)
    checks('string', 'table')

    local space = box.space[space_name]
    if space == nil then
        return nil, InsertError:new("Space %q doesn't exists", space_name)
    end

    return space:insert(tuple)
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
function insert.call(space_name, obj, opts)
    checks('string', 'table', {
        timeout = '?number',
    })

    opts = opts or {}

    local space = utils.get_space(space_name, vshard.router.routeall())
    if space == nil then
        return nil, InsertError:new("Space %q doesn't exists", space_name)
    end

    -- compute default buckect_id
    local tuple, err = utils.flatten(obj, space:format())
    if err ~= nil then
        return nil, InsertError:new("Object is specified in bad format: %s", err)
    end

    local key = utils.extract_key(tuple, space.index[0].parts)

    local bucket_id = vshard.router.bucket_id_strcrc32(key)
    local replicaset, err = vshard.router.route(bucket_id)
    if replicaset == nil then
        return nil, InsertError:new("Failed to get replicaset for bucket_id %s: %s", bucket_id, err.err)
    end

    local tuple, err = utils.flatten(obj, space:format(), bucket_id)
    if err ~= nil then
        return nil, InsertError:new("Object is specified in bad format: %s", err)
    end

    local results, err = call.rw(INSERT_FUNC_NAME, {space_name, tuple}, {
        replicasets = {replicaset},
        timeout = opts.timeout,
    })

    if err ~= nil then
        return nil, InsertError:new("Failed to insert: %s", err)
    end

    local tuple = results[replicaset.uuid]
    local object, err = utils.unflatten(tuple, space:format())
    if err ~= nil then
        return nil, InsertError:new("Received tuple that doesn't match space format: %s", err)
    end

    return object
end

return insert
