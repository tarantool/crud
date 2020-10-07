local checks = require('checks')
local errors = require('errors')
local vshard = require('vshard')

local call = require('crud.common.call')
local registry = require('crud.common.registry')
local utils = require('crud.common.utils')
local dev_checks = require('crud.common.dev_checks')

local InsertError = errors.new_class('Insert',  {capture_stack = false})

local insert = {}

local INSERT_FUNC_NAME = '__insert'

local function call_insert_on_storage(space_name, tuple)
    dev_checks('string', 'table')

    local space = box.space[space_name]
    if space == nil then
        return nil, InsertError:new("Space %q doesn't exist", space_name)
    end

    return space:insert(tuple)
end

function insert.init()
    registry.add({
        [INSERT_FUNC_NAME] = call_insert_on_storage,
    })
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
-- @return[1] tuple
-- @treturn[2] nil
-- @treturn[2] table Error description
--
function insert.tuple(space_name, tuple, opts)
    checks('string', 'table', {
        timeout = '?number',
    })

    opts = opts or {}

    local space = utils.get_space(space_name, vshard.router.routeall())
    if space == nil then
        return nil, InsertError:new("Space %q doesn't exist", space_name)
    end
    local space_format = space:format()

    local key = utils.extract_key(tuple, space.index[0].parts)
    local bucket_id = vshard.router.bucket_id_strcrc32(key)
    local replicaset, err = vshard.router.route(bucket_id)
    if replicaset == nil then
        return nil, InsertError:new("Failed to get replicaset for bucket_id %s: %s", bucket_id, err.err)
    end

    local bucket_id_fieldno, err = utils.get_bucket_id_fieldno(space)
    if err ~= nil then
        return nil, err
    end

    if tuple[bucket_id_fieldno] ~= nil then
        return nil, InsertError:new("Unexpected value (%s) at field %s (bucket_id)",
                tuple[bucket_id_fieldno], bucket_id_fieldno)
    end

    tuple[bucket_id_fieldno] = bucket_id
    local results, err = call.rw(INSERT_FUNC_NAME, {space_name, tuple}, {
        replicasets = {replicaset},
        timeout = opts.timeout,
    })

    if err ~= nil then
        return nil, InsertError:new("Failed to insert: %s", err)
    end

    local tuple = results[replicaset.uuid]
    return {
        metadata = table.copy(space_format),
        rows = {tuple},
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
-- @tparam ?number opts.timeout
--  Function call timeout
--
-- @return[1] object
-- @treturn[2] nil
-- @treturn[2] table Error description
--
function insert.object(space_name, obj, opts)
    checks('string', 'table', {
        timeout = '?number',
    })

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
