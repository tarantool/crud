local checks = require('checks')
local errors = require('errors')
local vshard = require('vshard')

local call = require('crud.common.call')
local registry = require('crud.common.registry')
local utils = require('crud.common.utils')

require('crud.common.checkers')

local ReplaceError = errors.new_class('Replace', { capture_stack = false })

local replace = {}

local REPLACE_FUNC_NAME = '__replace'

local function call_replace_on_storage(space_name, tuple)
    checks('string', 'table')

    local space = box.space[space_name]
    if space == nil then
        return nil, ReplaceError:new("Space %q doesn't exists", space_name)
    end

    return space:replace(tuple)
end

function replace.init()
    registry.add({
        [REPLACE_FUNC_NAME] = call_replace_on_storage,
    })
end

--- Insert or replace a tuple in the specifed space
--
-- @function call
--
-- @param string space_name
--  A space name
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
function replace.call(space_name, obj, opts)
    checks('string', 'table', {
        timeout = '?number',
    })

    opts = opts or {}

    local space = utils.get_space(space_name, vshard.router.routeall())
    if space == nil then
        return nil, ReplaceError:new("Space %q doesn't exists", space_name)
    end

    local space_format = space:format()
    -- compute default buckect_id
    local tuple, err = utils.flatten(obj, space_format)
    if err ~= nil then
        return nil, ReplaceError:new("Object is specified in bad format: %s", err)
    end

    local key = utils.extract_key(tuple, space.index[0].parts)

    local bucket_id = vshard.router.bucket_id_strcrc32(key)
    local replicaset, err = vshard.router.route(bucket_id)
    if replicaset == nil then
        return nil, ReplaceError:new("Failed to get replicaset for bucket_id %s: %s", bucket_id, err.err)
    end

    local tuple, err = utils.flatten(obj, space_format, bucket_id)
    if err ~= nil then
        return nil, ReplaceError:new("Object is specified in bad format: %s", err)
    end

    local results, err = call.rw(REPLACE_FUNC_NAME, {space_name, tuple}, {
        replicasets = {replicaset},
        timeout = opts.timeout,
    })

    if err ~= nil then
        return nil, ReplaceError:new("Failed to replace: %s", err)
    end

    local tuple = results[replicaset.uuid]
    local object, err = utils.unflatten(tuple, space:format())
    if err ~= nil then
        return nil, ReplaceError:new("Received tuple that doesn't match space format: %s", err)
    end

    return object
end

return replace
