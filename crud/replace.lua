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
-- @tparam ?tuples_tomap opts.tuples_tomap
--  defines type of returned result and input object as map or tuple, default true
--
-- @return[1] object
-- @treturn[2] nil
-- @treturn[2] table Error description
--
function replace.call(space_name, obj, opts)
    checks('string', 'table', {
        timeout = '?number',
        tuples_tomap = '?boolean',
    })

    opts = opts or {}
    if opts.tuples_tomap == nil then
        opts.tuples_tomap = true
    end

    local space = utils.get_space(space_name, vshard.router.routeall())
    if space == nil then
        return nil, ReplaceError:new("Space %q doesn't exists", space_name)
    end
    local space_format = space:format()

    -- convert input object to tuple if it need
    local tuple, err = nil, nil
    if opts.tuples_tomap == false then
        tuple = obj
    else
        tuple, err = utils.flatten(obj, space_format)
        if err ~= nil then
            return nil, InsertError:new("Object is specified in bad format: %s", err)
        end
    end

    -- compute default buckect_id
    local key = utils.extract_key(tuple, space.index[0].parts)
    local bucket_id = vshard.router.bucket_id_strcrc32(key)
    local replicaset, err = vshard.router.route(bucket_id)
    if replicaset == nil then
        return nil, ReplaceError:new("Failed to get replicaset for bucket_id %s: %s", bucket_id, err.err)
    end

    -- set buckect_id for tuple
    local tuple, err = nil, nil
    if opts.tuples_tomap == false then
        local pos, err = utils.get_bucket_id_pos(space_format)
        if err ~= nil then
            return nil, InsertError:new("%s, %s", err, space_name)
        end
        tuple = obj
        tuple[pos] = bucket_id
    else
        tuple, err = utils.flatten(obj, space_format, bucket_id)
        if err ~= nil then
            return nil, ReplaceError:new("Object is specified in bad format: %s", err)
        end
    end

    local results, err = call.rw(REPLACE_FUNC_NAME, {space_name, tuple}, {
        replicasets = {replicaset},
        timeout = opts.timeout,
    })

    if err ~= nil then
        return nil, ReplaceError:new("Failed to replace: %s", err)
    end

    local tuple = results[replicaset.uuid]

    if opts.tuples_tomap == false then
        return tuple
    end

    local object, err = utils.unflatten(tuple, space_format)
    if err ~= nil then
        return nil, ReplaceError:new("Received tuple that doesn't match space format: %s", err)
    end

    return object
end

return replace
