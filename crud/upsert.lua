local checks = require('checks')
local errors = require('errors')
local vshard = require('vshard')

local call = require('crud.common.call')
local registry = require('crud.common.registry')
local utils = require('crud.common.utils')

require('crud.common.checkers')

local UpsertError = errors.new_class('UpsertError',  { capture_stack = false})

local upsert = {}

local UPSERT_FUNC_NAME = '__upsert'

local function call_upsert_on_storage(space_name, tuple, operations)
    checks('string', 'table', 'update_operations')

    local space = box.space[space_name]
    if space == nil then
        return nil, UpsertError:new("Space %q doesn't exists", space_name)
    end

    return space:upsert(tuple, operations)
end

function upsert.init()
    registry.add({
        [UPSERT_FUNC_NAME] = call_upsert_on_storage,
    })
end

--- Update or insert a tuple in the specified space
--
-- @function call
--
-- @param string space_name
--  A space name
--
-- @param table obj
--  Tuple object (according to space format)
--
-- @param table user_operations
--  user_operations to be performed.
--  See `space_object:update` operations in Tarantool doc
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
function upsert.call(space_name, obj, user_operations, opts)
    checks('string', '?', 'update_operations', {
        timeout = '?number',
        tuples_tomap = '?boolean',
    })

    opts = opts or {}
    if opts.tuples_tomap == nil then
        opts.tuples_tomap = true
    end

    local space = utils.get_space(space_name, vshard.router.routeall())
    if space == nil then
        return nil, UpsertError:new("Space %q doesn't exists", space_name)
    end
    local space_format = space:format()

    local operations, err = utils.convert_operations(user_operations, space_format)
    if err ~= nil then
        return nil, UpsertError:new("Wrong operations are specified: %s", err)
    end

    -- convert input object to tuple if it need
    local tuple, err = nil, nil
    if opts.tuples_tomap == false then
        tuple = obj
    else
        tuple, err = utils.flatten(obj, space_format)
        if err ~= nil then
            return nil, UpsertError:new("Object is specified in bad format: %s", err)
        end
    end

    -- compute default buckect_id
    local key = utils.extract_key(tuple, space.index[0].parts)
    local bucket_id = vshard.router.bucket_id_strcrc32(key)
    local replicaset, err = vshard.router.route(bucket_id)
    if replicaset == nil then
        return nil, UpsertError:new("Failed to get replicaset for bucket_id %s: %s", bucket_id, err.err)
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
            return nil, UpsertError:new("Object is specified in bad format: %s", err)
        end
    end

    local results, err = call.rw(UPSERT_FUNC_NAME, {space_name, tuple, operations}, {
        replicasets = {replicaset},
        timeout = opts.timeout,
    })

    if err ~= nil then
        return nil, UpsertError:new("Failed to upsert: %s", err)
    end

    --upsert always return nil
    return results[replicaset.uuid]
end

return upsert
