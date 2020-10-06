local checks = require('checks')
local errors = require('errors')
local vshard = require('vshard')

local call = require('crud.common.call')
local registry = require('crud.common.registry')
local utils = require('crud.common.utils')
local dev_checks = require('crud.common.dev_checks')

local UpsertError = errors.new_class('UpsertError',  { capture_stack = false})

local upsert = {}

local UPSERT_FUNC_NAME = '__upsert'

local function call_upsert_on_storage(space_name, tuple, operations)
    dev_checks('string', 'table', 'table')

    local space = box.space[space_name]
    if space == nil then
        return nil, UpsertError:new("Space %q doesn't exist", space_name)
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
--  See `space:update` operations in Tarantool doc
--
-- @tparam ?number opts.timeout
--  Function call timeout
--
-- @return[1] object
-- @treturn[2] nil
-- @treturn[2] table Error description
--
function upsert.call(space_name, obj, user_operations, opts)
    checks('string', '?', 'table', {
        timeout = '?number',
    })

    opts = opts or {}

    local space = utils.get_space(space_name, vshard.router.routeall())
    if space == nil then
        return nil, UpsertError:new("Space %q doesn't exist", space_name)
    end

    local space_format = space:format()
    local operations, err = utils.convert_operations(user_operations, space_format)
    if err ~= nil then
        return nil, UpsertError:new("Wrong operations are specified: %s", err)
    end

    -- compute default bucket_id
    local tuple, err = utils.flatten(obj, space_format)
    if err ~= nil then
        return nil, UpsertError:new("Object is specified in bad format: %s", err)
    end

    local key = utils.extract_key(tuple, space.index[0].parts)

    local bucket_id = vshard.router.bucket_id_strcrc32(key)
    local replicaset, err = vshard.router.route(bucket_id)
    if replicaset == nil then
        return nil, UpsertError:new("Failed to get replicaset for bucket_id %s: %s", bucket_id, err.err)
    end

    local tuple, err = utils.flatten(obj, space_format, bucket_id)
    if err ~= nil then
        return nil, UpsertError:new("Object is specified in bad format: %s", err)
    end

    local _, err = call.rw(UPSERT_FUNC_NAME, {space_name, tuple, operations}, {
        replicasets = {replicaset},
        timeout = opts.timeout,
    })

    if err ~= nil then
        return nil, UpsertError:new("Failed to upsert: %s", err)
    end

    -- upsert always returns nil
    return {
        metadata = table.copy(space_format),
        rows = {},
    }
end

return upsert
