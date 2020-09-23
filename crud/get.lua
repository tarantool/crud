local checks = require('checks')
local errors = require('errors')
local vshard = require('vshard')

local call = require('crud.common.call')
local registry = require('crud.common.registry')
local utils = require('crud.common.utils')

local GetError = errors.new_class('Get',  {capture_stack = false})

local get = {}

local GET_FUNC_NAME = '__get'

local function call_get_on_storage(space_name, key)
    checks('string', '?')

    local space = box.space[space_name]
    if space == nil then
        return nil, GetError:new("Space %q doesn't exists", space_name)
    end

    local tuple = space:get(key)
    return tuple
end

function get.init()
    registry.add({
        [GET_FUNC_NAME] = call_get_on_storage,
    })
end

--- Get tuple from the specifed space by key
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
-- @tparam ?tuples_tomap opts.tuples_tomap
--  defines type of returned result as map or tuple, default true
--
-- @return[1] object / tuple
-- @treturn[2] nil
-- @treturn[2] table Error description
--
function get.call(space_name, key, opts)
    checks('string', '?', {
        timeout = '?number',
        tuples_tomap = '?boolean',
    })

    opts = opts or {}
    if opts.tuples_tomap == nil then
        opts.tuples_tomap = true
    end

    local space = utils.get_space(space_name, vshard.router.routeall())
    if space == nil then
        return nil, GetError:new("Space %q doesn't exists", space_name)
    end
    local space_format = space:format()

    -- compute default buckect_id
    if box.tuple.is(key) then
        key = key:totable()
    end

    local bucket_id = vshard.router.bucket_id_strcrc32(key)
    local replicaset, err = vshard.router.route(bucket_id)
    if replicaset == nil then
        return nil, GetError:new("Failed to get replicaset for bucket_id %s: %s", bucket_id, err.err)
    end

    local results, err = call.rw(GET_FUNC_NAME, {space_name, key}, {
        replicasets = {replicaset},
        timeout = opts.timeout,
    })

    if err ~= nil then
        return nil, GetError:new("Failed to get: %s", err)
    end

    local tuple = results[replicaset.uuid]
    if opts.tuples_tomap == false then
        return tuple
    end

    local object, err = utils.unflatten(tuple, space_format)
    if err ~= nil then
        return nil, GetError:new("Received tuple that doesn't match space format: %s", err)
    end

    return object
end

return get
