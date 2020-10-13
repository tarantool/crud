local checks = require('checks')
local errors = require('errors')
local vshard = require('vshard')

local call = require('crud.common.call')
local utils = require('crud.common.utils')
local dev_checks = require('crud.common.dev_checks')

local GetError = errors.new_class('Get',  {capture_stack = false})

local get = {}

local GET_FUNC_NAME = '_crud.get_on_storage'

local function get_on_storage(space_name, key)
    dev_checks('string', '?')

    local space = box.space[space_name]
    if space == nil then
        return nil, GetError:new("Space %q doesn't exist", space_name)
    end

    local tuple = space:get(key)
    return tuple
end

function get.init()
   _G._crud.get_on_storage = get_on_storage
end

--- Get tuple from the specified space by key
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
function get.call(space_name, key, opts)
    checks('string', '?', {
        timeout = '?number',
    })

    opts = opts or {}

    local space = utils.get_space(space_name, vshard.router.routeall())
    if space == nil then
        return nil, GetError:new("Space %q doesn't exist", space_name)
    end

    if box.tuple.is(key) then
        key = key:totable()
    end

    local bucket_id = vshard.router.bucket_id_strcrc32(key)
    local results, err = call.rw_single(
        bucket_id, GET_FUNC_NAME,
        {space_name, key}, {timeout=opts.timeout})

    if err ~= nil then
        return nil, GetError:new("Failed to get: %s", err)
    end

    if results == nil then
        return {
            metadata = table.copy(space:format()),
            rows = {},
        }
    else
        return {
           metadata = table.copy(space:format()),
           rows = {results},
        }
    end
end

return get
