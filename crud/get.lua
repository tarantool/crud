local checks = require('checks')
local errors = require('errors')
local vshard = require('vshard')

local call = require('crud.common.call')
local utils = require('crud.common.utils')
local sharding = require('crud.common.sharding')
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
-- @tparam ?number opts.bucket_id
--  Bucket ID
--  (by default, it's vshard.router.bucket_id_strcrc32 of primary key)
--
-- @return[1] object
-- @treturn[2] nil
-- @treturn[2] table Error description
--
function get.call(space_name, key, opts)
    checks('string', '?', {
        timeout = '?number',
        bucket_id = '?number|cdata',
    })

    opts = opts or {}

    local space = utils.get_space(space_name, vshard.router.routeall())
    if space == nil then
        return nil, GetError:new("Space %q doesn't exist", space_name)
    end

    if box.tuple.is(key) then
        key = key:totable()
    end

    local bucket_id = sharding.key_get_bucket_id(key, opts.bucket_id)
    -- We don't use callro() here, because if the replication is
    -- async, there could be a lag between master and replica, so a
    -- connector which sequentially calls put() and then get() may get
    -- a stale result.
    local result, err = call.rw_single(
        bucket_id, GET_FUNC_NAME,
        {space_name, key}, {timeout=opts.timeout})

    if err ~= nil then
        return nil, GetError:new("Failed to get: %s", err)
    end

    -- protect against box.NULL
    if result == nil then
       result = nil
    end

    return {
       metadata = table.copy(space:format()),
       rows = {result},
    }
end

return get
