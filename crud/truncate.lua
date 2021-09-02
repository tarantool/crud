local checks = require('checks')
local errors = require('errors')
local vshard = require('vshard')

local dev_checks = require('crud.common.dev_checks')
local call = require('crud.common.call')

local TruncateError = errors.new_class('TruncateError', {capture_stack = false})

local truncate = {}

local TRUNCATE_FUNC_NAME = '_crud.truncate_on_storage'

local function truncate_on_storage(space_name)
    dev_checks('string')

    local space = box.space[space_name]
    if space == nil then
        return nil, TruncateError:new("Space %q doesn't exist", space_name)
    end

    return space:truncate()
end

function truncate.init()
   _G._crud.truncate_on_storage = truncate_on_storage
end

--- Truncates specified space
--
-- @function call
--
-- @param string space_name
--  A space name
--
-- @tparam ?number opts.timeout
--  Function call timeout
--
-- @return[1] boolean true
-- @treturn[2] nil
-- @treturn[2] table Error description
--
function truncate.call(space_name, opts)
    checks('string', {
        timeout = '?number',
    })

    opts = opts or {}

    local replicasets = vshard.router.routeall()
    local _, err = call.map(TRUNCATE_FUNC_NAME, {space_name}, {
        mode = 'write',
        replicasets = replicasets,
        timeout = opts.timeout,
    })

    if err ~= nil then
        return nil, TruncateError:new("Failed to truncate: %s", err)
    end

    return true
end

return truncate
