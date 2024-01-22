local checks = require('checks')
local errors = require('errors')

local dev_checks = require('crud.common.dev_checks')
local call = require('crud.common.call')
local utils = require('crud.common.utils')

local TruncateError = errors.new_class('TruncateError', {capture_stack = false})

local truncate = {}

local TRUNCATE_FUNC_NAME = 'truncate_on_storage'
local CRUD_TRUNCATE_FUNC_NAME = utils.get_storage_call(TRUNCATE_FUNC_NAME)

local function truncate_on_storage(space_name)
    dev_checks('string')

    local space = box.space[space_name]
    if space == nil then
        return nil, TruncateError:new("Space %q doesn't exist", space_name)
    end

    return space:truncate()
end

truncate.storage_api = {[TRUNCATE_FUNC_NAME] = truncate_on_storage}

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
-- @tparam ?string|table opts.vshard_router
--  Cartridge vshard group name or vshard router instance.
--  Set this parameter if your space is not a part of the
--  default vshard cluster.
--
-- @return[1] boolean true
-- @treturn[2] nil
-- @treturn[2] table Error description
--
function truncate.call(space_name, opts)
    checks('string', {
        timeout = '?number',
        vshard_router = '?string|table',
    })

    opts = opts or {}

    local vshard_router, err = utils.get_vshard_router_instance(opts.vshard_router)
    if err ~= nil then
        return nil, TruncateError:new(err)
    end

    local replicasets, err = vshard_router:routeall()
    if err ~= nil then
        return nil, TruncateError:new("Failed to get router replicasets: %s", err)
    end

    local _, err = call.map(vshard_router, CRUD_TRUNCATE_FUNC_NAME, {space_name}, {
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
