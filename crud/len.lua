local checks = require('checks')
local errors = require('errors')
local log = require('log')

local utils = require('crud.common.utils')
local dev_checks = require('crud.common.dev_checks')

local LenError = errors.new_class('LenError', {capture_stack = false})

local len = {}

local LEN_FUNC_NAME = '_crud.len_on_storage'

local function len_on_storage(space_name)
    dev_checks('string|number')

    return box.space[space_name]:len()
end

function len.init()
    _G._crud.len_on_storage = len_on_storage
end

--- Calculates the number of tuples in the space for memtx engine
--- Calculates the maximum approximate number of tuples in the space for vinyl engine
--
-- @function call
--
-- @param string|number space_name
--  A space name as well as numerical id
--
-- @tparam ?number opts.timeout
--  Function call timeout
--
-- @tparam ?string|table opts.vshard_router
--  Cartridge vshard group name or vshard router instance.
--  Set this parameter if your space is not a part of the
--  default vshard cluster.
--
-- @return[1] number
-- @treturn[2] nil
-- @treturn[2] table Error description
--
function len.call(space_name, opts)
    checks('string|number', {
        timeout = '?number',
        vshard_router = '?string|table',
    })

    opts = opts or {}

    if type(space_name) == 'number' then
        log.warn('Using space id in crud.len is deprecated and will be removed in future releases.' ..
                 'Please, use space name instead.')
    end

    local vshard_router, err = utils.get_vshard_router_instance(opts.vshard_router)
    if err ~= nil then
        return nil, LenError:new(err)
    end

    local space = utils.get_space(space_name, vshard_router:routeall())
    if space == nil then
        return nil, LenError:new("Space %q doesn't exist", space_name)
    end

    local results, err = vshard_router:map_callrw(LEN_FUNC_NAME, {space_name}, opts)

    if err ~= nil then
        return nil, LenError:new("Failed to call len on storage-side: %s", err)
    end

    local total_len = 0
    for _, replicaset_results in pairs(results) do
        if replicaset_results[1] ~= nil then
            total_len = total_len + replicaset_results[1]
        end
    end

    return total_len
end

return len
