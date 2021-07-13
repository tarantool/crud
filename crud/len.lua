local checks = require('checks')
local errors = require('errors')
local vshard = require('vshard')

local call = require('crud.common.call')
local dev_checks = require('crud.common.dev_checks')

local LenError = errors.new_class('LenError',  {capture_stack = false})

local len = {}

local LEN_FUNC_NAME = '_crud.len_on_storage'

local function len_on_storage(space_name)
    dev_checks('string')

    local space = box.space[space_name]
    if space == nil then
        return nil, LenError:new("Space %q doesn't exist", space_name)
    end

    return space:len()
end

function len.init()
    _G._crud.len_on_storage = len_on_storage
end

--- Calculates the number of tuples in the space for memtx engine
--- Calculates the maximum approximate number of tuples in the space for vinyl engine
--
-- @function call
--
-- @param string space_name
--  A space name
--
-- @tparam ?number opts.timeout
--  Function call timeout
--
-- @return[1] number
-- @treturn[2] nil
-- @treturn[2] table Error description
--
function len.call(space_name, opts)
    checks('string', {
        timeout = '?number',
    })

    opts = opts or {}

    local replicasets = vshard.router.routeall()
    local results, err = call.map(LEN_FUNC_NAME, {space_name}, {
        mode = 'write',
        replicasets = replicasets,
        timeout = opts.timeout,
    })

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
