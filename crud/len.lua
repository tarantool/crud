local checks = require('checks')
local errors = require('errors')
local vshard = require('vshard')

local call = require('crud.common.call')
local utils = require('crud.common.utils')
local dev_checks = require('crud.common.dev_checks')
local schema = require('crud.common.schema')

local LenError = errors.new_class('Len',  {capture_stack = false})

local len = {}

local LEN_FUNC_NAME = '_crud.len_on_storage'

local function len_on_storage(space_name)
    dev_checks('string')

    local space = box.space[space_name]
    if space == nil then
        return nil, LenError:new("Space %q doesn't exist", space_name)
    end

    -- add_space_schema_hash is false because
    -- reloading space format on router can't avoid len error on storage
    return schema.wrap_box_space_func_result(space, 'len', {}, {
        add_space_schema_hash = false,
    })
end

function len.init()
    _G._crud.len_on_storage = len_on_storage
end

-- returns result, err, need_reload
-- need_reload indicates if reloading schema could help
-- see crud.common.schema.wrap_func_reload()
local function call_len_on_router(space_name, opts)
    dev_checks('string', {
        timeout = '?number',
        prefer_replica = '?boolean',
        balance = '?boolean',
        mode = '?string',
    })

    opts = opts or {}

    local space = utils.get_space(space_name, vshard.router.routeall())
    if space == nil then
        return nil, LenError:new("Space %q doesn't exist", space_name), true
    end

    local results, err = call.map(LEN_FUNC_NAME, {space_name}, {
        timeout = opts.timeout,
        mode = opts.mode or 'read',
        prefer_replica = opts.prefer_replica,
        balance = opts.balance,
    })

    if err ~= nil then
        return nil, LenError:new("Failed to call len on storage-side: %s", err)
    end

    local total_len = 0
    for _, replicaset_results in pairs(results) do
        if replicaset_results[1] ~= nil then
            total_len = total_len + replicaset_results[1].res
        end
    end

    return total_len
end

--- Len of the specified space
--
-- @function call
--
-- @param string space_name
--  A space name
--
-- @tparam ?number opts.timeout
--  Function call timeout
--
-- @tparam ?boolean opts.prefer_replica
--  Call on replica if it's possible
--
-- @tparam ?boolean opts.balance
--  Use replica according to round-robin load balancing
--
-- @return[1] object
-- @treturn[2] nil
-- @treturn[2] table Error description
--
function len.call(space_name, opts)
    checks('string', {
        timeout = '?number',
        prefer_replica = '?boolean',
        balance = '?boolean',
        mode = '?string',
    })

    return schema.wrap_func_reload(call_len_on_router, space_name, opts)
end

return len