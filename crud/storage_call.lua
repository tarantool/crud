--- Calls persistent stored functions on vshard storages.
--
-- The module routes calls by bucket id or by a space primary key. Batch calls
-- are grouped by vshard and sent to affected replica sets in parallel.
--
-- @module crud.storage_call

local dev_checks = require('crud.common.dev_checks')
local router = require('crud.storage_call.router')
local storage = require('crud.storage_call.storage')

local storage_call = {
    storage_api = storage.storage_api,
}

--- Call several persistent stored functions.
--
-- Each call must contain `func_name`, optional `args`, and exactly one route:
-- `bucket_id` or the `space_name` and `key` pair. Calls sent to the same
-- replicaset are executed sequentially, while different replicasets are
-- contacted in parallel.
--
-- @function call_many
--
-- @tab calls Ordered array of call descriptions.
-- @tab[opt] opts
-- @number[opt] opts.timeout Common timeout for routing, sending and waiting.
-- @param[opt] opts.vshard_router Cartridge vshard group name or vshard router
--  instance.
--
-- @treturn[1] table Result with an ordered `results` array. Every item has
--  either `returns` or `error`.
-- @treturn[1] nil
-- @treturn[2] nil
-- @treturn[2] table Error that prevents processing the whole request.
function storage_call.call_many(calls, opts)
    dev_checks('table', {
        timeout = '?number',
        vshard_router = '?string|table',
    })

    return router.call_many(calls, opts or {})
end

--- Call a persistent stored function.
--
-- @function call
--
-- @string func_name Exact name of a persistent function in `box.func`.
-- @tab[opt] args Function arguments.
-- @tab[opt] opts
-- @number[opt] opts.bucket_id Explicit bucket id.
-- @string[opt] opts.space_name Space used to calculate a bucket id.
-- @param[opt] opts.key Primary key used to calculate a bucket id.
-- @number[opt] opts.timeout Common operation timeout.
-- @param[opt] opts.vshard_router Cartridge vshard group name or vshard router
--  instance.
--
-- @treturn[1] table Result with a `returns` array.
-- @treturn[1] nil
-- @treturn[2] nil
-- @treturn[2] table Call error.
function storage_call.call(func_name, args, opts)
    dev_checks('string', '?table', {
        bucket_id = '?number',
        space_name = '?string',
        key = '?',
        timeout = '?number',
        vshard_router = '?string|table',
    })

    return router.call(func_name, args or {}, opts or {})
end

return storage_call
