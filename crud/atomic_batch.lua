local checks = require('checks')

local schema = require('crud.common.schema')
local sharding = require('crud.common.sharding')
local utils = require('crud.common.utils')

local common = require('crud.atomic_batch.common')
local router = require('crud.atomic_batch.router')
local storage = require('crud.atomic_batch.storage')

local atomic_batch = {}

local AtomicBatchExecutionError = common.AtomicBatchExecutionError

--- Execute a batch of CRUD operations atomically on a single replicaset.
--
-- @function call
--
-- @param table operations
--  Array of operations to execute in a single transaction.
--  Every operation is a table with fields:
--
--  - `type`: `'get'`, `'insert'`, `'replace'`, `'update'`, `'upsert'` or `'delete'`.
--  - `space`: target space name.
--  - `tuple` or `object`: required for `insert`, `replace`, `upsert`.
--  - `key`: required for `get`, `update`, `delete`.
--  - `operations`: required for `update` and `upsert`.
--
-- @tparam ?number opts.timeout
--  Function call timeout.
-- @tparam ?boolean opts.noreturn
--  Suppress returning successfully processed tuples.
-- @tparam ?table opts.fields    per-space field projection
--  Output field names by space, same format as `{[space_name] = {field1, field2, ...}}`.
--
-- @return[1] table
--  `{metadata = {[space_name] = format}, data = {op_results...}}`.
--  `data[i]` matches the i-th operation in `operations` when `opts.noreturn ~= true`.
-- @treturn[2] nil
-- @treturn[2] table Error
--
function atomic_batch.call(operations, opts)
    checks('table', {
        timeout = '?number',
        noreturn = '?boolean',
        fields = '?table',
    })

    opts = opts or {}

    local vshard_router, err = utils.get_vshard_router_instance()
    if err ~= nil then
        return nil, AtomicBatchExecutionError:new(err)
    end

    local unique_spaces = router.collect_unique_spaces(operations)
    local res, res_err = schema.wrap_func_reload(
        vshard_router,
        sharding.wrap_method_for_spaces,
        router.call_on_router,
        unique_spaces,
        operations,
        opts
    )

    return res, res_err
end

function atomic_batch.extract_space_names(operations)
    return router.collect_unique_spaces(operations)
end

atomic_batch.storage_api = storage.storage_api

return atomic_batch
