local errors = require('errors')

local dev_checks = require('crud.common.dev_checks')
local sharding = require('crud.common.sharding')

local BaseIterator = require('crud.common.map_call_cases.base_iter')

local SplitTuplesError = errors.new_class('SplitTuplesError')

local BatchUpsertIterator = {}
-- inheritance from BaseIterator
setmetatable(BatchUpsertIterator, {__index = BaseIterator})

--- Create new batch upsert iterator for map call
--
-- @function new
--
-- @tparam[opt] table opts
-- Options of BatchUpsertIterator:new
-- @tparam[opt] table opts.tuples
-- Tuples to be upserted
-- @tparam[opt] table opts.space
-- Space to be upserted into
-- @tparam[opt] table opts.operations
-- Operations to be performed on tuples
-- @tparam[opt] table opts.execute_on_storage_opts
-- Additional opts for call on storage
--
-- @return[1] table iterator
-- @treturn[2] nil
-- @treturn[2] table of tables Error description
function BatchUpsertIterator:new(opts)
    dev_checks('table', {
        tuples = 'table',
        space = 'table',
        operations = 'table',
        execute_on_storage_opts = 'table',
        vshard_router = 'table',
    })

    local sharding_data, err = sharding.split_tuples_by_replicaset(
        opts.vshard_router,
        opts.tuples,
        opts.space,
        {operations = opts.operations})

    if err ~= nil then
        return nil, SplitTuplesError:new("Failed to split tuples by replicaset: %s", err.err)
    end

    local next_index, next_batch = next(sharding_data.batches)

    local execute_on_storage_opts = opts.execute_on_storage_opts
    execute_on_storage_opts.sharding_func_hash = sharding_data.sharding_func_hash
    execute_on_storage_opts.sharding_key_hash = sharding_data.sharding_key_hash
    execute_on_storage_opts.skip_sharding_hash_check = sharding_data.skip_sharding_hash_check

    local iter = {
        space_name = opts.space.name,
        opts = execute_on_storage_opts,
        batches_by_replicasets = sharding_data.batches,
        next_index = next_index,
        next_batch = next_batch,
    }

    setmetatable(iter, self)
    self.__index = self

    return iter
end

--- Get function arguments and next replicaset
--
-- @function get
--
-- @return[1] table func_args
-- @return[2] table replicaset
-- @return[3] string replicaset_id
function BatchUpsertIterator:get()
    local replicaset_id = self.next_index
    local replicaset = self.next_batch.replicaset
    local func_args = {
        self.space_name,
        self.next_batch.tuples,
        self.next_batch.operations,
        self.opts,
    }

    self.next_index, self.next_batch = next(self.batches_by_replicasets, self.next_index)

    return func_args, replicaset, replicaset_id
end

return BatchUpsertIterator
