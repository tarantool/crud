local errors = require('errors')
local fiber = require('fiber')

local dev_checks = require('crud.common.dev_checks')
local sharding = require('crud.common.sharding')
local utils = require('crud.common.utils')

local UpdateTuplesError = errors.new_class('UpdateTuplesError')
local GetTupleError = errors.new_class('GetTupleError')

local Heap = require('vshard.heap')

local Iterator = {}
Iterator.__index = Iterator

function Iterator.new(opts)
    dev_checks({
        space_name = 'string',
        space = 'table',
        netbox_schema_version = '?number',
        comparator = 'function',
        iteration_func = 'function',

        plan = 'table',
        field_names = '?table',

        batch_size = 'number',
        replicasets = 'table',

        call_opts = 'table',
        sharding_hash = 'table',

        vshard_router = 'table',
        yield_every = '?number',
    })

    local iter = {
        space_name = opts.space_name,
        space = opts.space,
        netbox_schema_version = opts.netbox_schema_version,
        storages_info = {},
        iteration_func = opts.iteration_func,

        plan = opts.plan,
        field_names = opts.field_names,

        call_opts = opts.call_opts,

        replicasets = table.copy(opts.replicasets),
        replicasets_count = utils.table_count(opts.replicasets),
        empty_replicasets = {},
        empty_replicasets_count = 0,

        batch_size = opts.batch_size,

        tuples_by_replicasets = {},
        next_tuple_indexes = {},

        heap = Heap.new(opts.comparator),
        tuples_count = 0,

        update_tuples_channel = fiber.channel(1),
        wait_for_update = false,

        sharding_hash = opts.sharding_hash,

        vshard_router = opts.vshard_router,
        yield_every = opts.yield_every,
    }

    setmetatable(iter, Iterator)

    iter:_update_replicasets_tuples(iter.plan.after_tuple)

    return iter
end

function Iterator:has_next()
    if self.heap:count() == 0 and self.empty_replicasets_count >= self.replicasets_count then
        return false
    end

    if self.plan.total_tuples_count ~= nil and self.tuples_count >= self.plan.total_tuples_count then
        return false
    end

    return true
end

local function get_next_replicaset_tuple(iter, replicaset_uuid)
    local replicaset_tuples = iter.tuples_by_replicasets[replicaset_uuid]
    local next_tuple = replicaset_tuples[iter.next_tuple_indexes[replicaset_uuid]]

    iter.next_tuple_indexes[replicaset_uuid] = iter.next_tuple_indexes[replicaset_uuid] + 1

    return next_tuple
end

local function update_replicasets_tuples(iter, after_tuple, replicaset_uuid)
    local replicasets = {}
    if replicaset_uuid == nil then
        replicasets = iter.replicasets
    else
        replicasets[replicaset_uuid] = iter.replicasets[replicaset_uuid]
    end

    local limit_per_storage_call = iter.batch_size
    if iter.total_tuples_count ~= nil then
        limit_per_storage_call = math.min(iter.batch_size, iter.total_tuples_count - iter.tuples_count)
    end

    local results_map, err, storages_info = iter.iteration_func(iter.space_name, iter.plan, {
        after_tuple = after_tuple,
        replicasets = replicasets,
        limit = limit_per_storage_call,
        field_names = iter.field_names,
        call_opts = iter.call_opts,
        sharding_hash = iter.sharding_hash,
        vshard_router = iter.vshard_router,
        yield_every = iter.yield_every,
    })
    iter.storages_info = storages_info
    if err ~= nil then
        if sharding.result_needs_sharding_reload(err) then
            return false, err
        end

        return false, UpdateTuplesError:new('Failed to select tuples from storages: %s', err)
    end

    for replicaset_uuid, tuples in pairs(results_map) do
        if #tuples == 0 or #tuples < limit_per_storage_call then
            iter.empty_replicasets[replicaset_uuid] = true
            iter.empty_replicasets_count = iter.empty_replicasets_count + 1
        end

        iter.tuples_by_replicasets[replicaset_uuid] = tuples
        iter.next_tuple_indexes[replicaset_uuid] = 1

        local next_tuple = get_next_replicaset_tuple(iter, replicaset_uuid)

        if next_tuple ~= nil then
            iter.heap:push({
                obj = next_tuple,
                meta = {replicaset_uuid = replicaset_uuid},
            })
        end
    end

    return true
end

function Iterator:_update_replicasets_tuples(after_tuple, replicaset_uuid)
    self.wait_for_update = true

    local function _update_replicasets_tuples(channel, iter, after_tuple, replicaset_uuid)
        local ok, err = update_replicasets_tuples(iter, after_tuple, replicaset_uuid)
        channel:put({
            ok = ok,
            err = err,
        })
    end

    fiber.create(_update_replicasets_tuples, self.update_tuples_channel, self, after_tuple, replicaset_uuid)
end

function Iterator:get()

    if self.wait_for_update then
        -- wait for _update_replicasets_tuples

        self.wait_for_update = false

        local res = self.update_tuples_channel:get()

        if res == nil then
            if self.update_tuples_channel:is_closed() then
                return nil, GetTupleError:new("Channel is closed")
            end

            return nil, GetTupleError:new("Timeout was reached")
        end

        if not res.ok then
            if sharding.result_needs_sharding_reload(res.err) then
                return false, res.err
            end

            return nil, GetTupleError:new("Failed to get tuples from storages: %s", res.err)
        end
    end

    local node = self.heap:pop()

    if node == nil then
        return nil
    end

    local tuple = node.obj
    local last_tuple_replicaset_uuid = node.meta.replicaset_uuid

    self.tuples_count = self.tuples_count + 1

    if self.plan.total_tuples_count == nil or self.tuples_count < self.plan.total_tuples_count then
        local replicaset_tuples_count = #self.tuples_by_replicasets[last_tuple_replicaset_uuid]
        local next_tuple_index = self.next_tuple_indexes[last_tuple_replicaset_uuid]

        if next_tuple_index <= replicaset_tuples_count then
            local next_tuple = get_next_replicaset_tuple(self, last_tuple_replicaset_uuid)

            self.heap:push({
                obj = next_tuple,
                meta = {replicaset_uuid = last_tuple_replicaset_uuid},
            })

        elseif not self.empty_replicasets[last_tuple_replicaset_uuid] then
            self:_update_replicasets_tuples(
                tuple,
                last_tuple_replicaset_uuid
            )
        end
    end

    return tuple
end

return Iterator
