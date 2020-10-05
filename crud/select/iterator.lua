local errors = require('errors')
local fiber = require('fiber')

local dev_checks = require('crud.common.dev_checks')
local utils = require('crud.common.utils')

local UpdateTuplesError = errors.new('UpdateTuplesError')
local GetTupleError = errors.new('GetTupleError')

local Heap = require('crud.common.heap')

local Iterator = {}
Iterator.__index = Iterator

function Iterator.new(opts)
    dev_checks({
        space_name = 'string',
        space_format = 'table',
        comparator = 'function',
        iteration_func = 'function',

        plan = 'table',

        batch_size = 'number',
        replicasets = 'table',

        timeout = '?number',
    })

    local iter = {
        space_name = opts.space_name,
        space_format = opts.space_format,
        iteration_func = opts.iteration_func,

        plan = opts.plan,
        reversed = opts.plan.reversed,

        timeout = opts.timeout,

        replicasets = table.copy(opts.replicasets),
        replicasets_count = utils.table_count(opts.replicasets),
        empty_replicasets = {},
        empty_replicasets_count = 0,

        batch_size = opts.batch_size,

        tuples_by_replicasets = {},
        next_tuple_indexes = {},

        heap = Heap.new({ comparator = opts.comparator }),
        tuples_count = 0,

        update_tuples_channel = fiber.channel(1),
        wait_for_update = false,
    }

    setmetatable(iter, Iterator)

    iter:_update_replicasets_tuples(iter.plan.after_tuple)

    return iter
end

function Iterator:has_next()
    if self.heap:size() == 0 and self.empty_replicasets_count >= self.replicasets_count then
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

    local results_map, err = iter.iteration_func(iter.space_name, iter.plan, {
        after_tuple = after_tuple,
        replicasets = replicasets,
        timeout = iter.timeout,
        limit = limit_per_storage_call,
    })
    if err ~= nil then
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
            iter.heap:add(next_tuple, {
                replicaset_uuid = replicaset_uuid
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

            self.heap:add(next_tuple, {
                replicaset_uuid = last_tuple_replicaset_uuid
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
