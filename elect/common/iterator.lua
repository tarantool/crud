local checks = require('checks')
local uuid = require('uuid')
local utils = require('elect.common.utils')

require('elect.common.checkers')

local Heap = require('elect.heap')

local Iterator = {}
Iterator.__index = Iterator

function Iterator.new(initial_state, opts)
    checks('table', {
        limit = '?number',
        batch_size = '?number',
        replicasets = 'table',
        iteration_func = 'function',
        timeout = '?number',
        key_parts = 'strings_array',
    })

    local obj = {
        operation_id = uuid.str(),
        limit = opts.limit,
        timeout = opts.timeout,

        replicasets = table.copy(opts.replicasets),
        replicasets_count = utils.table_count(opts.replicasets),
        empty_replicasets_count = 0,
        obj_by_replicasets = {},

        heap = Heap.new({ key_parts = opts.key_parts }),
        objects_count = 0,

        iteration_func = opts.iteration_func,
        state = initial_state,
    }

    obj.batch_size = opts.batch_size
    if obj.batch_size == nil or (obj.limit ~= nil and obj.limit < obj.batch_size) then
        obj.batch_size = obj.limit
    end

    setmetatable(obj, Iterator)

    obj:_update_replicasets_objects(obj.replicasets)

    return obj
end

function Iterator:has_next()
    if self.heap:size() == 0 and self.empty_replicasets_count >= self.replicasets_count then
        return false
    end

    if self.limit ~= nil and self.objects_count >= self.limit then
        return false
    end

    return true
end

function Iterator:_update_replicasets_objects(replicasets)
    local results_map, err = self.iteration_func(self.operation_id, self.state, {
        timeout = self.timeout,
        batch_size = self.batch_size,
        replicasets = replicasets,
    })
    if err ~= nil then return nil, err end

    for replicaset_uuid, replicaset_result in pairs(results_map) do
        if #replicaset_result.objects > 0 then
            self.heap:add(table.remove(replicaset_result.objects, 1), {
                replicaset_uuid = replicaset_uuid
            })
        end

        self.obj_by_replicasets[replicaset_uuid] = replicaset_result.objects

        if #replicaset_result.objects == 0 or replicaset_result.last_batch then
            self.replicasets[replicaset_uuid] = nil
            self.empty_replicasets_count = self.empty_replicasets_count + 1
        end
    end

    return true
end

function Iterator:get()
    local node = self.heap:pop()

    if node == nil then return nil end

    self.objects_count = self.objects_count + 1
    local last_obj_replicaset_uuid = node.meta.replicaset_uuid

    if #self.obj_by_replicasets[last_obj_replicaset_uuid] > 0 then
        self.heap:add(table.remove(self.obj_by_replicasets[last_obj_replicaset_uuid], 1), {
            replicaset_uuid = last_obj_replicaset_uuid
        })
    elseif self.replicasets[last_obj_replicaset_uuid] ~= nil then
        self:_update_replicasets_objects({[last_obj_replicaset_uuid] = self.replicasets[last_obj_replicaset_uuid]})
    end

    return node.obj
end

return Iterator
