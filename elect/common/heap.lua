local checks = require('checks')

require('elect.common.checkers')

local Heap = {}
Heap.__index = Heap

function Heap.new(opts)
    checks({
        key_parts = 'strings_array',
    })

    local obj = {
        key_parts = opts.key_parts,
        nodes = {},
    }

    setmetatable(obj, Heap)
    return obj
end

local function get_parent_idx(idx)
    if idx == 1 then
        return nil
    end

    return math.floor(idx / 2)
end

local function get_children_idxs(idx)
    return {idx*2, idx*2 + 1}
end

function Heap:_swap(idx1, idx2)
    local t = self.nodes[idx1]
    self.nodes[idx1] = self.nodes[idx2]
    self.nodes[idx2] = t
end

local function get_key(obj, key_parts)
    local key = {}
    for _, key_part in ipairs(key_parts) do
        table.insert(key, obj[key_part])
    end
    return key
end

-- left <= right
local function cmp_keys(left, right)
    checks('table', 'table')

    local max_len = #left > #right and #left or #right

    for i=1,max_len do
        local part_left = left[i]
        local part_right = right[i]

        if part_left ~= nil and part_right == nil then return false end

        if part_left ~= nil and part_right ~= nil then
            if part_left < part_right then return true end
            if part_left > part_right then return false end
        end
    end

    return true
end

function Heap:_ok(child_idx, parent_idx)
    -- XXX Use key parts
    local child_node = self.nodes[child_idx]
    local parent_node = self.nodes[parent_idx]

    local child_key = get_key(child_node.obj, self.key_parts)
    local parent_key = get_key(parent_node.obj, self.key_parts)

    -- return parent_key <= child_key
    return cmp_keys(parent_key, child_key)
end

function Heap:add(obj, meta)
    checks('table', 'table', '?')

    local node = {
        obj = obj,
        meta = meta
    }

    table.insert(self.nodes, node)

    local idx = #self.nodes
    local parent_idx = get_parent_idx(idx)

    while parent_idx ~= nil do
        if self:_ok(idx, parent_idx) then
            break
        end

        self:_swap(idx, parent_idx)

        idx = parent_idx
        parent_idx = get_parent_idx(idx)
    end
end

function Heap:get()
    if #self.nodes == 0 then
        return nil
    end

    return self.nodes[1]
end

function Heap:heapify(idx)
    idx = idx or 1

    while idx < #self.nodes do
        local better_idx = idx

        for _, child_idx in ipairs(get_children_idxs(idx)) do
            if child_idx <= #self.nodes and not self:_ok(child_idx, better_idx) then
                better_idx = child_idx
            end
        end

        if better_idx == idx then
            break
        end

        self:_swap(idx, better_idx)
        idx = better_idx
    end
end

function Heap:pop()
    local node = self:get()

    if node == nil then
        return nil
    end

    self:_swap(1, #self.nodes)
    table.remove(self.nodes, #self.nodes)

    self:heapify(1)

    return node
end

function Heap:size()
    return #self.nodes
end

function Heap:_print()
    for _, node in ipairs(self.nodes) do
        require('log').info('node: ' .. require('json').encode(node))
    end
end

return Heap
