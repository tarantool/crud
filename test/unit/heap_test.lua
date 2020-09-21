local key_def_lib = require('key_def')

local t = require('luatest')
local g = t.group('heap')

local Heap = require('elect.common.heap')

g.test_get_one_part_number_key = function()
    local key_parts = {
        {fieldno = 1, type = 'unsigned'},
    }

    local function gt_comparator(left, right)
        local key_def = key_def_lib.new(key_parts)
        return key_def:compare(left, right) < 0
    end

    local heap = Heap.new({comparator = gt_comparator})
    t.assert_is(heap:get(), nil)

    heap:add({2, 'B'}, {replicaset_uuid = 1})
    t.assert_equals(heap:size(), 1)
    t.assert_equals(heap:get(), {
        obj = {2, 'B'},
        meta = {replicaset_uuid = 1},
    })

    heap:add({3, 'A'}, {replicaset_uuid = 2})
    t.assert_equals(heap:size(), 2)
    t.assert_equals(heap:get(), {
        obj = {2, 'B'},
        meta = {replicaset_uuid = 1},
    })

    heap:add({1, 'C'}, {replicaset_uuid = 3})
    t.assert_equals(heap:size(), 3)
    t.assert_equals(heap:get(), {
        obj = {1, 'C'},
        meta = {replicaset_uuid = 3},
    })
end

g.test_get_one_part_string_key = function()
    local key_parts = {
        {fieldno = 2, type = 'string'}
    }

    local function gt_comparator(left, right)
        local key_def = key_def_lib.new(key_parts)
        return key_def:compare(left, right) < 0
    end

    local heap = Heap.new({comparator = gt_comparator})
    t.assert_is(heap:get(), nil)

    heap:add({2, 'B'}, {replicaset_uuid = 1})
    t.assert_equals(heap:size(), 1)
    t.assert_equals(heap:get(), {
        obj = {2, 'B'},
        meta = {replicaset_uuid = 1},
    })

    heap:add({3, 'A'}, {replicaset_uuid = 2})
    t.assert_equals(heap:size(), 2)
    t.assert_equals(heap:get(), {
        obj = {3, 'A'},
        meta = {replicaset_uuid = 2},
    })

    heap:add({1, 'C'}, {replicaset_uuid = 3})
    t.assert_equals(heap:size(), 3)
    t.assert_equals(heap:get(), {
        obj = {3, 'A'},
        meta = {replicaset_uuid = 2},
    })
end

g.test_get_multipart_key = function()
    local key_parts = {
        {fieldno = 2, type = 'unsigned'},
        {fieldno = 1, type = 'string'},
    }

    local function gt_comparator(left, right)
        local key_def = key_def_lib.new(key_parts)
        return key_def:compare(left, right) < 0
    end

    local heap = Heap.new({comparator = gt_comparator})
    t.assert_is(heap:get(), nil)

    heap:add({'B', 2, 'X'}, {replicaset_uuid = 3})
    t.assert_equals(heap:size(), 1)
    t.assert(heap:get(), {
        obj = {'B', 2, 'X'},
        meta = {replicaset_uuid = 3},
    })

    heap:add({'B', 1, 'X'}, 2)
    t.assert_equals(heap:size(), 2)
    t.assert(heap:get(), {
        obj = {'B', 1, 'X'},
        meta = {replicaset_uuid = 2},
    })

    heap:add({'C', 1, 'X'}, 3)
    t.assert_equals(heap:size(), 3)
    t.assert(heap:get(), {
        obj = {'B', 1, 'X'},
        meta = {replicaset_uuid = 2},
    })
end

g.test_pop = function()
    local key_parts = {
        {fieldno = 3, type = 'unsigned'},
        {fieldno = 2, type = 'string'},
    }

    local function gt_comparator(left, right)
        local key_def = key_def_lib.new(key_parts)
        return key_def:compare(left, right) < 0
    end

    local heap = Heap.new({comparator = gt_comparator})

    local tuples = {
        {'x', 'A', 3},
        {'y', 'B', 10},
        {'z', 'C', 5},
        {'a', 'D', 2},
        {'b', 'E', 5},
        {'c', 'F', 2},
        {'k', 'G', 1},
        {'l', 'H', 6},
    }

    for _, tuple in ipairs(tuples) do
        heap:add(tuple)
    end

    local size = #tuples
    t.assert_equals(heap:size(), size)

    local sorted_tuples = {
        {'k', 'G', 1},
        {'a', 'D', 2},
        {'c', 'F', 2},
        {'x', 'A', 3},
        {'z', 'C', 5},
        {'b', 'E', 5},
        {'l', 'H', 6},
        {'y', 'B', 10},
    }

    for _, tuple in ipairs(sorted_tuples) do
        local node = heap:pop()
        t.assert_equals(node.obj, tuple)

        size = size - 1
        t.assert_equals(heap:size(), size)
    end

    t.assert_equals(heap:pop(), nil)
end
