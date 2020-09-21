local t = require('luatest')
local g = t.group('heap')

local Heap = require('elect.common.heap')

g.test_get_one_part_number_key = function()
    local function comparator(left, right)
        return left[1] > right[1]
    end

    local heap = Heap.new({comparator = comparator})
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
    local function comparator(left, right)
        return left[2] > right[2]
    end

    local heap = Heap.new({comparator = comparator})
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
    local function comparator(left, right)
        if left[2] > right[2] then
            return true
        end

        if left[1] > right[1] then
            return true
        end

        return false
    end

    local heap = Heap.new({comparator = comparator})
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
    local function comparator(left, right)
        return left > right
    end

    local heap = Heap.new({comparator = comparator})
    t.assert_is(heap:get(), nil)


    local numbers = {3, 10, 5, 2, 5, 2, 1, 6}

    for _, number in ipairs(numbers) do
        heap:add(number)
    end

    local size = #numbers
    t.assert_equals(heap:size(), size)

    local sorted_numbers = {1, 2, 2, 3, 5, 5, 6, 10}

    for _, number in ipairs(sorted_numbers) do
        local node = heap:pop()
        t.assert_equals(node.obj, number)

        size = size - 1
        t.assert_equals(heap:size(), size)
    end

    t.assert_equals(heap:pop(), nil)
end
