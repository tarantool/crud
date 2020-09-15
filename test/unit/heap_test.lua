local t = require('luatest')
local g = t.group('heap')

local Heap = require('elect.common.heap')

g.test_get_one_part_number_key = function()
    local heap = Heap.new({ key_parts = {'id'} })
    t.assert_is(heap:get(), nil)

    heap:add({id = 2, value = 'B'}, 1)
    t.assert_equals(heap:size(), 1)
    t.assert(heap:get(), {
        obj = {id = 2, value = 'B'},
        replicaset_uuid = 1,
    })
    t.assert_equals(heap:size(), 1)

    heap:add({id = 3, value = 'A'}, 2)
    t.assert_equals(heap:size(), 2)
    t.assert(heap:get(), {
        tuple = {id = 2, value = 'B'},
        replicaset_uuid = 1,
    })
    t.assert_equals(heap:size(), 2)

    heap:add({id = 1, value = 'C'}, 3)
    t.assert_equals(heap:size(), 3)
    t.assert(heap:get(), {
        tuple = {id = 1, value = 'C'},
        replicaset_uuid = 3,
    })
    t.assert_equals(heap:size(), 3)
end

g.test_get_one_part_string_key = function()
    local heap = Heap.new({ key_parts = {'value'} })
    t.assert_is(heap:get(), nil)

    heap:add({id = 2, value = 'B'}, 1)
    t.assert_equals(heap:size(), 1)
    t.assert(heap:get(), {
        obj = {id = 2, value = 'B'},
        replicaset_uuid = 1,
    })
    t.assert_equals(heap:size(), 1)

    heap:add({id = 3, value = 'A'}, 2)
    t.assert_equals(heap:size(), 2)
    t.assert(heap:get(), {
        tuple = {id = 3, value = 'A'},
        replicaset_uuid = 2,
    })
    t.assert_equals(heap:size(), 2)

    heap:add({id = 1, value = 'C'}, 3)
    t.assert_equals(heap:size(), 3)
    t.assert(heap:get(), {
        tuple = {id = 3, value = 'A'},
        replicaset_uuid = 2,
    })
    t.assert_equals(heap:size(), 3)
end

g.test_get_multipart_key = function()
    local heap = Heap.new({ key_parts = {'id', 'value'} })
    t.assert_is(heap:get(), nil)

    heap:add({id = 2, value = 'B'}, 1)
    t.assert_equals(heap:size(), 1)
    t.assert(heap:get(), {
        obj = {id = 2, value = 'B'},
        replicaset_uuid = 1,
    })
    t.assert_equals(heap:size(), 1)

    heap:add({id = 2, value = 'A'}, 2)
    t.assert_equals(heap:size(), 2)
    t.assert(heap:get(), {
        tuple = {id = 2, value = 'A'},
        replicaset_uuid = 2,
    })
    t.assert_equals(heap:size(), 2)

    heap:add({id = 1, value = 'C'}, 3)
    t.assert_equals(heap:size(), 3)
    t.assert(heap:get(), {
        tuple = {id = 1, value = 'C'},
        replicaset_uuid = 3,
    })
    t.assert_equals(heap:size(), 3)
end

g.test_pop = function()
    local heap = Heap.new({ key_parts = {'id'} })

    local objects = {
        {id = 3, value = 'A'},
        {id = 10, value = 'B'},
        {id = 4, value = 'C'},
        {id = 2, value = 'D'},
        {id = 5, value = 'E'},
        {id = 7, value = 'F'},
        {id = 1, value = 'G'},
        {id = 6, value = 'H'},
    }

    for _, obj in ipairs(objects) do
        heap:add(obj)
    end

    local size = #objects
    t.assert_equals(heap:size(), size)

    table.sort(objects, function(a, b) return a.id < b.id end)

    for _, obj in ipairs(objects) do
        local node = heap:pop()
        t.assert_equals(node.obj, obj)

        size = size - 1
        t.assert_equals(heap:size(), size)
    end

    t.assert_equals(heap:pop(), nil)
end
