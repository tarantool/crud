local lb = require('luabench')
local setup = require('test.bench.setup')
local operations = require('test.bench.operations')

local M = {}

local space_name = "customers"
local tuple = {45, 392, "John Fedor",100}
local opts = {bucket_id = 392}
local primary_key = 45
local primary_key_empty = 100500

lb.before_all(function ()
    setup.setup()
end)

lb.after_all(function ()
    box.space[space_name]:truncate()
end)

---@param b luabench.B
function M.bench_get(b)
    b:run('raw', function(sb)
        operations.replace_raw(space_name, tuple)
        for _ = 1, sb.N do
            operations.get_raw(space_name, primary_key)
        end
    end)
    b:run('crud', function(sb)
        operations.replace_raw(space_name, tuple)
        for _ = 1, sb.N do
            operations.get_crud(space_name, primary_key, nil, opts)
        end
    end)
end

---@param b luabench.B
function M.bench_get_empty(b)
    b:run('raw', function (sb)
        operations.replace_raw(space_name, tuple)
        for _ = 1, sb.N do
            operations.get_raw(space_name, primary_key_empty)
        end
    end)
    b:run('crud', function (sb)
        operations.replace_raw(space_name, tuple)
        for _ = 1, sb.N do
            operations.get_crud(space_name, primary_key_empty, nil, opts)
        end
    end)
end

return M
