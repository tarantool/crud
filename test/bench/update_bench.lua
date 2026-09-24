local lb = require('luabench')
local setup = require('test.bench.setup')
local operations = require('test.bench.operations')

local M = {}

local space_name, tuple, opts = "customers", {45, 392, "John Fedor",100}, {bucket_id = 392}
local key = 45
local ops = {{'+', 4, 1}}

lb.before_all(function ()
    setup.setup()
end)

lb.after_all(function ()
    box.space[space_name]:truncate()
end)

---@param b luabench.B
function M.bench_update(b)
    b:run('raw', function(sb)
        box.space[space_name]:truncate()
        operations.replace_crud(space_name, tuple)
        for _ = 1, sb.N do
            operations.update_raw(space_name, key, ops)
        end
    end)
    b:run('crud', function(sb)
        box.space[space_name]:truncate()
        operations.replace_crud(space_name, tuple)
        for _ = 1, sb.N do
            operations.update_crud(space_name, key, ops, nil, opts)
        end
    end)
end

return M
