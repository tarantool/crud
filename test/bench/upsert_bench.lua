local lb = require('luabench')
local setup = require('test.bench.setup')
local operations = require('test.bench.operations')

local M = {}

local space_name, tuple, opts = "customers", {45, 392, "John Fedor",100}, {}
local ops = {{'+', 4, 1}}

lb.before_all(function ()
    setup.setup()
end)

lb.after_all(function ()
    box.space[space_name]:truncate()
end)

---@param b luabench.B
function M.bench_upsert(b)

    b:run('raw', function(sb)
        box.space[space_name]:truncate()
        operations.replace_crud(space_name, tuple)
        for _ = 1, sb.N do
            operations.upsert_raw(space_name, tuple, ops)
        end
    end)
    b:run('crud',function(sb)
        box.space[space_name]:truncate()
        operations.replace_crud(space_name, tuple)
        for _ = 1, sb.N do
            operations.upsert_crud(space_name, tuple, ops, opts)
        end
    end)
end

return M
