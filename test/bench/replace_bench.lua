local lb = require('luabench')
local setup = require('test.bench.setup')
local operations = require('test.bench.operations')

local M = {}

local space_name, tuple, opts = "customers", {45, 392, "John Fedor",100}, {}

lb.before_all(function ()
    setup.setup()
end)

lb.after_all(function ()
    box.space[space_name]:truncate()
end)

---@param b luabench.B
function M.bench_replace(b)
    b:run('raw', function(sb)
        box.space[space_name]:truncate()
        for _ = 1, sb.N do
            operations.replace_raw(space_name, tuple)
        end
    end)
    b:run('crud', function(sb)
        box.space[space_name]:truncate()
        for _ = 1, sb.N do
            operations.replace_crud(space_name, tuple, opts)
        end
    end)
end



return M
