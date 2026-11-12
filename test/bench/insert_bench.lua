local lb = require('luabench')
local setup = require('test.bench.setup')
local operations = require('test.bench.operations')

local M = {}

local space_name, opts = "customers", {}

lb.before_all(function ()
    setup.setup()
end)

lb.after_all(function ()
    box.space[space_name]:truncate()
end)

---@param b luabench.B
function M.bench_insert(b)
    b:run('raw', function(sb)
        box.space[space_name]:truncate()
        for i = 1, sb.N do
            -- Truncate space every 10 million inserts to keep memory usage at bay.
            if i % 10000000 == 0 then
                box.space[space_name]:truncate()
            end
            operations.insert_raw(space_name, {i, 392, "John Fedor",100})
        end
    end)
    b:run('crud', function(sb)
        box.space[space_name]:truncate()
        for i = 1, sb.N do
            -- Truncate space every 10 million inserts to keep memory usage at bay.
            if i % 10000000 == 0 then
                box.space[space_name]:truncate()
            end
            operations.insert_crud(space_name, {i, 392, "John Fedor",100}, opts)
        end
    end)
end

return M
