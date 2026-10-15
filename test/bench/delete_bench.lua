local lb = require('luabench')
local setup = require('test.bench.setup')
local operations = require('test.bench.operations')

local M = {}

local space_name, primary_key, opts = 'customers', 45, {bucket_id = 392}

lb.before_all(function ()
    setup.setup()
end)

lb.after_all(function ()
    box.space[space_name]:truncate()
end)

---@param b luabench.B
function M.bench_delete(b)
    b:run('raw', function(sb)
        for _ = 1, sb.N do
            operations.delete_raw(space_name, primary_key)
        end
    end)
    b:run('crud', function(sb)
        for _ = 1, sb.N do
            operations.delete_crud(space_name, primary_key, nil, opts)
        end
    end)
end

return M
