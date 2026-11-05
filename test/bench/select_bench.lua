local lb = require('luabench')
local setup = require('test.bench.setup')
local operations = require('test.bench.operations')

local M = {}

local space_name = "customers"
local LIMIT = 10
local NUM_RECORDS = 100

lb.before_all(function ()
    setup.setup()

    -- 100 records, bucket_id cycles 0..9 so each bucket has 10 records.
    for i = 1, NUM_RECORDS do
        box.space[space_name]:replace({i, i % 10, "John Fedor", 100})
    end
end)

lb.after_all(function ()
    box.space[space_name]:truncate()
end)

-- ============================================================
-- Select by primary index (GE scan, multiple records)
-- ============================================================

---@param b luabench.B
function M.bench_select_primary(b)
    b:run('raw', function(sb)
        local opts = {iterator = box.index.GE, limit = LIMIT}
        for _ = 1, sb.N do
            operations.select_raw(space_name, 'id', {1}, opts)
        end
    end)
    b:run('crud', function(sb)
        local opts = {
            scan_value = {1},
            tarantool_iter = box.index.GE,
            limit = LIMIT,
        }
        for _ = 1, sb.N do
            operations.select_crud(space_name, 0, {}, opts)
        end
    end)
end

-- ============================================================
-- Select by secondary index (EQ on bucket_id)
-- ============================================================

---@param b luabench.B
function M.bench_select_secondary(b)
    b:run('raw', function(sb)
        local opts = {iterator = box.index.EQ, limit = LIMIT}
        for _ = 1, sb.N do
            operations.select_raw(space_name, 'bucket_id', {5}, opts)
        end
    end)
    b:run('crud', function(sb)
        local opts = {
            scan_value = {5},
            tarantool_iter = box.index.EQ,
            limit = LIMIT,
        }
        for _ = 1, sb.N do
            operations.select_crud(space_name, 1, {}, opts)
        end
    end)
end

-- ============================================================
-- Select with after by primary index (pagination)
-- after_tuple at id=90 (near end of 100 records)
-- ============================================================

---@param b luabench.B
function M.bench_select_after_primary(b)
    b:run('raw', function(sb)
        local after_tuple = box.space[space_name]:get(90)
        local opts = { iterator = box.index.GE, limit = LIMIT, after = after_tuple }
        for _ = 1, sb.N do
            operations.select_raw(space_name, 'id', {}, opts)
        end
    end)
    b:run('crud', function(sb)
        local after_tuple = box.space[space_name]:get(90)
        local opts = {
            scan_value = {},
            tarantool_iter = box.index.GE,
            limit = LIMIT,
            after_tuple = after_tuple,
        }
        for _ = 1, sb.N do
            operations.select_crud(space_name, 0, {}, opts)
        end
    end)
end

-- ============================================================
-- Select with after by secondary index (pagination)
-- after_tuple at id=85 (bucket_id=85%10=5, matches key)
-- ============================================================

---@param b luabench.B
function M.bench_select_after_secondary(b)
    b:run('raw', function(sb)
        -- id=85 has bucket_id=5, consistent with EQ key below
        local after_tuple = box.space[space_name]:get(85)
        local opts = {iterator = box.index.EQ, limit = LIMIT, after = after_tuple}
        for _ = 1, sb.N do
            operations.select_raw(space_name, 'bucket_id', {5}, opts)
        end
    end)
    b:run('crud', function(sb)
        local after_tuple = box.space[space_name]:get(85)
        local opts = {
            scan_value = {5},
            tarantool_iter = box.index.EQ,
            limit = LIMIT,
            after_tuple = after_tuple,
        }
        for _ = 1, sb.N do
            operations.select_crud(space_name, 1, {}, opts)
        end
    end)
end

return M
