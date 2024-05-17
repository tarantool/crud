local t = require('luatest')
local checks = require('checks')

-- Scenario is for 'srv_batch_operations' entrypoint.
local function gh_437_many_explicit_bucket_ids(cg, operation, opts)
    checks('table', 'string', {
        objects = '?boolean',
        upsert = '?boolean',
        partial_explicit_bucket_ids = '?boolean',
    })

    opts = opts or {}

    local rows = {
        {1, 1, 'Kumiko', 18},
        {2, 2, 'Reina', 19},
        {3, 3, 'Shuuichi', 18},
    }

    if opts.partial_explicit_bucket_ids then
        rows[2][2] = box.NULL
    end

    local objects = {}
    for k, v in ipairs(rows) do
        objects[k] = {id = v[1], bucket_id = v[2], name = v[3], age = v[4]}
    end

    local data
    if opts.objects then
        data = objects
    else
        data = rows
    end

    if opts.upsert then
        local update_operations = {}
        for k, v in ipairs(data) do
            data[k] = {v, update_operations}
        end
    end

    local args = {'customers_sharded_by_age', data}

    local result, errs = cg.router:call('crud.' .. operation, args)
    t.assert_equals(errs, nil)

    local result_rows = table.deepcopy(rows)
    if opts.partial_explicit_bucket_ids then
        result_rows[2][2] = 1325
    end
    if opts.upsert then
        -- upsert never return anything.
        t.assert_equals(result.rows, nil)
    else
        t.assert_items_equals(result.rows, result_rows)
    end
end

return {
    gh_437_many_explicit_bucket_ids = gh_437_many_explicit_bucket_ids,
}
