-- crud.select/crud.pairs/readview:select/readview:pairs
-- have a lot of common scenarios, which are mostly tested with
-- four nearly identical copypasted test functions now.
-- This approach is expected to improve it at least for new test cases.
-- Scenarios here are for `srv_select` entrypoint.

local t = require('luatest')

local helpers = require('test.helper')

local function gh_418_read_with_secondary_noneq_index_condition(cg, read)
    -- Pin bucket_id to reproduce issue on a single storage.
    local PINNED_BUCKET_NO = 1

    local expected_objects = {
        {
            id = 1,
            bucket_id = PINNED_BUCKET_NO,
            city = 'Tatsumi Port Island',
            name = 'Yukari',
            last_login = 42,
        },
        {
            id = 2,
            bucket_id = PINNED_BUCKET_NO,
            city = 'Tatsumi Port Island',
            name = 'Junpei',
            last_login = 52,
        },
        {
            id = 3,
            bucket_id = PINNED_BUCKET_NO,
            city = 'Tatsumi Port Island',
            name = 'Mitsuru',
            last_login = 42,
        },
    }

    helpers.prepare_ordered_data(cg,
        'logins',
        expected_objects,
        PINNED_BUCKET_NO,
        {'=', 'city', 'Tatsumi Port Island'}
    )

    -- Issue https://github.com/tarantool/crud/issues/418 is as follows:
    -- storage iterator exits early on the second tuple because
    -- iterator had erroneously expected tuples to be sorted by `last_login`
    -- index while iterating on `city` index. Before the issue had beed fixed,
    -- user had received only one record instead of two.
    local objects = read(cg,
        'logins',
        {{'=', 'city', 'Tatsumi Port Island'}, {'<=', 'last_login', 42}},
        {bucket_id = PINNED_BUCKET_NO}
    )

    t.assert_equals(objects, {expected_objects[1], expected_objects[3]})
end

return {
    gh_418_read_with_secondary_noneq_index_condition = gh_418_read_with_secondary_noneq_index_condition,
}
