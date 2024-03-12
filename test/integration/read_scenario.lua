-- crud.select/crud.pairs/crud.count/readview:select/readview:pairs
-- have a lot of common scenarios, which are mostly tested with
-- four nearly identical copypasted test functions now.
-- This approach is expected to improve it at least for new test cases.
-- Scenarios here are for `srv_select` entrypoint.

local t = require('luatest')
local datetime_supported, datetime = pcall(require, 'datetime')
local decimal_supported, decimal = pcall(require, 'decimal')

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
    local result, err = read(cg,
        'logins',
        {{'=', 'city', 'Tatsumi Port Island'}, {'<=', 'last_login', 42}},
        {bucket_id = PINNED_BUCKET_NO}
    )
    t.assert_equals(err, nil)

    if type(result) == 'number' then -- crud.count
        t.assert_equals(result, 2)
    else
        t.assert_equals(result, {expected_objects[1], expected_objects[3]})
    end
end


local function build_condition_case(
    skip_test_condition,
    space_name,
    space_objects,
    conditions,
    expected_objects_without_bucket_id
)
    return function(cg, read)
        skip_test_condition()

        helpers.truncate_space_on_cluster(cg.cluster, space_name)
        helpers.insert_objects(cg, space_name, space_objects)

        local result, err = read(cg, space_name, conditions)
        t.assert_equals(err, nil)

        if type(result) == 'number' then -- crud.count
            t.assert_equals(result, #expected_objects_without_bucket_id)
        else
            local actual_objects_without_bucket_id = {}
            for k, v in pairs(result) do
                v['bucket_id'] = nil
                actual_objects_without_bucket_id[k] = v
            end

            t.assert_items_equals(actual_objects_without_bucket_id, expected_objects_without_bucket_id)
        end
    end
end


local decimal_vals = {}

if decimal_supported then
    decimal_vals = {
        smallest_negative = decimal.new('-123456789012345678.987431234678392'),
        bigger_negative = decimal.new('-123456789012345678.987431234678391'),
        bigger_positive = decimal.new('123456789012345678.987431234678391'),
    }

    assert(decimal_vals.smallest_negative < decimal_vals.bigger_negative)
    assert(decimal_vals.bigger_negative < decimal_vals.bigger_positive)
end

local decimal_data = {
    {
        id = 1,
        decimal_field = decimal_vals.smallest_negative,
    },
    {
        id = 2,
        decimal_field = decimal_vals.bigger_negative,
    },
    {
        id = 3,
        decimal_field = decimal_vals.bigger_positive,
    },
}

local function bigger_negative_condition(operator, operand, is_multipart)
    if is_multipart then
        return {operator, operand, {2, decimal_vals.bigger_negative}}
    else
        return {operator, operand, decimal_vals.bigger_negative}
    end
end

local decimal_condition_operator_options = {
    single_lt = function(operand, is_multipart)
        return {
            conditions = {bigger_negative_condition('<', operand, is_multipart)},
            expected_objects_without_bucket_id = {
                {
                    id = 1,
                    decimal_field = decimal_vals.smallest_negative,
                },
            },
        }
    end,
    single_le = function(operand, is_multipart)
        return {
            conditions = {bigger_negative_condition('<=', operand, is_multipart)},
            expected_objects_without_bucket_id = {
                {
                    id = 1,
                    decimal_field = decimal_vals.smallest_negative,
                },
                {
                    id = 2,
                    decimal_field = decimal_vals.bigger_negative,
                },
            },
        }
    end,
    single_eq = function(operand, is_multipart)
        return {
            conditions = {bigger_negative_condition('==', operand, is_multipart)},
            expected_objects_without_bucket_id = {
                {
                    id = 2,
                    decimal_field = decimal_vals.bigger_negative,
                },
            },
        }
    end,
    single_ge = function(operand, is_multipart)
        return {
            conditions = {bigger_negative_condition('>=', operand, is_multipart)},
            expected_objects_without_bucket_id = {
                {
                    id = 2,
                    decimal_field = decimal_vals.bigger_negative,
                },
                {
                    id = 3,
                    decimal_field = decimal_vals.bigger_positive,
                },
            },
        }
    end,
    single_gt = function(operand, is_multipart)
        return {
            conditions = {bigger_negative_condition('>', operand, is_multipart)},
            expected_objects_without_bucket_id = {
                {
                    id = 3,
                    decimal_field = decimal_vals.bigger_positive,
                },
            },
        }
    end,
    second_lt = function(operand, is_multipart)
        return {
            conditions = {{'>=', 'id', 1}, bigger_negative_condition('<', operand, is_multipart)},
            expected_objects_without_bucket_id = {
                {
                    id = 1,
                    decimal_field = decimal_vals.smallest_negative,
                },
            },
        }
    end,
    second_le = function(operand, is_multipart)
        return {
            conditions = {{'>=', 'id', 1}, bigger_negative_condition('<=', operand, is_multipart)},
            expected_objects_without_bucket_id = {
                {
                    id = 1,
                    decimal_field = decimal_vals.smallest_negative,
                },
                {
                    id = 2,
                    decimal_field = decimal_vals.bigger_negative,
                },
            },
        }
    end,
    second_eq = function(operand, is_multipart)
        return {
            conditions = {{'>=', 'id', 1}, bigger_negative_condition('==', operand, is_multipart)},
            expected_objects_without_bucket_id = {
                {
                    id = 2,
                    decimal_field = decimal_vals.bigger_negative,
                },
            },
        }
    end,
    second_ge = function(operand, is_multipart)
        return {
            conditions = {{'>=', 'id', 1}, bigger_negative_condition('>=', operand, is_multipart)},
            expected_objects_without_bucket_id = {
                {
                    id = 2,
                    decimal_field = decimal_vals.bigger_negative,
                },
                {
                    id = 3,
                    decimal_field = decimal_vals.bigger_positive,
                },
            },
        }
    end,
    second_gt = function(operand, is_multipart)
        return {
            conditions = {{'>=', 'id', 1}, bigger_negative_condition('>', operand, is_multipart)},
            expected_objects_without_bucket_id = {
                {
                    id = 3,
                    decimal_field = decimal_vals.bigger_positive,
                },
            },
        }
    end,
}

local decimal_condition_space_options = {
    nonindexed = {
        space_name = 'decimal_nonindexed',
        index_kind = nil,
    },
    indexed = {
        space_name = 'decimal_indexed',
        index_kind = 'secondary',
    },
    pk = {
        space_name = 'decimal_pk',
        index_kind = 'primary',
    },
    multipart_indexed = {
        space_name = 'decimal_multipart_index',
        index_kind = 'multipart',
        is_multipart = true,
    },
}

local gh_373_read_with_decimal_condition_cases = {}

for space_kind, space_option in pairs(decimal_condition_space_options) do
    for operator_kind, operator_options_builder in pairs(decimal_condition_operator_options) do
        local field_case_name_template = ('gh_373_%%s_with_decimal_%s_field_%s_condition'):format(
                                          space_kind, operator_kind)

        local field_operator_options = operator_options_builder('decimal_field', false)

        gh_373_read_with_decimal_condition_cases[field_case_name_template] = build_condition_case(
            helpers.skip_decimal_unsupported,
            space_option.space_name,
            decimal_data,
            field_operator_options.conditions,
            field_operator_options.expected_objects_without_bucket_id
        )

        if space_option.index_kind ~= nil then
            local index_case_name_template = ('gh_373_%%s_with_decimal_%s_index_%s_condition'):format(
                                              space_option.index_kind, operator_kind)

            local index_operator_options = operator_options_builder('decimal_index', space_option.is_multipart)

            gh_373_read_with_decimal_condition_cases[index_case_name_template] = build_condition_case(
                helpers.skip_decimal_unsupported,
                space_option.space_name,
                decimal_data,
                index_operator_options.conditions,
                index_operator_options.expected_objects_without_bucket_id
            )
        end
    end
end


local datetime_vals = {}

if datetime_supported then
    datetime_vals = {
        yesterday = datetime.new{
            year = 2024,
            month = 3,
            day = 10,
        },
        today = datetime.new{
            year = 2024,
            month = 3,
            day = 11,
        },
        tomorrow = datetime.new{
            year = 2024,
            month = 3,
            day = 12,
        },
    }

    assert(datetime_vals.yesterday < datetime_vals.today)
    assert(datetime_vals.today < datetime_vals.tomorrow)
end

local datetime_data = {
    {
        id = 1,
        datetime_field = datetime_vals.yesterday,
    },
    {
        id = 2,
        datetime_field = datetime_vals.today,
    },
    {
        id = 3,
        datetime_field = datetime_vals.tomorrow,
    },
}

local function today_condition(operator, operand, is_multipart)
    if is_multipart then
        return {operator, operand, {2, datetime_vals.today}}
    else
        return {operator, operand, datetime_vals.today}
    end
end

local datetime_condition_operator_options = {
    single_lt = function(operand, is_multipart)
        return {
            conditions = {today_condition('<', operand, is_multipart)},
            expected_objects_without_bucket_id = {
                {
                    id = 1,
                    datetime_field = datetime_vals.yesterday,
                },
            },
        }
    end,
    single_le = function(operand, is_multipart)
        return {
            conditions = {today_condition('<=', operand, is_multipart)},
            expected_objects_without_bucket_id = {
                {
                    id = 1,
                    datetime_field = datetime_vals.yesterday,
                },
                {
                    id = 2,
                    datetime_field = datetime_vals.today,
                },
            },
        }
    end,
    single_eq = function(operand, is_multipart)
        return {
            conditions = {today_condition('==', operand, is_multipart)},
            expected_objects_without_bucket_id = {
                {
                    id = 2,
                    datetime_field = datetime_vals.today,
                },
            },
        }
    end,
    single_ge = function(operand, is_multipart)
        return {
            conditions = {today_condition('>=', operand, is_multipart)},
            expected_objects_without_bucket_id = {
                {
                    id = 2,
                    datetime_field = datetime_vals.today,
                },
                {
                    id = 3,
                    datetime_field = datetime_vals.tomorrow,
                },
            },
        }
    end,
    single_gt = function(operand, is_multipart)
        return {
            conditions = {today_condition('>', operand, is_multipart)},
            expected_objects_without_bucket_id = {
                {
                    id = 3,
                    datetime_field = datetime_vals.tomorrow,
                },
            },
        }
    end,
    second_lt = function(operand, is_multipart)
        return {
            conditions = {{'>=', 'id', 1}, today_condition('<', operand, is_multipart)},
            expected_objects_without_bucket_id = {
                {
                    id = 1,
                    datetime_field = datetime_vals.yesterday,
                },
            },
        }
    end,
    second_le = function(operand, is_multipart)
        return {
            conditions = {{'>=', 'id', 1}, today_condition('<=', operand, is_multipart)},
            expected_objects_without_bucket_id = {
                {
                    id = 1,
                    datetime_field = datetime_vals.yesterday,
                },
                {
                    id = 2,
                    datetime_field = datetime_vals.today,
                },
            },
        }
    end,
    second_eq = function(operand, is_multipart)
        return {
            conditions = {{'>=', 'id', 1}, today_condition('==', operand, is_multipart)},
            expected_objects_without_bucket_id = {
                {
                    id = 2,
                    datetime_field = datetime_vals.today,
                },
            },
        }
    end,
    second_ge = function(operand, is_multipart)
        return {
            conditions = {{'>=', 'id', 1}, today_condition('>=', operand, is_multipart)},
            expected_objects_without_bucket_id = {
                {
                    id = 2,
                    datetime_field = datetime_vals.today,
                },
                {
                    id = 3,
                    datetime_field = datetime_vals.tomorrow,
                },
            },
        }
    end,
    second_gt = function(operand, is_multipart)
        return {
            conditions = {{'>=', 'id', 1}, today_condition('>', operand, is_multipart)},
            expected_objects_without_bucket_id = {
                {
                    id = 3,
                    datetime_field = datetime_vals.tomorrow,
                },
            },
        }
    end,
}

local datetime_condition_space_options = {
    nonindexed = {
        space_name = 'datetime_nonindexed',
        index_kind = nil,
    },
    indexed = {
        space_name = 'datetime_indexed',
        index_kind = 'secondary',
    },
    pk = {
        space_name = 'datetime_pk',
        index_kind = 'primary',
    },
    multipart_indexed = {
        space_name = 'datetime_multipart_index',
        index_kind = 'multipart',
        is_multipart = true,
    },
}

local gh_373_read_with_datetime_condition_cases = {}

for space_kind, space_option in pairs(datetime_condition_space_options) do
    for operator_kind, operator_options_builder in pairs(datetime_condition_operator_options) do
        local field_case_name_template = ('gh_373_%%s_with_datetime_%s_field_%s_condition'):format(
                                          space_kind, operator_kind)

        local field_operator_options = operator_options_builder('datetime_field', false)

        gh_373_read_with_datetime_condition_cases[field_case_name_template] = build_condition_case(
            helpers.skip_datetime_unsupported,
            space_option.space_name,
            datetime_data,
            field_operator_options.conditions,
            field_operator_options.expected_objects_without_bucket_id
        )

        if space_option.index_kind ~= nil then
            local index_case_name_template = ('gh_373_%%s_with_datetime_%s_index_%s_condition'):format(
                                              space_option.index_kind, operator_kind)

            local index_operator_options = operator_options_builder('datetime_index', space_option.is_multipart)

            gh_373_read_with_datetime_condition_cases[index_case_name_template] = build_condition_case(
                helpers.skip_datetime_unsupported,
                space_option.space_name,
                datetime_data,
                index_operator_options.conditions,
                index_operator_options.expected_objects_without_bucket_id
            )
        end
    end
end


local function before_merger_process_storage_error(cg)
    helpers.call_on_storages(cg.cluster, function(server)
        server:exec(function()
            local space
            if box.info.ro == false then
                space = box.schema.space.create('speedy_gonzales', {if_not_exists = true})

                space:format({
                    {name = 'id', type = 'unsigned'},
                    {name = 'bucket_id', type = 'unsigned'},
                })

                space:create_index('pk', {
                    parts = {'id'},
                    if_not_exists = true,
                })

                space:create_index('bucket_id', {
                    parts = {'bucket_id'},
                    unique = false,
                    if_not_exists = true,
                })
            end

            local real_select_impl = rawget(_G, '_crud').select_on_storage
            rawset(_G, '_real_select_impl', real_select_impl)

            local real_select_readview_impl = rawget(_G, '_crud').select_readview_on_storage
            rawset(_G, '_real_select_readview_impl', real_select_readview_impl)

            -- Drop right before select to cause storage-side error.
            -- Work guaranteed only with mode = 'write'.
            local function erroneous_select_impl(...)
                if box.info.ro == false then
                    space:drop()
                end

                return real_select_impl(...)
            end
            rawget(_G, '_crud').select_on_storage = erroneous_select_impl

            -- Close right before select to cause storage-side error.
            -- Work guaranteed only with mode = 'write'.
            local function erroneous_select_readview_impl(space_name, index_id, conditions, opts)
                local list = box.read_view.list()

                for k,v in pairs(list) do
                    if v.id == opts.readview_id then
                        list[k]:close()
                    end
                end

                return real_select_readview_impl(space_name, index_id, conditions, opts)
            end
            rawget(_G, '_crud').select_readview_on_storage = erroneous_select_readview_impl
        end)
    end)
end

local function merger_process_storage_error(cg, read)
    local _, err = read(cg, 'speedy_gonzales', {{'==', 'id', 1}})
    t.assert_not_equals(err, nil)

    local err_msg = err.err or tostring(err)
    t.assert_str_contains(err_msg, "Space \"speedy_gonzales\" doesn't exist")
end

local function after_merger_process_storage_error(cg)
    helpers.call_on_storages(cg.cluster, function(server)
        server:exec(function()
            local real_select_impl = rawget(_G, '_real_select_impl')
            rawget(_G, '_crud').select_on_storage = real_select_impl

            local real_select_readview_impl = rawget(_G, '_real_select_readview_impl')
            rawget(_G, '_crud').select_readview_on_storage = real_select_readview_impl
        end)
    end)
end

return {
    gh_418_read_with_secondary_noneq_index_condition = gh_418_read_with_secondary_noneq_index_condition,
    gh_373_read_with_decimal_condition_cases = gh_373_read_with_decimal_condition_cases,
    gh_373_read_with_datetime_condition_cases = gh_373_read_with_datetime_condition_cases,
    before_merger_process_storage_error = before_merger_process_storage_error,
    merger_process_storage_error = merger_process_storage_error,
    after_merger_process_storage_error = after_merger_process_storage_error,
}
