local fun = require('fun')

local t = require('luatest')
local g = t.group('utils')

local utils = require('crud.common.utils')

local reverse_inplace_cases = {
    single_value = {1},
    two_values = {1, 2},
    three_values = {1, 2, 3},
    four_values = {1, 2, 3, 4},
    uneven_values = {1, 2, 3, 4, 5, 6, 7, 8, 9},
    even_values = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
}

for case_name, case in pairs(reverse_inplace_cases) do
    g["test_reverse_inplace_" .. case_name] = function()
        local case_reversed = {}
        for i=#case,1,-1 do
            table.insert(case_reversed, case[i])
        end
        case = utils.reverse_inplace(case)
        t.assert_equals(case, case_reversed)
    end
end


g.test_get_tarantool_version = function()
    local major, minor, patch, suffix, commits_since = utils.get_tarantool_version()

    t.assert_gt(major, 0, 'Tarantool version major is a positive number')
    t.assert_ge(minor, 0, 'Tarantool version minor is a non-negative number')
    t.assert_ge(patch, 0, 'Tarantool version patch is a non-negative number')

    if suffix ~= nil then
        t.assert_type(suffix, 'string')
    end

    if commits_since ~= nil then
        t.assert_type(commits_since, 'number')
    end
end


local get_version_suffix_cases = {
    entrypoint = {
        arg = 'entrypoint',
        res = 'entrypoint',
    },
    alpha1 = {
        arg = 'alpha1',
        res = 'alpha1',
    },
    alpha5 = {
        arg = 'alpha5',
        res = 'alpha5',
    },
    beta1 = {
        arg = 'beta1',
        res = 'beta1',
    },
    beta3 = {
        arg = 'beta3',
        res = 'beta3',
    },
    rc1 = {
        arg = 'rc1',
        res = 'rc1',
    },
    rc2 = {
        arg = 'rc2',
        res = 'rc2',
    },
    empty_string = {
        arg = '',
        res = nil,
    },
    commits_since_instead_of_suffix = {
        arg = '142',
        res = nil,
    },
}

for name, case in pairs(get_version_suffix_cases) do
    g['test_get_version_suffix_' .. name] = function()
        t.assert_equals(utils.get_version_suffix(case.arg), case.res)
    end
end


local get_version_suffix_weight_cases = {
    entrypoint = {
        arg = 'entrypoint',
        res = -math.huge,
    },
    alpha1 = {
        arg = 'alpha1',
        res = -2999,
    },
    alpha5 = {
        arg = 'alpha5',
        res = -2995,
    },
    beta1 = {
        arg = 'beta1',
        res = -1999,
    },
    beta3 = {
        arg = 'beta3',
        res = -1997,
    },
    rc1 = {
        arg = 'rc1',
        res = -999,
    },
    rc2 = {
        arg = 'rc2',
        res = -998,
    },
    ['nil'] = {
        arg = nil,
        res = 0,
    },
    empty_string = {
        arg = '',
        err = 'Unexpected suffix "", parse with "utils.get_version_suffix" first',
    },
    commits_since_instead_of_suffix = {
        arg = '142',
        err = 'Unexpected suffix "142", parse with "utils.get_version_suffix" first',
    },
}

for name, case in pairs(get_version_suffix_weight_cases) do
    g['test_get_version_suffix_weight_' .. name] = function()
        if case.err == nil then
            t.assert_equals(utils.get_version_suffix_weight(case.arg), case.res)
        else
            t.assert_error_msg_contains(case.err, utils.get_version_suffix_weight, case.arg)
        end
    end
end


-- Lua unpack() stops at first nil.
-- https://www.lua.org/pil/5.1.html
local function unpack_indices(args, indices, i)
    i = i or 1

    if #indices == i then
        return args[indices[i]]
    end

    return args[indices[i]], unpack_indices(args, indices, i + 1)
end

local function unpack_N(args, N)
    assert(N > 0)
    return unpack_indices(args, fun.range(1, N):totable())
end

do
    local v1, v2, v3, v4, v5, v6, v7, v8 = unpack_N({1, 2, nil, nil, 5, nil, 7, 8}, 8)
    assert(v1 == 1)
    assert(v2 == 2)
    assert(v3 == nil)
    assert(v4 == nil)
    assert(v5 == 5)
    assert(v6 == nil)
    assert(v7 == 7)
    assert(v8 == 8)
end

local function unpack_N_swap_halves(args, N)
    assert(N > 0)
    assert(N % 2 == 0)
    return unpack_indices(args, fun.chain(fun.range(N / 2 + 1, N), fun.range(1, N / 2)):totable())
end

do
    local v1, v2, v3, v4, v5, v6, v7, v8 = unpack_N_swap_halves({1, 2, nil, nil, 5, nil, 7, 8}, 8)
    assert(v1 == 5)
    assert(v2 == nil)
    assert(v3 == 7)
    assert(v4 == 8)
    assert(v5 == 1)
    assert(v6 == 2)
    assert(v7 == nil)
    assert(v8 == nil)
end


-- Pairwise-like testing cases since there are too many combinations.
local is_version_ge_cases = {
    case_1 = {
        args = {2, 10, 5, nil, nil,
                2, 10, 5, nil, nil},
        le = true,
    },
    case_2 = {
        args = {2, 10, 5, 'entrypoint', nil,
                2, 10, 5, 'entrypoint', nil},
        le = true,
    },
    case_3 = {
        args = {2, 10, 5, nil, nil,
                2, 10, 5, 'entrypoint', nil},
    },
    case_4 = {
        args = {2, 10, 5, 'alpha3', nil,
                2, 10, 5, 'alpha1', nil},
    },
    case_5 = {
        args = {2, 10, 5, 'rc1', nil,
                2, 10, 5, 'alpha1', nil},
    },
    case_6 = {
        args = {2, 10, 5, nil, nil,
                2, 10, 4, nil, nil},
    },
    case_7 = {
        args = {2, 10, 4, nil, nil,
                2, 9, 5, nil, nil},
    },
    case_8 = {
        args = {2, 10, 5, nil, nil,
                2, 9, 4, nil, nil},
    },
    case_9 = {
        args = {2, 10, 5, 'alpha1', nil,
                2, 9, 4, nil, nil},
    },
    case_10 = {
        args = {2, 10, 5, nil, nil,
                2, 9, 4, 'alpha1', nil},
    },
    case_11 = {
        args = {3, 10, 5, nil, nil,
                2, 10, 5, nil, nil},
    },
    case_12 = {
        args = {3, 9, 5, nil, nil,
                2, 10, 5, nil, nil},
    },
    case_13 = {
        args = {3, 10, 5, nil, nil,
                2, 9, 5, nil, nil},
    },
    case_14 = {
        args = {3, 10, 4, nil, nil,
                2, 10, 5, nil, nil},
    },
    case_15 = {
        args = {3, 10, 5, nil, nil,
                2, 10, 4, nil, nil},
    },
    case_16 = {
        args = {3, 10, 5, 'alpha1', nil,
                2, 10, 5, nil, nil},
    },
    case_17 = {
        args = {3, 10, 5, nil, nil,
                2, 10, 5, 'alpha1', nil},
    },
    case_18 = {
        args = {3, 10, 5, 'alpha1', nil,
                2, 10, 5, nil, nil},
    },
    case_19 = {
        args = {3, 10, 4, nil, nil,
                2, 10, 5, 'alpha1', nil},
    },
    case_20 = {
        args = {3, 9, 5, nil, nil,
                2, 10, 5, 'alpha1', nil},
    },
    case_21 = {
        args = {3, 9, 5, 'rc1', nil,
                2, 10, 4, nil, nil},
    },
    case_22 = {
        args = {2, 10, 5, nil, 0,
                2, 10, 5, nil, 0},
        le = true,
    },
    case_23 = {
        args = {2, 10, 5, nil, 10,
                2, 10, 5, nil, 0},
    },
    case_24 = {
        args = {2, 10, 5, 'beta2', 0,
                2, 10, 5, 'alpha1', 10},
    },
    case_25 = {
        args = {2, 10, 5, 'beta2', 15,
                2, 10, 5, 'alpha1', 10},
    },
    case_26 = {
        args = {2, 10, 6, nil, 0,
                2, 10, 5, nil, 10},
    },
    case_27 = {
        args = {2, 11, 5, nil, 0,
                2, 10, 5, nil, 10},
    },
    case_28 = {
        args = {3, 10, 5, nil, 0,
                2, 10, 5, nil, 10},
    },
}

for name, case in pairs(is_version_ge_cases) do
    g['test_is_version_ge_' .. name] = function()
        local ge = true
        if case.ge ~= nil then
            ge = case.ge
        end

        local le = not ge
        if case.le ~= nil then
            le = case.le
        end

        t.assert_equals(utils.is_version_ge(unpack_N(case.args, 10)), ge)
        t.assert_equals(utils.is_version_ge(unpack_N_swap_halves(case.args, 10)), le)
    end
end


-- Even more cases here, rely on utils.is_version_ge tests.
local is_version_in_range_cases = {
    case_1 = {
        args = {2, 11, 0, 'entrypoint', nil,
                2, 10, 4, nil, nil,
                3, 9, 5, 'rc1', nil},
        res = true,
    },
    case_2 = {
        args = {3, 9, 0, nil, nil,
                2, 10, 4, nil, nil,
                3, 4, 5, 'rc1', nil},
        res = false,
    },
    case_3 = {
        args = {3, 9, 0, nil, nil,
                2, 10, 4, nil, nil,
                3, 4, 5, 'rc1', nil},
        res = false,
    },
}

for name, case in pairs(is_version_in_range_cases) do
    g['test_is_version_range_' .. name] = function()
        t.assert_equals(utils.is_version_in_range(unpack_N(case.args, 15)), case.res)
    end
end
