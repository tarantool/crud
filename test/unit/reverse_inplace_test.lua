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
