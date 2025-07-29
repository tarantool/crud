local t = require('luatest')
local g = t.group()

local sharding = require('crud.common.sharding')

local ffi = require('ffi')

local cases = {
    positive_number      = {value = 1, should_fail = false},
    large_number         = {value = 100000, should_fail = false},
    zero                 = {value = 0, should_fail = true},
    negative_number      = {value = -1, should_fail = true},
    non_integer_number   = {value = 123.45, should_fail = true},
    string_value         = {value = 'abc', should_fail = true},
    boolean_value        = {value = true, should_fail = true},
    table_value          = {value = {}, should_fail = true},
    nil_value            = {value = nil, should_fail = true},
    box_null             = {value = box.NULL, should_fail = true},
    ffi_uint64           = {value = ffi.new('uint64_t', 1), should_fail = false},
    ffi_uint64_zero      = {value = ffi.new('uint64_t', 0), should_fail = true},
    ffi_int64_negative   = {value = ffi.new('int64_t', -1), should_fail = true},
}

for name, case in pairs(cases) do
    g["test_validate_bucket_id_" .. name] = function()
        local err = sharding.validate_bucket_id(case.value)

        if case.should_fail then
            t.assert(err, ('%s should be rejected'):format(name))
            t.assert_equals(err.class_name, 'BucketIDError')
            t.assert_str_contains(err.err, 'expected unsigned')
        else
            t.assert_equals(err, nil, ('%s should be accepted'):format(name))
        end
    end
end
