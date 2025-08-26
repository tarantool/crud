local t = require("luatest")
local g = t.group()

local utils = require("crud.common.utils")

g.test_append_void = function()
    local res = utils.append_array({"too, foo"})
    t.assert_equals(res, {"too, foo"})
end

g.test_concat = function()
    local res = utils.append_array({"too, foo"}, {"bar, baz, buzz"})
    t.assert_equals(res, {"too, foo", "bar, baz, buzz"})
end
