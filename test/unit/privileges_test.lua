local t = require("luatest")
local g = t.group()

local helper = require("test.helper")
local call = require("crud.common.call")

g.before_all(function()
    helper.box_cfg({listen = 3401})

    box.schema.user.create("unittestuser", {password = "secret", if_not_exists = true})
    box.schema.user.grant("unittestuser", "read,write,execute,create,alter,drop", "universe", nil,
        {if_not_exists = true})

    rawset(_G, "unittestfunc", function(...)
        return ...
    end)
end)

g.test_prepend_current_user_smoke = function()
    local res = call.storage_api.call_on_storage(box.session.effective_user(), {}, "read", "unittestfunc", {"too", "foo"})
    t.assert_equals(res, {"too", "foo"})
end

g.test_non_existent_user = function()
    t.assert_error_msg_contains("User 'non_existent_user' is not found",
        call.storage_api.call_on_storage, "non_existent_user", {}, "read", "unittestfunc")
end

g.test_that_the_session_switches_back = function()
    rawset(_G, "unittestfunc2", function()
        return box.session.effective_user()
    end)

    local reference_user = box.session.effective_user()
    t.assert_not_equals(reference_user, "unittestuser")

    local res = call.storage_api.call_on_storage("unittestuser", {}, "read", "unittestfunc2")
    t.assert_equals(res, "unittestuser")
    t.assert_equals(box.session.effective_user(), reference_user)
end
