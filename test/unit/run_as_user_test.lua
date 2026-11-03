local t = require('luatest')

local helper = require('test.helper')
local utils = require('crud.common.utils')

local g = t.group()

g.before_all(function()
    helper.box_cfg({listen = 3402})
end)

g.after_each(function()
    utils.set_run_as_user(true)
end)

g.test_default_is_run_as_user = function()
    t.assert_equals(utils.is_run_as_user_enabled(), true)
end

g.test_set_run_as_user_toggles = function()
    utils.set_run_as_user(false)

    t.assert_equals(utils.is_run_as_user_enabled(), false)

    utils.set_run_as_user(true)

    t.assert_equals(utils.is_run_as_user_enabled(), true)
end

g.test_set_requires_boolean = function()
    t.assert_error(function()
        utils.set_run_as_user(nil)
    end)
end

g.test_prepare_storage_call_with_run_as_user = function()
    local storage_func_name, func_args = utils.prepare_storage_call('get_on_storage', {1, 2})

    t.assert_equals(storage_func_name, '_crud.call_on_storage')
    t.assert_equals(func_args, {box.session.effective_user(), 'get_on_storage', 1, 2})
end

g.test_prepare_storage_call_without_run_as_user = function()
    utils.set_run_as_user(false)

    local storage_func_name, func_args = utils.prepare_storage_call('get_on_storage', {1, 2})

    t.assert_equals(storage_func_name, '_crud.call_on_storage_no_su')
    t.assert_equals(func_args, {'get_on_storage', 1, 2})
end
