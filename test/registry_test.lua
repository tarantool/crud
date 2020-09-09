local t = require('luatest')
local g = t.group('registry')

local registry = require('elect.registry')

local global_func_names = {}

g.before_each = function()
    registry.clean()
    for _, func_name in pairs(global_func_names) do
        rawset(_G, func_name, nil)
    end
end

g.test_register = function()
    local res, err

    local funcs = {
        say_hi = function(name) return string.format("Hi, %s", name) end,
        say_bye = function(name) return string.format("Bye, %s", name) end,
    }

    for func_name in pairs(funcs) do
        t.assert_equals(registry.is_registered(func_name), false)
        t.assert_equals(registry.get(func_name), nil)
    end

    res, err = registry.add(funcs)
    t.assert_equals(res, true, err)

    for func_name, func in pairs(funcs) do
        t.assert_equals(registry.is_registered(func_name), true)
        t.assert_equals(registry.get(func_name), func)
    end

    registry.clean()

    for func_name in pairs(funcs) do
        t.assert_equals(registry.is_registered(func_name), false)
        t.assert_equals(registry.get(func_name), nil)
    end
end

g.test_already_registered = function()
    local res, err

    local funcs = {
        say_hi = function(name) return string.format("Hi, %s", name) end,
        say_bye = function(name) return string.format("Bye, %s", name) end,
    }

    res, err = registry.add({
        say_hi = funcs['say_hi'],
    })
    t.assert_equals(res, true, err)

    res, err = registry.add(funcs)
    t.assert_equals(res, nil)
    t.assert_str_contains(err.err, "Function say_hi is already registered")

    t.assert_equals(registry.is_registered('say_bye'), false)
    t.assert_equals(registry.get('say_bye'), nil)
end
