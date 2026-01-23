local t = require('luatest')
local helpers = require('test.helper')

local pgroup = t.group('schema_reload_deadlock', helpers.backend_matrix({
    { engine = 'memtx' },
}))

pgroup.before_all(function(g)
    helpers.start_default_cluster(g, 'srv_select')
end)

pgroup.after_all(function(g)
    helpers.stop_cluster(g.cluster, g.params.backend)
end)

pgroup.before_each(function(g)
    g._old_reload_schema_timeout = g.router:exec(function(new_timeout)
        local const = require('crud.common.const')
        local old = const.RELOAD_SCHEMA_TIMEOUT
        const.RELOAD_SCHEMA_TIMEOUT = new_timeout
        return old
    end, { 0.2 })
end)

pgroup.after_each(function(g)
    if g._old_reload_schema_timeout ~= nil then
        g.router:exec(function(prev_timeout)
            local const = require('crud.common.const')
            const.RELOAD_SCHEMA_TIMEOUT = prev_timeout
        end, { g._old_reload_schema_timeout })

        g._old_reload_schema_timeout = nil
    end
end)

pgroup.test_schema_reload_in_progress_is_cleared_after_error = function(g)
    local _, err = g.router:call('crud.schema')
    t.assert_equals(err, nil)

    t.assert_is_not(g.cluster:server('s2-master'), nil)
    g.cluster:server('s2-master'):stop()

    local _, err1 = g.router:call('crud.schema')
    t.assert_is_not(err1, nil)
    t.assert_str_contains(err1.err, 'timed out')

    g.cluster:server('s2-master'):start()

    t.helpers.retrying({ timeout = 30, delay = 0.1 }, function()
        local _, err2 = g.router:call('crud.schema')
        t.assert_equals(err2, nil)
    end)
end
