local t = require('luatest')
local helpers = require('test.helper')

local pgroup = t.group('locate', helpers.backend_matrix({
    {engine = 'memtx'},
}))

pgroup.before_all(function(g)
    helpers.skip_if_tarantool3_crud_roles_unsupported()
    helpers.start_default_cluster(g, 'srv_select')
end)

pgroup.after_all(function(g)
    helpers.stop_cluster(g.cluster, g.params.backend)
end)

local function inject_mock_cooler(cluster, mock_script)
    for _, server in ipairs(cluster.servers) do
        server:exec(function(script)
            local mock_cooler = {}

            local func, err = load(script)
            if func == nil then
                error("Failed to load mock: " .. tostring(err))
            end

            mock_cooler.locate = func()
            package.loaded['cooler'] = mock_cooler
        end, {mock_script})
    end
end

local function unload_mock_cooler(cluster)
    for _, server in ipairs(cluster.servers) do
        server:exec(function()
            package.loaded['cooler'] = nil
        end)
    end
end

pgroup.test_locate_when_cooler_missing = function(g)
    unload_mock_cooler(g.cluster)

    local result, err = g.router:call('crud.locate', {'customers', 1})

    t.assert_equals(result, nil)
    t.assert_str_contains(err.err, "'locate' is not available")
end

pgroup.test_locate_non_existent_space = function(g)
    -- Inject a dummy mock just to bypass the module presence check
    inject_mock_cooler(g.cluster, [[
        return function(space_name, key, bucket_id) return 'memtx' end
    ]])

    local result, err = g.router:call('crud.locate', {'non_existent_space', 1})

    t.assert_equals(result, nil)
    t.assert_str_contains(err.err, "Space \"non_existent_space\" doesn't exist")
end

pgroup.test_locate_various_locations = function(g)
    inject_mock_cooler(g.cluster, [[
        return function(space_name, key, bucket_id)
            if key == 100 then
                return 'memtx'
            elseif key == 200 then
                return 'vinyl'
            elseif key == 300 then
                return nil
            elseif key == 400 then
                return nil, "Internal cooler error occurred"
            end
            return nil
        end
    ]])

    local res_memtx, err_memtx = g.router:call('crud.locate', {'customers', 100})
    t.assert_equals(err_memtx, nil)
    t.assert_equals(res_memtx, 'memtx')

    local res_vinyl, err_vinyl = g.router:call('crud.locate', {'customers', 200})
    t.assert_equals(err_vinyl, nil)
    t.assert_equals(res_vinyl, 'vinyl')

    local res_nil, err_nil = g.router:call('crud.locate', {'customers', 300})
    t.assert_equals(err_nil, nil)
    t.assert_equals(res_nil, nil)

    local res_err, err_msg = g.router:call('crud.locate', {'customers', 400})
    t.assert_equals(res_err, nil)
    t.assert_str_contains(err_msg.err, "Internal cooler error occurred")
end

pgroup.test_opts_not_damaged = function(g)
    inject_mock_cooler(g.cluster, [[
        return function(space_name, key, bucket_id) return 'memtx' end
    ]])

    local locate_opts = {timeout = 1, mode = 'read'}

    local new_locate_opts, err = g.router:exec(function(opts)
        local crud = require('crud')
        local _, err = crud.locate('customers', 100, opts)
        return opts, err
    end, {locate_opts})

    t.assert_equals(err, nil)
    t.assert_equals(new_locate_opts, locate_opts)
end
