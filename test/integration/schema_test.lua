local t = require('luatest')

local helpers = require('test.helper')

local pgroup = t.group('schema', helpers.backend_matrix({
    {engine = 'memtx'},
    {engine = 'vinyl'},
}))

pgroup.before_all(function(g)
    helpers.start_default_cluster(g, 'srv_schema')

    g.router = helpers.get_router(g.cluster, g.params.backend)
end)

pgroup.after_all(function(g)
    helpers.stop_cluster(g.cluster, g.params.backend)
end)

pgroup.after_each(function(g)
    helpers.call_on_servers(g.cluster, {'s1-master', 's2-master'}, function(server)
        server:call('reload_schema')
    end)

    local _, err = g.router:call('crud.schema')
    assert(err == nil)
end)

local schema = {
    customers = {
        format = {
            {name = "id", type = "unsigned"},
            {name = "bucket_id", type = "unsigned", is_nullable = true},
            {name = "name", type = "string"},
            {name = "age", type = "number"},
        },
        indexes = {
            [0] = {
                id = 0,
                name = "id",
                parts = {{exclude_null = false, fieldno = 1, is_nullable = false, type = "unsigned"}},
                type = "TREE",
                unique = true,
            },
        },
    },
    shops = {
        format = {
            {name = 'registry_id', type = 'unsigned'},
            {name = 'bucket_id', type = 'unsigned', is_nullable = true},
            {name = 'name', type = 'string'},
            {name = 'address', type = 'string'},
            {name = 'owner', type = 'string', is_nullable = true},
        },
        indexes = {
            [0] = {
                id = 0,
                name = "registry",
                parts = {{exclude_null = false, fieldno = 1, is_nullable = false, type = "unsigned"}},
                type = "TREE",
                unique = true,
            },
            [2] = {
                id = 2,
                name = "address",
                parts = {{exclude_null = false, fieldno = 4, is_nullable = false, type = "string"}},
                type = "TREE",
                unique = true,
            },
        },
    },
}

local function expected_schema()
    local sch = table.deepcopy(schema)
    return helpers.schema_compatibility(sch)
end

local function altered_schema()
    local sch = table.deepcopy(schema)

    sch['customers'].indexes[2] = {
        id = 2,
        name = "age",
        parts = {{exclude_null = false, fieldno = 4, is_nullable = false, type = "number"}},
        type = "TREE",
        unique = false,
    }

    sch['shops'].format[6] = {name = 'salary', type = 'unsigned', is_nullable = true}

    return helpers.schema_compatibility(sch)
end

pgroup.test_get_all = function(g)
    local result, err = g.router:call('crud.schema')

    t.assert_equals(err, nil)
    t.assert_equals(result, expected_schema())
end

pgroup.test_get_one = function(g)
    local result, err = g.router:call('crud.schema', {'customers'})

    t.assert_equals(err, nil)
    t.assert_equals(result, expected_schema()['customers'])
end

pgroup.test_get_non_existent_space = function(g)
    local result, err = g.router:call('crud.schema', {'owners'})

    t.assert_equals(result, nil, err)
    t.assert_str_contains(err.err, "Space \"owners\" doesn't exist")
end

pgroup.test_timeout_option = function(g)
    local _, err = g.router:call('crud.schema', {nil, {timeout = 2}})

    t.assert_equals(err, nil)
end

pgroup.test_schema_cached = function(g)
    helpers.call_on_servers(g.cluster, {'s1-master', 's2-master'}, function(server)
        server:call('alter_schema')
    end)

    local result_after, err = g.router:call('crud.schema', {nil, {cached = true}})
    t.assert_equals(err, nil)
    t.assert_equals(result_after, expected_schema())
end

pgroup.test_schema_reloaded = function(g)
    helpers.call_on_servers(g.cluster, {'s1-master', 's2-master'}, function(server)
        server:call('alter_schema')
    end)

    local result_after, err = g.router:call('crud.schema', {nil, {cached = false}})
    t.assert_equals(err, nil)
    t.assert_equals(result_after, altered_schema())
end
