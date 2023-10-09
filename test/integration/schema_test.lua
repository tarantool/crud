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

local function expected_schema()
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

    return helpers.schema_compatibility(schema)
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
