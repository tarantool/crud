local t = require('luatest')

local helpers = require('test.helper')

local fiber = require('fiber')
local crud = require('crud')
local crud_utils = require('crud.common.utils')

local pgroup = t.group('updated_schema', helpers.backend_matrix({
    {engine = 'memtx'},
    {engine = 'vinyl'},
}))

pgroup.before_all(function(g)
    helpers.start_default_cluster(g, 'srv_update_schema')
end)

pgroup.after_all(function(g)
    helpers.stop_cluster(g.cluster, g.params.backend)
end)

pgroup.before_each(function(g)
    helpers.drop_space_on_cluster(g.cluster, 'customers')
    -- force schema update on router
    g.cluster.main_server.net_box:eval([[
        local vshard = require('vshard')
        for _, replicaset in pairs(vshard.router.routeall()) do
            if replicaset.locate_master ~= nil then
                replicaset:locate_master()
            end

            local master = replicaset.master

            if not master.conn:ping({timeout = 3}) then
                return nil, FetchSchemaError:new(
                    "Failed to ping replicaset %s master (%s)",
                    replicaset.uuid,
                    master.uuid
                )
            end
        end
    ]])
end)

pgroup.test_insert_non_existent_space = function(g)
    -- non-existent space err
    local obj, err = g.cluster.main_server.net_box:call(
       'crud.insert', {'customers', {11, nil, 'XXX'}}
    )

    t.assert_equals(obj, nil)
    t.assert_is_not(err, nil)
    t.assert_str_contains(err.err, "Space \"customers\" doesn't exist")

    -- create space
    helpers.call_on_servers(g.cluster, {'s1-master', 's2-master'}, function(server)
        server.net_box:call('create_space')
        server.net_box:call('create_bucket_id_index')
    end)

    -- check that schema changes were applied
    local obj, err = g.cluster.main_server.net_box:call(
       'crud.insert', {'customers', {11, nil, 'XXX'}}
    )

    t.assert_is_not(obj, nil)
    t.assert_equals(err, nil)
end

pgroup.test_insert_object_non_existent_space = function(g)
    -- non-existent space err
    local obj, err = g.cluster.main_server.net_box:call(
       'crud.insert_object', {'customers', {id = 11, value = 'XXX'}}
    )

    t.assert_equals(obj, nil)
    t.assert_is_not(err, nil)
    t.assert_str_contains(err.err, "Space \"customers\" doesn't exist")

    -- create space
    helpers.call_on_servers(g.cluster, {'s1-master', 's2-master'}, function(server)
        server.net_box:call('create_space')
        server.net_box:call('create_bucket_id_index')
    end)

    -- check that schema changes were applied
    local obj, err = g.cluster.main_server.net_box:call(
       'crud.insert_object', {'customers', {id = 11, value = 'XXX'}}
    )

    t.assert_is_not(obj, nil)
    t.assert_equals(err, nil)
end

pgroup.test_get_non_existent_space = function(g)
    -- non-existent space err
    local obj, err = g.cluster.main_server.net_box:call(
       'crud.get', {'customers', 1}
    )

    t.assert_equals(obj, nil)
    t.assert_is_not(err, nil)
    t.assert_str_contains(err.err, "Space \"customers\" doesn't exist")

    -- create space
    helpers.call_on_servers(g.cluster, {'s1-master', 's2-master'}, function(server)
        server.net_box:call('create_space')
        server.net_box:call('create_bucket_id_index')
    end)

    -- check that schema changes were applied
    local obj, err = g.cluster.main_server.net_box:call(
        'crud.get', {'customers', 1}
    )

    t.assert_is_not(obj, nil)
    t.assert_equals(err, nil)
end

pgroup.test_delete_non_existent_space = function(g)
    -- non-existent space err
    local obj, err = g.cluster.main_server.net_box:call(
       'crud.delete', {'customers', 11}
    )

    t.assert_equals(obj, nil)
    t.assert_is_not(err, nil)
    t.assert_str_contains(err.err, "Space \"customers\" doesn't exist")

    -- create space
    helpers.call_on_servers(g.cluster, {'s1-master', 's2-master'}, function(server)
        server.net_box:call('create_space')
        server.net_box:call('create_bucket_id_index')
    end)

    -- check that schema changes were applied
    local obj, err = g.cluster.main_server.net_box:call(
        'crud.delete', {'customers', 11}
    )

    t.assert_is_not(obj, nil)
    t.assert_equals(err, nil)
end

pgroup.test_update_non_existent_space = function(g)
    -- non-existent space err
    local obj, err = g.cluster.main_server.net_box:call(
       'crud.update', {'customers', 11, {{'=', 'value', 'YYY'}}}
    )

    t.assert_equals(obj, nil)
    t.assert_is_not(err, nil)
    t.assert_str_contains(err.err, "Space \"customers\" doesn't exist")

    -- create space
    helpers.call_on_servers(g.cluster, {'s1-master', 's2-master'}, function(server)
        server.net_box:call('create_space')
        server.net_box:call('create_bucket_id_index')
    end)

    -- insert tuple
    local obj, err = g.cluster.main_server.net_box:call(
       'crud.insert', {'customers', {11, nil, 'XXX'}}
    )
    t.assert_is_not(obj, nil)
    t.assert_equals(err, nil)

    -- check that schema changes were applied
    local obj, err = g.cluster.main_server.net_box:call(
        'crud.update', {'customers', 11, {{'=', 'value', 'YYY'}}}
    )

    t.assert_is_not(obj, nil)
    t.assert_equals(err, nil)
end

pgroup.test_replace_non_existent_space = function(g)
    -- non-existent space err
    local obj, err = g.cluster.main_server.net_box:call(
        'crud.replace', {'customers', {11, nil, 'XXX'}}
    )

    t.assert_equals(obj, nil)
    t.assert_is_not(err, nil)
    t.assert_str_contains(err.err, "Space \"customers\" doesn't exist")

    -- create space
    helpers.call_on_servers(g.cluster, {'s1-master', 's2-master'}, function(server)
        server.net_box:call('create_space')
        server.net_box:call('create_bucket_id_index')
    end)

    -- check that schema changes were applied
    local obj, err = g.cluster.main_server.net_box:call(
        'crud.replace', {'customers', {11, nil, 'XXX'}}
    )

    t.assert_is_not(obj, nil)
    t.assert_equals(err, nil)
end

pgroup.test_replace_object_non_existent_space = function(g)
    -- non-existent space err
    local obj, err = g.cluster.main_server.net_box:call(
        'crud.replace_object', {'customers', {id = 11, value = 'XXX'}}
    )

    t.assert_equals(obj, nil)
    t.assert_is_not(err, nil)
    t.assert_str_contains(err.err, "Space \"customers\" doesn't exist")

    -- create space
    helpers.call_on_servers(g.cluster, {'s1-master', 's2-master'}, function(server)
        server.net_box:call('create_space')
        server.net_box:call('create_bucket_id_index')
    end)

    -- check that schema changes were applied
    local obj, err = g.cluster.main_server.net_box:call(
        'crud.replace_object', {'customers', {id = 11, value = 'XXX'}}
    )

    t.assert_is_not(obj, nil)
    t.assert_equals(err, nil)
end

pgroup.test_upsert_non_existent_space = function(g)
    -- non-existent space err
    local obj, err = g.cluster.main_server.net_box:call(
        'crud.upsert', {'customers', {11, nil, 'XXX'}, {{'=', 'value', 'YYY'}}}
    )

    t.assert_equals(obj, nil)
    t.assert_is_not(err, nil)
    t.assert_str_contains(err.err, "Space \"customers\" doesn't exist")

    -- create space
    helpers.call_on_servers(g.cluster, {'s1-master', 's2-master'}, function(server)
        server.net_box:call('create_space')
        server.net_box:call('create_bucket_id_index')
    end)

    -- check that schema changes were applied
    local obj, err = g.cluster.main_server.net_box:call(
        'crud.upsert', {'customers', {11, nil, 'XXX'}, {{'=', 'value', 'YYY'}}}
    )

    t.assert_is_not(obj, nil)
    t.assert_equals(err, nil)
end

pgroup.test_upsert_object_non_existent_space = function(g)
    -- non-existent space err
    local obj, err = g.cluster.main_server.net_box:call(
        'crud.upsert_object', {'customers', {id = 11, value = 'XXX'}, {{'=', 'value', 'YYY'}}}
    )

    t.assert_equals(obj, nil)
    t.assert_is_not(err, nil)
    t.assert_str_contains(err.err, "Space \"customers\" doesn't exist")

    -- create space
    helpers.call_on_servers(g.cluster, {'s1-master', 's2-master'}, function(server)
        server.net_box:call('create_space')
        server.net_box:call('create_bucket_id_index')
    end)

    -- check that schema changes were applied
    local obj, err = g.cluster.main_server.net_box:call(
        'crud.upsert_object', {'customers', {id = 11, value = 'XXX'}, {{'=', 'value', 'YYY'}}}
    )

    t.assert_is_not(obj, nil)
    t.assert_equals(err, nil)
end

pgroup.test_select_non_existent_space = function(g)
    -- non-existent space err
    local obj, err = g.cluster.main_server.net_box:call(
        'crud.select', {'customers', nil, {fullscan = true}}
    )

    t.assert_equals(obj, nil)
    t.assert_is_not(err, nil)
    t.assert_str_contains(err.err, "Space \"customers\" doesn't exist")

    -- create space
    helpers.call_on_servers(g.cluster, {'s1-master', 's2-master'}, function(server)
        server.net_box:call('create_space')
        server.net_box:call('create_bucket_id_index')
    end)

    -- check that schema changes were applied
    local obj, err = g.cluster.main_server.net_box:call(
        'crud.select', {'customers', nil, {fullscan = true}}
    )

    t.assert_is_not(obj, nil)
    t.assert_equals(err, nil)
end

pgroup.test_borders_non_existent_space = function(g)
    for _, border_func_name in ipairs({'crud.max', 'crud.min'}) do
        -- non-existent space err
        local obj, err = g.cluster.main_server.net_box:call(
            border_func_name, {'customers'}
        )

        t.assert_equals(obj, nil)
        t.assert_is_not(err, nil)
        t.assert_str_contains(err.err, "Space \"customers\" doesn't exist")
    end

    -- create space
    helpers.call_on_servers(g.cluster, {'s1-master', 's2-master'}, function(server)
        server.net_box:call('create_space')
        server.net_box:call('create_bucket_id_index')
    end)

    for _, border_func_name in ipairs({'crud.max', 'crud.min'}) do
        -- check that schema changes were applied
        local obj, err = g.cluster.main_server.net_box:call(
            border_func_name, {'customers'}
        )

        t.assert_is_not(obj, nil)
        t.assert_equals(err, nil)
    end
end

pgroup.test_insert_no_bucket_id_index = function(g)
    -- create space w/o bucket_id index
    helpers.call_on_servers(g.cluster, {'s1-master', 's2-master'}, function(server)
        server.net_box:call('create_space')
    end)

    -- no bucket ID index error
    local obj, err = g.cluster.main_server.net_box:call(
        'crud.insert', {'customers', {11, nil, 'XXX'}}
    )

    t.assert_equals(obj, nil)
    t.assert_is_not(err, nil)
    t.assert_str_contains(err.err, "\"bucket_id\" index is not found")

    -- create bucket_id index
    helpers.call_on_servers(g.cluster, {'s1-master', 's2-master'}, function(server)
        server.net_box:call('create_bucket_id_index')
    end)

    -- check that schema changes were applied
    local obj, err = g.cluster.main_server.net_box:call(
       'crud.insert', {'customers', {11, nil, 'XXX'}}
    )

    t.assert_is_not(obj, nil)
    t.assert_equals(err, nil)
end

pgroup.test_replace_no_bucket_id_index = function(g)
    -- create space w/o bucket_id index
    helpers.call_on_servers(g.cluster, {'s1-master', 's2-master'}, function(server)
        server.net_box:call('create_space')
    end)

    -- no bucket ID index error
    local obj, err = g.cluster.main_server.net_box:call(
        'crud.replace', {'customers', {11, nil, 'XXX'}}
    )

    t.assert_equals(obj, nil)
    t.assert_is_not(err, nil)
    t.assert_str_contains(err.err, "\"bucket_id\" index is not found")

    -- create bucket_id index
    helpers.call_on_servers(g.cluster, {'s1-master', 's2-master'}, function(server)
        server.net_box:call('create_bucket_id_index')
    end)

    -- check that schema changes were applied
    local obj, err = g.cluster.main_server.net_box:call(
        'crud.replace', {'customers', {11, nil, 'XXX'}}
    )

    t.assert_is_not(obj, nil)
    t.assert_equals(err, nil)
end

pgroup.test_upsert_no_bucket_id_index = function(g)
    -- create space w/o bucket_id index
    helpers.call_on_servers(g.cluster, {'s1-master', 's2-master'}, function(server)
        server.net_box:call('create_space')
    end)

    -- no bucket ID index error
    local obj, err = g.cluster.main_server.net_box:call(
        'crud.upsert', {'customers', {11, nil, 'XXX'}, {{'=', 'value', 'YYY'}}}
    )

    t.assert_equals(obj, nil)
    t.assert_is_not(err, nil)
    t.assert_str_contains(err.err, "\"bucket_id\" index is not found")

    -- create bucket_id index
    helpers.call_on_servers(g.cluster, {'s1-master', 's2-master'}, function(server)
        server.net_box:call('create_bucket_id_index')
    end)

    -- check that schema changes were applied
    local obj, err = g.cluster.main_server.net_box:call(
        'crud.upsert', {'customers', {11, nil, 'XXX'}, {{'=', 'value', 'YYY'}}}
    )

    t.assert_is_not(obj, nil)
    t.assert_equals(err, nil)
end

pgroup.test_insert_field_type_changed = function(g)
    -- create space w/ bucket_id index
    helpers.call_on_servers(g.cluster, {'s1-master', 's2-master'}, function(server)
        server.net_box:call('create_space')
        server.net_box:call('create_bucket_id_index')
    end)

    -- value should be string error
    local obj, err = g.cluster.main_server.net_box:call(
        'crud.insert', {'customers', {11, nil, 123}}
    )

    t.assert_equals(obj, nil)
    t.assert_is_not(err, nil)
    t.assert_str_contains(err.err, "type does not match one required by operation: expected string")

    -- set value type to unsigned
    helpers.call_on_servers(g.cluster, {'s1-master', 's2-master'}, function(server)
        server.net_box:call('set_value_type_to_unsigned')
    end)

    -- check that schema changes were applied
    -- insert value unsigned - OK
    local obj, err = g.cluster.main_server.net_box:call(
       'crud.insert', {'customers', {11, nil, 123}}
    )

    t.assert_is_not(obj, nil)
    t.assert_equals(err, nil)
end

pgroup.test_replace_field_type_changed = function(g)
    -- create space w/ bucket_id index
    helpers.call_on_servers(g.cluster, {'s1-master', 's2-master'}, function(server)
        server.net_box:call('create_space')
        server.net_box:call('create_bucket_id_index')
    end)

    -- value should be string error
    local obj, err = g.cluster.main_server.net_box:call(
        'crud.replace', {'customers', {11, nil, 123}}
    )

    t.assert_equals(obj, nil)
    t.assert_is_not(err, nil)
    t.assert_str_contains(err.err, "type does not match one required by operation: expected string")

    -- set value type to unsigned
    helpers.call_on_servers(g.cluster, {'s1-master', 's2-master'}, function(server)
        server.net_box:call('set_value_type_to_unsigned')
    end)

    -- check that schema changes were applied
    -- insert value unsigned - OK
    local obj, err = g.cluster.main_server.net_box:call(
        'crud.replace', {'customers', {11, nil, 123}}
    )

    t.assert_is_not(obj, nil)
    t.assert_equals(err, nil)
end

pgroup.test_upsert_field_type_changed = function(g)
    -- create space w/ bucket_id index
    helpers.call_on_servers(g.cluster, {'s1-master', 's2-master'}, function(server)
        server.net_box:call('create_space')
        server.net_box:call('create_bucket_id_index')
    end)

    -- value should be string error
    local obj, err = g.cluster.main_server.net_box:call(
        'crud.upsert', {'customers', {11, nil, 123}, {{'=', 'value', 456}}}
    )

    t.assert_equals(obj, nil)
    t.assert_is_not(err, nil)
    t.assert_str_contains(err.err, "type does not match one required by operation: expected string")

    -- set value type to unsigned
    helpers.call_on_servers(g.cluster, {'s1-master', 's2-master'}, function(server)
        server.net_box:call('set_value_type_to_unsigned')
    end)

    -- check that schema changes were applied
    -- insert value unsigned - OK
    local obj, err = g.cluster.main_server.net_box:call(
        'crud.upsert', {'customers', {11, nil, 123}, {{'=', 'value', 456}}}
    )

    t.assert_is_not(obj, nil)
    t.assert_equals(err, nil)
end

pgroup.test_update_field_added = function(g)
    -- create space w/ bucket_id index and insert tuple
    helpers.call_on_servers(g.cluster, {'s1-master', 's2-master'}, function(server)
        server.net_box:call('create_space')
        server.net_box:call('create_bucket_id_index')
    end)

    local obj, err = g.cluster.main_server.net_box:call(
        'crud.insert', {'customers', {11, nil, 'XXX'}}
    )
    t.assert_is_not(obj, nil)
    t.assert_equals(err, nil)

    -- unknown field error
    local obj, err = g.cluster.main_server.net_box:call(
       'crud.update', {'customers', 11, {{'=', 'extra', 'EXTRRRRA'}}}
    )

    t.assert_equals(obj, nil)
    t.assert_is_not(err, nil)

    if not crud_utils.tarantool_supports_fieldpaths() then
        t.assert_str_contains(err.err, "Space format doesn't contain field named \"extra\"")
    else
        t.assert_str_contains(err.err, "Field 'extra' was not found in the tuple")
    end

    -- add extra field
    helpers.call_on_servers(g.cluster, {'s1-master', 's2-master'}, function(server)
        server.net_box:call('add_extra_field')
    end)

    -- check that schema changes were applied
    local obj, err = g.cluster.main_server.net_box:call(
        'crud.update', {'customers', 11, {{'=', 'extra', 'EXTRRRRA'}}}
    )

    t.assert_is_not(obj, nil)
    t.assert_equals(err, nil)
end

pgroup.test_upsert_field_added = function(g)
    -- create space w/ bucket_id index and insert tuple
    helpers.call_on_servers(g.cluster, {'s1-master', 's2-master'}, function(server)
        server.net_box:call('create_space')
        server.net_box:call('create_bucket_id_index')
    end)

    -- unknown field error
    local obj, err = g.cluster.main_server.net_box:call(
        'crud.upsert', {'customers', {11, nil, 'XXX'}, {{'=', 'extra', 'EXTRRRRA'}}}
    )

    t.assert_equals(obj, nil)
    t.assert_is_not(err, nil)

    if not crud_utils.tarantool_supports_fieldpaths() then
        t.assert_str_contains(err.err, "Space format doesn't contain field named \"extra\"")
    else
        t.assert_str_contains(err.err, "Field 'extra' was not found in the tuple")
    end

    -- add extra field
    helpers.call_on_servers(g.cluster, {'s1-master', 's2-master'}, function(server)
        server.net_box:call('add_extra_field')
    end)

    -- check that schema changes were applied
    local obj, err = g.cluster.main_server.net_box:call(
        'crud.upsert', {'customers', {11, nil, 'XXX'}, {{'=', 'extra', 'EXTRRRRA'}}}
    )

    t.assert_is_not(obj, nil)
    t.assert_equals(err, nil)
end

pgroup.test_select_field_added = function(g)
    -- create space w/ bucket_id index and insert tuple
    helpers.call_on_servers(g.cluster, {'s1-master', 's2-master'}, function(server)
        server.net_box:call('create_space')
        server.net_box:call('create_bucket_id_index')
    end)

    -- unknown field (no results)
    local obj, err = g.cluster.main_server.net_box:call(
        'crud.select', {'customers', {{'==', 'extra', 'EXTRRRRA'}}, {fullscan = true}}
    )

    t.assert_equals(obj.rows, {})
    t.assert_equals(err, nil)

    -- add extra field
    helpers.call_on_servers(g.cluster, {'s1-master', 's2-master'}, function(server)
        server.net_box:call('add_extra_field')
    end)

    -- check that schema changes were applied
    local obj, err = g.cluster.main_server.net_box:call(
        'crud.select', {'customers', {{'==', 'extra', 'EXTRRRRA'}}, {fullscan = true}}
    )

    t.assert_is_not(obj, nil)
    t.assert_equals(err, nil)
end

pgroup.test_insert_object_field_type_changed = function(g)
    -- create space w/ bucket_id index
    helpers.call_on_servers(g.cluster, {'s1-master', 's2-master'}, function(server)
        server.net_box:call('create_space')
        server.net_box:call('create_bucket_id_index')
    end)

    -- value should be string error
    local obj, err = g.cluster.main_server.net_box:call(
        'crud.insert_object', {'customers', {id = 11, value = 123}}
    )

    t.assert_equals(obj, nil)
    t.assert_is_not(err, nil)
    t.assert_str_contains(err.err, "type does not match one required by operation: expected string")

    -- set value type to unsigned
    helpers.call_on_servers(g.cluster, {'s1-master', 's2-master'}, function(server)
        server.net_box:call('set_value_type_to_unsigned')
    end)

    -- check that schema changes were applied
    -- insert value unsigned - OK
    local obj, err = g.cluster.main_server.net_box:call(
       'crud.insert_object', {'customers', {id = 11, value = 123}}
    )

    t.assert_is_not(obj, nil)
    t.assert_equals(err, nil)
end

pgroup.test_replace_object_field_type_changed = function(g)
    -- create space w/ bucket_id index
    helpers.call_on_servers(g.cluster, {'s1-master', 's2-master'}, function(server)
        server.net_box:call('create_space')
        server.net_box:call('create_bucket_id_index')
    end)

    -- value should be string error
    local obj, err = g.cluster.main_server.net_box:call(
        'crud.replace_object', {'customers', {id = 11, value = 123}}
    )

    t.assert_equals(obj, nil)
    t.assert_is_not(err, nil)
    t.assert_str_contains(err.err, "type does not match one required by operation: expected string")

    -- set value type to unsigned
    helpers.call_on_servers(g.cluster, {'s1-master', 's2-master'}, function(server)
        server.net_box:call('set_value_type_to_unsigned')
    end)

    -- check that schema changes were applied
    -- insert value unsigned - OK
    local obj, err = g.cluster.main_server.net_box:call(
       'crud.replace_object', {'customers', {id = 11, value = 123}}
    )

    t.assert_is_not(obj, nil)
    t.assert_equals(err, nil)
end

pgroup.test_upsert_object_field_type_changed = function(g)
    -- create space w/ bucket_id index
    helpers.call_on_servers(g.cluster, {'s1-master', 's2-master'}, function(server)
        server.net_box:call('create_space')
        server.net_box:call('create_bucket_id_index')
    end)

    -- value should be string error
    local obj, err = g.cluster.main_server.net_box:call(
        'crud.upsert_object', {'customers', {id = 11, value = 123}, {}}
    )

    t.assert_equals(obj, nil)
    t.assert_is_not(err, nil)
    t.assert_str_contains(err.err, "type does not match one required by operation: expected string")

    -- set value type to unsigned
    helpers.call_on_servers(g.cluster, {'s1-master', 's2-master'}, function(server)
        server.net_box:call('set_value_type_to_unsigned')
    end)

    -- check that schema changes were applied
    -- insert value unsigned - OK
    local obj, err = g.cluster.main_server.net_box:call(
        'crud.upsert_object', {'customers', {id = 11, value = 123}, {}}
    )

    t.assert_is_not(obj, nil)
    t.assert_equals(err, nil)
end

pgroup.test_borders_value_index_added = function(g)
    -- create space w/ bucket_id index
    helpers.call_on_servers(g.cluster, {'s1-master', 's2-master'}, function(server)
        server.net_box:call('create_space')
        server.net_box:call('create_bucket_id_index')
    end)

    for _, border_func_name in ipairs({'crud.max', 'crud.min'}) do
        -- non-existent space err
        local obj, err = g.cluster.main_server.net_box:call(border_func_name, {
            'customers',
            'value_index',
            {mode = 'write'},
        })

        t.assert_equals(obj, nil)
        t.assert_is_not(err, nil)
        t.assert_str_contains(err.err, "Index \"value_index\" of space \"customers\" doesn't exist")
    end

    -- create value_index index
    helpers.call_on_servers(g.cluster, {'s1-master', 's2-master'}, function(server)
        server.net_box:call('add_value_index')
    end)

    for _, border_func_name in ipairs({'crud.max', 'crud.min'}) do
        -- check that schema changes were applied
        local obj, err = g.cluster.main_server.net_box:call(border_func_name, {
            'customers',
            'value_index',
            {mode = 'write'},
        })

        t.assert_is_not(obj, nil)
        t.assert_equals(err, nil)
    end
end

pgroup.test_alter_index_parts = function(g)
    -- create space w/ bucket_id index
    helpers.call_on_servers(g.cluster, {'s1-master', 's2-master'}, function(server)
        server.net_box:call('create_space')
        server.net_box:call('create_bucket_id_index')
        server.net_box:call('create_number_value_index')
    end)

    for i = 0, 9 do
        -- Insert {0, 9}, {1, 8}, ..., {9, 0} paris in index
        local _, err = g.cluster.main_server.net_box:call(
                'crud.replace', {'customers', {i, nil, tostring(i), 9 - i}})
        t.assert_equals(err, nil)
    end

    -- Check sort order before alter
    local result, err = g.cluster.main_server.net_box:call('crud.select', {
        'customers',
        {{'>=', 'number_value_index', {0, "0"}}},
        {fullscan = true, mode = 'write'},
    })
    t.assert_equals(err, nil)
    t.assert_equals(#result.rows, 10)

    local objects = crud.unflatten_rows(result.rows, result.metadata)
    for i = 0, 9 do
        t.assert_equals(objects[i + 1].number, i)
        t.assert_equals(objects[i + 1].value, tostring(9 - i))
    end

    -- Alter index (lead to index rebuild - change parts order)
    helpers.call_on_servers(g.cluster, {'s1-master', 's2-master'}, function(server)
        server.net_box:call('alter_number_value_index')
    end)

    -- Wait for index rebuild and schema update
    fiber.sleep(1)

    -- Sort order should be new
    local result, err = g.cluster.main_server.net_box:call('crud.select', {
        'customers',
        {{'>=', 'number_value_index', {"0", 0}}},
        {fullscan = true, mode = 'write'},
    })
    t.assert_equals(err, nil)
    t.assert_equals(#result.rows, 10)

    local objects = crud.unflatten_rows(result.rows, result.metadata)
    for i = 0, 9 do
        t.assert_equals(objects[i + 1].number, 9 - i)
        t.assert_equals(objects[i + 1].value, tostring(i))
    end
end
