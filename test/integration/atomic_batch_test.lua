local t = require('luatest')
local crud = require('crud')

local helpers = require('test.helper')

local ID_SCAN_LIMIT = 10000

-- Main matrix: both supported engines for the regular batch entrypoint.
local pgroup = t.group('atomic_batch', helpers.backend_matrix({
    {engine = 'memtx'},
    {engine = 'vinyl'},
}))

-- Dedicated matrix for mixed memtx+vinyl transactional checks under vshard.
local mvcc_group = t.group('atomic_batch_mvcc_vshard', {
    {
        backend = helpers.backend.VSHARD,
        backend_cfg = nil,
        memtx_use_mvcc_engine = false,
    },
    {
        backend = helpers.backend.VSHARD,
        backend_cfg = nil,
        memtx_use_mvcc_engine = true,
    },
})

pgroup.before_all(function(g)
    helpers.start_default_cluster(g, 'srv_batch_operations')
end)

pgroup.after_all(function(g)
    helpers.stop_cluster(g.cluster, g.params.backend)
end)

pgroup.before_each(function(g)
    helpers.truncate_space_on_cluster(g.cluster, 'customers')
    helpers.truncate_space_on_cluster(g.cluster, 'developers')
    helpers.truncate_space_on_cluster(g.cluster, 'customers_sharded_by_age')
end)

mvcc_group.before_all(function(g)
    local vshard_cfg = helpers.build_default_vshard_cfg('srv_atomic_batch_mixed')
    vshard_cfg.memtx_use_mvcc_engine = g.params.memtx_use_mvcc_engine

    helpers.start_cluster(g, nil, vshard_cfg, nil, {
        backend = g.params.backend,
    })
end)

mvcc_group.after_all(function(g)
    helpers.stop_cluster(g.cluster, g.params.backend)
end)

mvcc_group.before_each(function(g)
    helpers.truncate_space_on_cluster(g.cluster, 'customers_memtx')
    helpers.truncate_space_on_cluster(g.cluster, 'customers_vinyl')
end)

local function assert_error_contains(err, expected)
    t.assert_not_equals(err, nil)
    local message = err.err or err.message or tostring(err)
    t.assert_str_contains(message, expected)
end

-- Kept local on purpose: most assertions are clearer in object form (field names).
local function get_single_object(g, space_name, id)
    local res, err = g.router:call('crud.get', {space_name, {id}, {mode = 'write'}})
    t.assert_equals(err, nil)

    local objects, unflatten_err = crud.unflatten_rows(res.rows, res.metadata)
    t.assert_equals(unflatten_err, nil)
    t.assert_equals(#objects, 1)
    return objects[1]
end

local function assert_absent_by_id(g, space_name, id)
    local res, err = g.router:call('crud.get', {space_name, {id}, {mode = 'write'}})
    t.assert_equals(err, nil)
    t.assert_equals(#res.rows, 0)
end

local function scan_ids_by_replicaset(g)
    local ids_by_uuid = g.router:eval([[
        local vshard = require('vshard')

        local by_uuid = {}
        for id = 1, ... do
            local bucket_id = vshard.router.bucket_id_strcrc32(id)
            local rs, err = vshard.router.route(bucket_id)
            if err == nil and rs ~= nil then
                local uuid = rs.uuid or rs.id or tostring(rs)
                by_uuid[uuid] = by_uuid[uuid] or {}
                table.insert(by_uuid[uuid], {id = id, bucket_id = bucket_id})
            end
        end

        return by_uuid
    ]], {ID_SCAN_LIMIT})

    t.assert_type(ids_by_uuid, 'table')
    return ids_by_uuid
end
local function find_ids_same_replicaset(g, count)
    local ids_by_uuid = scan_ids_by_replicaset(g)

    for _, ids in pairs(ids_by_uuid) do
        if #ids >= count then
            local result = {}
            for i = 1, count do
                result[i] = ids[i].id
            end
            return result
        end
    end

    t.fail(('failed to find %d ids on same replicaset within first %d ids'):format(count, ID_SCAN_LIMIT))
end

local function find_two_ids_same_replicaset(g)
    local ids = find_ids_same_replicaset(g, 2)
    return ids[1], ids[2]
end

local function find_two_ids_different_replicasets(g)
    local ids_by_uuid = scan_ids_by_replicaset(g)

    local first_id = nil
    for _, ids in pairs(ids_by_uuid) do
        if #ids > 0 then
            if first_id == nil then
                first_id = ids[1].id
            else
                return first_id, ids[1].id
            end
        end
    end

    t.fail(('failed to find ids on different replicasets within first %d ids'):format(ID_SCAN_LIMIT))
end

local function call_mixed_engine_atomic_batch(g, id1, id2)
    return g.router:call('crud.atomic_batch', {{
        {
            type = 'insert',
            space = 'customers_memtx',
            tuple = {id1, box.NULL, 'memtx_row', 31},
        },
        {
            type = 'insert',
            space = 'customers_vinyl',
            tuple = {id2, box.NULL, 'vinyl_row', 32},
        },
    }})
end

local function assert_mixed_mvcc_result(g, id1, id2, res, err)
    if g.params.memtx_use_mvcc_engine then
        t.assert_equals(err, nil)
        t.assert_type(res, 'table')

        local memtx_obj = get_single_object(g, 'customers_memtx', id1)
        local vinyl_obj = get_single_object(g, 'customers_vinyl', id2)
        t.assert_equals(memtx_obj.name, 'memtx_row')
        t.assert_equals(vinyl_obj.name, 'vinyl_row')
        return
    end

    t.assert_equals(res, nil)
    assert_error_contains(err, 'requires MVCC')
    assert_absent_by_id(g, 'customers_memtx', id1)
    assert_absent_by_id(g, 'customers_vinyl', id2)
end

-- -----------------------------------------------------------------------------
-- Happy path
-- -----------------------------------------------------------------------------

pgroup.test_empty_batch = function(g)
    local res, err = g.router:call('crud.atomic_batch', { { } })

    t.assert_equals(err, nil)
    t.assert_equals(res, {metadata = {}, data = {}})
end

pgroup.test_success_heterogeneous_batch = function(g)
    local res, err = g.router:call('crud.atomic_batch', {{
        {
            type = 'insert',
            space = 'customers',
            tuple = {1001, box.NULL, 'alice', 30},
        },
        {
            type = 'insert',
            space = 'developers',
            object = {id = 1001, name = 'alice_dev', login = 'alice_login'},
        },
        {
            type = 'get',
            space = 'customers',
            key = {1001},
        },
        {
            type = 'update',
            space = 'developers',
            key = {1001},
            operations = {{'=', 'name', 'alice_dev_updated'}},
        },
    }})

    t.assert_equals(err, nil)
    t.assert_type(res, 'table')
    t.assert_equals(#res.data, 4)

    local customer = get_single_object(g, 'customers', 1001)
    t.assert_equals(customer.name, 'alice')
    t.assert_equals(customer.age, 30)

    local developer = get_single_object(g, 'developers', 1001)
    t.assert_equals(developer.name, 'alice_dev_updated')
    t.assert_equals(developer.login, 'alice_login')
end

pgroup.test_read_your_own_writes_in_single_batch = function(g)
    local res, err = g.router:call('crud.atomic_batch', {{
        {
            type = 'insert',
            space = 'customers',
            tuple = {6001, box.NULL, 'read_own_writes', 20},
        },
        {
            type = 'update',
            space = 'customers',
            key = {6001},
            operations = {{'+', 'age', 5}},
        },
        {
            type = 'get',
            space = 'customers',
            key = {6001},
        },
    }})

    t.assert_equals(err, nil)
    t.assert_type(res, 'table')
    t.assert_equals(#res.data, 3)
    t.assert_equals(res.data[3][1], 6001)
    t.assert_equals(res.data[3][4], 25)

    local customer = get_single_object(g, 'customers', 6001)
    t.assert_equals(customer.age, 25)
end

pgroup.test_all_operation_types_in_single_batch = function(g)
    local ids = find_ids_same_replicaset(g, 3)
    local id1, id2, id3 = ids[1], ids[2], ids[3]

    helpers.insert_objects(g, 'customers', {{
        id = id1,
        name = 'seed_customer',
        age = 20,
    }})

    local res, err = g.router:call('crud.atomic_batch', {{
        {
            type = 'get',
            space = 'customers',
            key = {id1},
        },
        {
            type = 'insert',
            space = 'customers',
            tuple = {id2, box.NULL, 'inserted_customer', 31},
        },
        {
            type = 'replace',
            space = 'customers',
            tuple = {id2, box.NULL, 'replaced_customer', 32},
        },
        {
            type = 'update',
            space = 'customers',
            key = {id1},
            operations = {{'+', 'age', 2}},
        },
        {
            type = 'upsert',
            space = 'customers',
            tuple = {id3, box.NULL, 'upserted_customer', 40},
            operations = {{'=', 'name', 'upserted_customer_updated'}},
        },
        {
            type = 'delete',
            space = 'customers',
            key = {id3},
        },
    }})

    t.assert_equals(err, nil)
    t.assert_type(res, 'table')

    local customer_1 = get_single_object(g, 'customers', id1)
    t.assert_equals(customer_1.age, 22)

    local customer_2 = get_single_object(g, 'customers', id2)
    t.assert_equals(customer_2.name, 'replaced_customer')
    t.assert_equals(customer_2.age, 32)

    assert_absent_by_id(g, 'customers', id3)
end

-- -----------------------------------------------------------------------------
-- Options
-- -----------------------------------------------------------------------------

pgroup.test_noreturn = function(g)
    local res, err = g.router:call('crud.atomic_batch', {{
        {
            type = 'insert',
            space = 'customers',
            tuple = {4001, box.NULL, 'no_return_customer', 40},
        },
        {
            type = 'insert',
            space = 'developers',
            tuple = {4001, box.NULL, 'no_return_developer', 'no_return_login'},
        },
    }, {
        noreturn = true,
    }})

    t.assert_equals(err, nil)
    t.assert_equals(res, nil)

    local customer = get_single_object(g, 'customers', 4001)
    t.assert_equals(customer.name, 'no_return_customer')

    local developer = get_single_object(g, 'developers', 4001)
    t.assert_equals(developer.login, 'no_return_login')
end

pgroup.test_fields_projection = function(g)
    local res, err = g.router:call('crud.atomic_batch', {{
        {
            type = 'insert',
            space = 'customers',
            tuple = {5001, box.NULL, 'bob', 44},
        },
        {
            type = 'insert',
            space = 'developers',
            tuple = {5001, box.NULL, 'bob_dev', 'bob_login'},
        },
    }, {
        fields = {
            customers = {'id', 'name'},
            developers = {'id', 'login'},
        },
    }})

    t.assert_equals(err, nil)
    t.assert_type(res, 'table')

    t.assert_equals(res.metadata.customers, {
        {name = 'id', type = 'unsigned'},
        {name = 'name', type = 'string'},
    })
    t.assert_equals(res.metadata.developers, {
        {name = 'id', type = 'unsigned'},
        {name = 'login', type = 'string'},
    })

    t.assert_equals(res.data[1], {5001, 'bob'})
    t.assert_equals(res.data[2], {5001, 'bob_login'})
end

-- -----------------------------------------------------------------------------
-- Rollback and validation errors
-- -----------------------------------------------------------------------------

pgroup.test_rollback_on_mid_batch_error = function(g)
    helpers.insert_objects(g, 'developers', {{
        id = 2001,
        name = 'existing_developer',
        login = 'duplicate_login',
    }})

    local res, err = g.router:call('crud.atomic_batch', {{
        {
            type = 'insert',
            space = 'customers',
            tuple = {2001, box.NULL, 'rollback_customer', 18},
        },
        {
            type = 'insert',
            space = 'developers',
            tuple = {3001, box.NULL, 'conflicting_developer', 'duplicate_login'},
        },
        {
            type = 'update',
            space = 'customers',
            key = {2001},
            operations = {{'+', 'age', 1}},
        },
    }})

    t.assert_equals(res, nil)
    assert_error_contains(err, 'Operation #2')
    t.assert_equals(err.operation_index, 2)
    t.assert_equals(err.operation_data.type, 'insert')
    t.assert_equals(err.operation_data.space, 'developers')

    assert_absent_by_id(g, 'customers', 2001)
    assert_absent_by_id(g, 'developers', 3001)

    local existing_dev = get_single_object(g, 'developers', 2001)
    t.assert_equals(existing_dev.login, 'duplicate_login')
end

pgroup.test_invalid_tuple_causes_rollback = function(g)
    local res, err = g.router:call('crud.atomic_batch', {{
        {
            type = 'insert',
            space = 'customers',
            tuple = {8001, box.NULL, 'will_rollback', 23},
        },
        {
            type = 'insert',
            space = 'customers',
            tuple = {8002, box.NULL, 'wrong_type', 'not_a_number'},
        },
    }})

    t.assert_equals(res, nil)
    t.assert_not_equals(err, nil)
    t.assert_equals(err.operation_index, 2)

    assert_absent_by_id(g, 'customers', 8001)
    assert_absent_by_id(g, 'customers', 8002)
end

pgroup.test_validation_unsupported_operation_type = function(g)
    local res, err = g.router:call('crud.atomic_batch', {{
        {
            type = 'select',
            space = 'customers',
            key = {1},
        },
    }})

    t.assert_equals(res, nil)
    assert_error_contains(err, 'unsupported type')
end

pgroup.test_validation_missing_required_key = function(g)
    local res, err = g.router:call('crud.atomic_batch', {{
        {
            type = 'update',
            space = 'customers',
            operations = {{'+', 'age', 1}},
        },
    }})

    t.assert_equals(res, nil)
    assert_error_contains(err, "'key' is required")
end

-- -----------------------------------------------------------------------------
-- Sharding/routing behavior
-- -----------------------------------------------------------------------------

pgroup.test_multiple_bucket_ids_on_same_replicaset = function(g)
    local id1, id2 = find_two_ids_same_replicaset(g)

    local res, err = g.router:call('crud.atomic_batch', {{
        {
            type = 'insert',
            space = 'customers',
            tuple = {id1, box.NULL, 'same_replicaset_1', 31},
        },
        {
            type = 'insert',
            space = 'customers',
            tuple = {id2, box.NULL, 'same_replicaset_2', 32},
        },
    }})

    t.assert_equals(err, nil)
    t.assert_type(res, 'table')

    local obj1 = get_single_object(g, 'customers', id1)
    local obj2 = get_single_object(g, 'customers', id2)

    t.assert_not_equals(obj1.bucket_id, obj2.bucket_id)
end

pgroup.test_rejects_cross_replicaset_batch = function(g)
    local id1, id2 = find_two_ids_different_replicasets(g)

    local res, err = g.router:call('crud.atomic_batch', {{
        {
            type = 'insert',
            space = 'customers',
            tuple = {id1, box.NULL, 'cross_replicaset_1', 19},
        },
        {
            type = 'insert',
            space = 'customers',
            tuple = {id2, box.NULL, 'cross_replicaset_2', 20},
        },
    }})

    t.assert_equals(res, nil)
    assert_error_contains(err, 'must target the same replicaset')
    t.assert_equals(err.operation_index, 2)
    t.assert_equals(err.operation_data.type, 'insert')
    t.assert_equals(err.operation_data.space, 'customers')

    local tuple = err.operation_data.tuple
    t.assert_type(tuple, 'table')
    t.assert_equals(tuple[1], id2)
    t.assert_equals(tuple[3], 'cross_replicaset_2')
    t.assert_equals(tuple[4], 20)
    t.assert_type(tuple[2], 'number')

    assert_absent_by_id(g, 'customers', id1)
    assert_absent_by_id(g, 'customers', id2)
end

pgroup.test_custom_sharding_key_from_ddl_space = function(g)
    local age = 37

    local expected_bucket = g.router:eval([[
        local vshard = require('vshard')
        return vshard.router.bucket_id_strcrc32(...)
    ]], {age})

    local res, err = g.router:call('crud.atomic_batch', {{
        {
            type = 'insert',
            space = 'customers_sharded_by_age',
            tuple = {7101, box.NULL, 'by_age_1', age},
        },
        {
            type = 'insert',
            space = 'customers_sharded_by_age',
            tuple = {7102, box.NULL, 'by_age_2', age},
        },
    }})

    t.assert_equals(err, nil)
    t.assert_type(res, 'table')

    local get_1, get_1_err = g.router:call('crud.get', {
        'customers_sharded_by_age',
        {7101},
        {mode = 'write', bucket_id = expected_bucket},
    })
    t.assert_equals(get_1_err, nil)

    local get_2, get_2_err = g.router:call('crud.get', {
        'customers_sharded_by_age',
        {7102},
        {mode = 'write', bucket_id = expected_bucket},
    })
    t.assert_equals(get_2_err, nil)

    local objects_1, unflatten_1_err = crud.unflatten_rows(get_1.rows, get_1.metadata)
    t.assert_equals(unflatten_1_err, nil)
    t.assert_equals(#objects_1, 1)

    local objects_2, unflatten_2_err = crud.unflatten_rows(get_2.rows, get_2.metadata)
    t.assert_equals(unflatten_2_err, nil)
    t.assert_equals(#objects_2, 1)

    t.assert_equals(objects_1[1].bucket_id, expected_bucket)
    t.assert_equals(objects_2[1].bucket_id, expected_bucket)
end

-- -----------------------------------------------------------------------------
-- MVCC requirement for mixed memtx/vinyl transaction
-- -----------------------------------------------------------------------------

mvcc_group.test_mixed_memtx_vinyl_requires_mvcc = function(g)
    local id1, id2 = find_two_ids_same_replicaset(g)

    local res, err = call_mixed_engine_atomic_batch(g, id1, id2)
    assert_mixed_mvcc_result(g, id1, id2, res, err)
end
