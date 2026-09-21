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

local function find_two_ids_different_buckets(g)
    local ids = g.router:eval([[
        local vshard = require('vshard')

        local first_bucket = vshard.router.bucket_id_strcrc32(1)
        for id = 2, ... do
            if vshard.router.bucket_id_strcrc32(id) ~= first_bucket then
                return {1, id}
            end
        end
    ]], {ID_SCAN_LIMIT})

    t.assert_type(ids, 'table')
    return ids[1], ids[2]
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
    local cross_engine_supported = helpers.tarantool_version_at_least(3, 4, 0)

    if g.params.memtx_use_mvcc_engine and cross_engine_supported then
        t.assert_equals(err, nil)
        t.assert_type(res, 'table')

        local memtx_obj = get_single_object(g, 'customers_memtx', id1)
        local vinyl_obj = get_single_object(g, 'customers_vinyl', id2)
        t.assert_equals(memtx_obj.name, 'memtx_row')
        t.assert_equals(vinyl_obj.name, 'vinyl_row')
        return
    end

    t.assert_equals(res, nil)
    if g.params.memtx_use_mvcc_engine then
        assert_error_contains(err, 'Tarantool 3.4.0')
    else
        assert_error_contains(err, 'requires MVCC')
    end
    assert_absent_by_id(g, 'customers_memtx', id1)
    assert_absent_by_id(g, 'customers_vinyl', id2)
end

-- -----------------------------------------------------------------------------
-- Happy path
-- -----------------------------------------------------------------------------

pgroup.test_empty_batch = function(g)
    local res, err = g.router:call('crud.atomic_batch', { { } })

    t.assert_equals(err, nil)
    t.assert_equals(res, {metadata = {}, data = {}, ops = {}})
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

pgroup.test_preserves_result_slots_for_no_result_ops = function(g)
    local id = 7001

    local res, err = g.router:call('crud.atomic_batch', {{
        {
            type = 'get',
            space = 'customers',
            key = {id},
        },
        {
            type = 'insert',
            space = 'customers',
            tuple = {id, box.NULL, 'slot_insert_customer', 33},
        },
        {
            type = 'upsert',
            space = 'customers',
            tuple = {id, box.NULL, 'slot_upsert_customer', 34},
            operations = {{'+', 'age', 1}},
        },
    }})

    t.assert_equals(err, nil)
    t.assert_type(res, 'table')
    t.assert_equals(#res.data, 3)
    t.assert_equals(res.data[1], box.NULL)
    t.assert_equals(res.data[2][1], id)
    t.assert_equals(res.data[2][3], 'slot_insert_customer')
    t.assert_equals(res.data[3], box.NULL)
end

pgroup.test_all_operation_types_in_single_batch = function(g)
    local id = 7002

    local res, err = g.router:call('crud.atomic_batch', {{
        {
            type = 'get',
            space = 'customers',
            key = {id},
        },
        {
            type = 'insert',
            space = 'customers',
            tuple = {id, box.NULL, 'inserted_customer', 31},
        },
        {
            type = 'replace',
            space = 'customers',
            tuple = {id, box.NULL, 'replaced_customer', 32},
        },
        {
            type = 'update',
            space = 'customers',
            key = {id},
            operations = {{'+', 'age', 2}},
        },
        {
            type = 'upsert',
            space = 'customers',
            tuple = {id, box.NULL, 'upserted_customer', 40},
            operations = {{'=', 'name', 'upserted_customer_updated'}},
        },
        {
            type = 'delete',
            space = 'customers',
            key = {id},
        },
    }})

    t.assert_equals(err, nil)
    t.assert_type(res, 'table')
    t.assert_equals(#res.data, 6)

    t.assert_equals(res.data[1], box.NULL)
    t.assert_equals(res.data[2][1], id)
    t.assert_equals(res.data[2][3], 'inserted_customer')
    t.assert_equals(res.data[3][3], 'replaced_customer')
    t.assert_equals(res.data[4][3], 'replaced_customer')
    t.assert_equals(res.data[4][4], 34)
    t.assert_equals(res.data[5], box.NULL)
    if g.params.engine == 'memtx' then
        t.assert_equals(res.data[6][3], 'upserted_customer_updated')
    else
        t.assert_equals(res.data[6], box.NULL)
    end

    assert_absent_by_id(g, 'customers', id)
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

    t.assert_equals(res.ops, {
        {type = 'insert', space = 'customers'},
        {type = 'insert', space = 'developers'},
    })
end

-- -----------------------------------------------------------------------------
-- Rollback and validation errors
-- -----------------------------------------------------------------------------

pgroup.test_rollback_on_mid_batch_error = function(g)
    local id = 7003

    helpers.insert_objects(g, 'developers', {{
        id = id,
        name = 'existing_developer',
        login = 'existing_login',
    }})

    local res, err = g.router:call('crud.atomic_batch', {{
        {
            type = 'insert',
            space = 'customers',
            tuple = {id, box.NULL, 'rollback_customer', 18},
        },
        {
            type = 'insert',
            space = 'developers',
            tuple = {id, box.NULL, 'conflicting_developer', 'conflicting_login'},
        },
        {
            type = 'update',
            space = 'customers',
            key = {id},
            operations = {{'+', 'age', 1}},
        },
    }})

    t.assert_equals(res, nil)
    assert_error_contains(err, 'Operation #2')
    t.assert_equals(err.operation_index, 2)
    t.assert_equals(err.operation_data.type, 'insert')
    t.assert_equals(err.operation_data.space, 'developers')

    assert_absent_by_id(g, 'customers', id)

    local existing_dev = get_single_object(g, 'developers', id)
    t.assert_equals(existing_dev.login, 'existing_login')
end

pgroup.test_invalid_tuple_causes_rollback = function(g)
    local id = 7004

    local res, err = g.router:call('crud.atomic_batch', {{
        {
            type = 'insert',
            space = 'customers',
            tuple = {id, box.NULL, 'will_rollback', 23},
        },
        {
            type = 'insert',
            space = 'customers',
            tuple = {id, box.NULL, 'wrong_type', 'not_a_number'},
        },
    }})

    t.assert_equals(res, nil)
    t.assert_not_equals(err, nil)
    t.assert_equals(err.operation_index, 2)

    assert_absent_by_id(g, 'customers', id)
end

pgroup.test_unrefs_bucket_on_commit_failure = function(g)
    local res = g.cluster:server('s1-master'):exec(function()
        local bucket_ref_unref = require('crud.common.sharding.bucket_ref_unref')

        local orig_begin = box.begin
        local orig_commit = box.commit
        local orig_rollback = box.rollback
        local orig_bucket_refrw = bucket_ref_unref.bucket_refrw

        local unref_called = false

        -- Force commit to fail to assert the bucket is still unref'd.
        box.begin = function() end
        box.rollback = function() end
        box.commit = function() error('simulated commit failure') end
        bucket_ref_unref.bucket_refrw = function()
            return true, nil, function()
                unref_called = true
                return true
            end
        end

        local storage = require('crud.atomic_batch.storage')
        local yield_checks = require('crud.common.yield_checks')
        local atomic_batch_on_storage = storage.storage_api.atomic_batch_on_storage

        local storage_result = yield_checks.guard(atomic_batch_on_storage, {
            {type = 'get', space = 'customers', key = {123456789}, bucket_id = 1},
        }, {sharding_meta = {}})

        box.begin = orig_begin
        box.commit = orig_commit
        box.rollback = orig_rollback
        bucket_ref_unref.bucket_refrw = orig_bucket_refrw

        return {
            unref_called = unref_called,
            err = storage_result ~= nil and storage_result.err or nil,
        }
    end)

    t.assert_equals(res.unref_called, true)
    assert_error_contains(res.err, 'simulated commit failure')
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
    t.assert_equals(err.class_name, 'AtomicBatchError')
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

pgroup.test_rejects_cross_bucket_batch = function(g)
    local id1, id2 = find_two_ids_different_buckets(g)

    local res, err = g.router:call('crud.atomic_batch', {{
        {
            type = 'insert',
            space = 'customers',
            tuple = {id1, box.NULL, 'cross_bucket_1', 19},
        },
        {
            type = 'insert',
            space = 'customers',
            tuple = {id2, box.NULL, 'cross_bucket_2', 20},
        },
    }})

    t.assert_equals(res, nil)
    t.assert_equals(err.class_name, 'AtomicBatchError')
    assert_error_contains(err, 'must target the same bucket')
    t.assert_equals(err.operation_index, 2)
    t.assert_equals(err.operation_data.type, 'insert')
    t.assert_equals(err.operation_data.space, 'customers')

    local tuple = err.operation_data.tuple
    t.assert_type(tuple, 'table')
    t.assert_equals(tuple[1], id2)
    t.assert_equals(tuple[3], 'cross_bucket_2')
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

pgroup.test_mixed_sharding_keys_in_single_batch = function(g)
    -- customers is sharded by id, customers_sharded_by_age by age.
    -- Both values are 37, so both ops target the same bucket and replicaset.
    local id = 37
    local age = 37

    local res, err = g.router:call('crud.atomic_batch', {{
        {type = 'insert', space = 'customers', tuple = {id, box.NULL, 'mixed_by_id', 30}},
        {type = 'insert', space = 'customers_sharded_by_age', tuple = {id, box.NULL, 'mixed_by_age', age}},
    }})

    t.assert_equals(err, nil)
    t.assert_type(res, 'table')

    local obj_by_id = get_single_object(g, 'customers', id)
    t.assert_equals(obj_by_id.name, 'mixed_by_id')

    local expected_bucket = g.router:eval([[
        local vshard = require('vshard')
        return vshard.router.bucket_id_strcrc32(...)
    ]], {age})

    local get_by_age, get_err = g.router:call('crud.get', {
        'customers_sharded_by_age',
        {id},
        {mode = 'write', bucket_id = expected_bucket},
    })
    t.assert_equals(get_err, nil)

    local objects, unflatten_err = crud.unflatten_rows(get_by_age.rows, get_by_age.metadata)
    t.assert_equals(unflatten_err, nil)
    t.assert_equals(#objects, 1)
    t.assert_equals(objects[1].name, 'mixed_by_age')
    t.assert_equals(objects[1].age, age)
end

pgroup.test_explicit_bucket_id_does_not_skip_sharding_check_for_other_spaces = function(g)
    local res = g.cluster:server('s1-master'):exec(function()
        local storage = require('crud.atomic_batch.storage')
        local yield_checks = require('crud.common.yield_checks')
        local sharding_utils = require('crud.common.sharding.utils')

        local atomic_batch_on_storage = storage.storage_api.atomic_batch_on_storage

        -- customers had an explicit bucket_id, so its sharding hash check is skipped.
        -- customers_sharded_by_age had a computed bucket_id, so its hash must still
        -- be validated. A wrong key hash is rejected even though the other space's
        -- check is skipped.
        local wrong_key_hash = sharding_utils.compute_hash({'name'})

        local _, err = yield_checks.guard(atomic_batch_on_storage, {
            {type = 'insert', space = 'customers', tuple = {1, box.NULL, 'x', 1}},
            {type = 'insert', space = 'customers_sharded_by_age', tuple = {2, box.NULL, 'y', 2}},
        }, {
            sharding_meta = {
                customers = {skip_sharding_hash_check = true},
                customers_sharded_by_age = {sharding_key_hash = wrong_key_hash},
            },
        })

        return {err = err ~= nil and tostring(err) or nil}
    end)

    assert_error_contains(res.err, 'ShardingHashMismatchError')
end

-- -----------------------------------------------------------------------------
-- MVCC requirement for mixed memtx/vinyl transaction
-- -----------------------------------------------------------------------------

mvcc_group.test_mixed_memtx_vinyl_requires_mvcc = function(g)
    local id = 7005

    local res, err = call_mixed_engine_atomic_batch(g, id, id)
    assert_mixed_mvcc_result(g, id, id, res, err)
end
