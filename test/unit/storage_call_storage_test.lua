local t = require('luatest')
local helpers = require('test.helper')
local utils = require('crud.common.utils')
local vshard = require('vshard')
local api = require('crud.storage_call.storage').storage_api

local g = t.group('storage_call_storage')
local functions = {
    storage_call_unit_open = [[function()
        box.begin()
        box.space.storage_call_unit:replace{1, 'uncommitted'}
        return true
    end]],
    storage_call_unit_commit = [[function()
        box.commit()
        return true
    end]],
}

g.before_all(function()
    helpers.box_cfg()
    box.schema.space.create('storage_call_unit'):create_index('primary')
    for name, body in pairs(functions) do
        box.schema.func.create(name, {body = body, is_sandboxed = false})
    end
end)

g.after_all(function()
    for name in pairs(functions) do
        box.schema.func.drop(name)
    end
    box.space.storage_call_unit:drop()
    rawset(_G, 'storage_call_unit_shared_value', nil)
end)

g.before_each(function(cg)
    cg.rollback = box.rollback
    cg.get_user = utils.get_this_replica_user
    cg.refrw = vshard.storage.bucket_refrw
    cg.unrefrw = vshard.storage.bucket_unrefrw
    cg.refs = 0
    cg.unrefs = 0
    utils.get_this_replica_user = box.session.user
    vshard.storage.bucket_refrw = function()
        cg.refs = cg.refs + 1
        return true
    end
    vshard.storage.bucket_unrefrw = function()
        cg.refs = cg.refs - 1
        cg.unrefs = cg.unrefs + 1
        return true
    end
end)

g.after_each(function(cg)
    box.rollback = cg.rollback
    box.rollback()
    utils.get_this_replica_user = cg.get_user
    vshard.storage.bucket_refrw = cg.refrw
    vshard.storage.bucket_unrefrw = cg.unrefrw
    box.space.storage_call_unit:truncate()
end)

local function call(name, index)
    return {
        func_name = name, args = {}, bucket_id = 1,
        operation_index = index, skip_sharding_hash_check = true,
    }
end

g.test_failed_rollback_stops_batch_and_releases_bucket = function(cg)
    box.rollback = function() error('rollback failed before cleanup') end
    t.assert_error_msg_contains('failed to clean up the open transaction', function()
        api.storage_call_many_on_storage('admin', {[1] = {
            call('storage_call_unit_open', 1),
            call('storage_call_unit_commit', 2),
        }})
    end)
    -- If the second item ran it would have committed the first item's write.
    t.assert(box.is_in_txn())
    cg.rollback()
    t.assert_equals(box.space.storage_call_unit:get{1}, nil)
    t.assert_equals(cg.refs, 0)
    t.assert_equals(cg.unrefs, 1)
end

g.test_transient_rollback_failure_is_cleaned_before_next_item = function(cg)
    local attempts = 0
    box.rollback = function()
        attempts = attempts + 1
        if attempts == 1 then
            error('temporary rollback failure')
        end
        return cg.rollback()
    end
    local results = api.storage_call_many_on_storage('admin', {[1] = {
        call('storage_call_unit_open', 1),
        call('storage_call_unit_commit', 2),
    }})
    t.assert_str_contains(results[1].error.err, 'temporary rollback failure')
    t.assert_equals(results[2].returns, {true})
    t.assert_equals(box.space.storage_call_unit:get{1}, nil)
    t.assert_not(box.is_in_txn())
    t.assert_equals(cg.refs, 0)
end

