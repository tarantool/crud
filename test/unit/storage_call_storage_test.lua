local t = require('luatest')
local helpers = require('test.helper')
local utils = require('crud.common.utils')
local api = require('crud.storage_call.storage').storage_api

local g = t.group('storage_call_storage')
local functions = {
    storage_call_unit_open = [[function()
        box.begin()
        box.space.storage_call_unit:replace{1, 'uncommitted'}
        return true
    end]],
    storage_call_unit_read = [[function()
        return box.space.storage_call_unit:get{1}
    end]],
    storage_call_unit_shared = [[function()
        rawset(_G, 'storage_call_unit_shared_value', {'original'})
        return _G.storage_call_unit_shared_value, nil, false, nil
    end]],
    storage_call_unit_mutate = [[function()
        _G.storage_call_unit_shared_value[1] = function() end
        return true
    end]],
    storage_call_unit_serialize_txn = [[function()
        return setmetatable({}, {__serialize = function()
            box.begin()
            box.space.storage_call_unit:replace{1, 'uncommitted'}
            return {'value'}
        end})
    end]],
    storage_call_unit_serialize_commit = [[function()
        return setmetatable({}, {__serialize = function()
            if box.is_in_txn() then
                box.commit()
            end
            return {'value'}
        end})
    end]],
    storage_call_unit_serialize_error_txn = [[function()
        return setmetatable({}, {__serialize = function()
            box.begin()
            box.space.storage_call_unit:replace{2, 'uncommitted'}
            error('serializer failed')
        end})
    end]],
    storage_call_unit_access_error = [[function()
        box.space.storage_call_unit:replace{1, 'committed'}
        box.error(box.error.new{
            code = box.error.ACCESS_DENIED,
            reason = "Execute access to function 'storage_call_unit_access_error' is denied",
        })
    end]],
    storage_call_unit_cdata_result = [[function()
        local ffi = require('ffi')
        local ctype = rawget(_G, 'storage_call_unit_cdata_type')
        if type(ctype) == 'nil' then
            ctype = ffi.metatype(ffi.typeof('struct { int marker; }'), {
                __eq = function()
                    _G.storage_call_unit_eq_calls =
                        _G.storage_call_unit_eq_calls + 1
                    box.space.storage_call_unit:replace{77, 'elevated'}
                    return false
                end,
            })
            rawset(_G, 'storage_call_unit_cdata_type', ctype)
        end
        return ffi.new(ctype)
    end]],
    storage_call_unit_cdata_error = [[function()
        local ffi = require('ffi')
        local ctype = rawget(_G, 'storage_call_unit_error_type')
        if type(ctype) == 'nil' then
            ctype = ffi.metatype(ffi.typeof('struct { int marker; }'), {
                __eq = function()
                    box.space.storage_call_unit:replace{78, 'elevated'}
                    return false
                end,
                __tostring = function()
                    box.space.storage_call_unit:replace{79, 'elevated'}
                    return 'target error'
                end,
            })
            rawset(_G, 'storage_call_unit_error_type', ctype)
        end
        error(ffi.new(ctype))
    end]],
    storage_call_unit_serialize_cdata_error = [[function()
        return setmetatable({}, {__serialize = function()
            local ffi = require('ffi')
            local ctype = rawget(_G, 'storage_call_unit_serialize_error_type')
            if type(ctype) == 'nil' then
                ctype = ffi.metatype(ffi.typeof('struct { int serializer_marker; }'), {
                    __index = function()
                        box.space.storage_call_unit:replace{80, 'elevated'}
                    end,
                    __tostring = function()
                        box.space.storage_call_unit:replace{81, 'elevated'}
                        return 'serializer error'
                    end,
                })
                rawset(_G, 'storage_call_unit_serialize_error_type', ctype)
            end
            error(ffi.new(ctype))
        end})
    end]],
}

g.before_all(function()
    helpers.box_cfg()
    box.schema.space.create('storage_call_unit'):create_index('primary')
    for name, body in pairs(functions) do
        box.schema.func.create(name, {body = body, is_sandboxed = false})
    end
    box.schema.user.create('storage_call_unit_user')
    box.schema.user.grant('storage_call_unit_user', 'execute', 'function',
        'storage_call_unit_cdata_result')
    box.schema.user.grant('storage_call_unit_user', 'execute', 'function',
        'storage_call_unit_cdata_error')
    box.schema.user.grant('storage_call_unit_user', 'execute', 'function',
        'storage_call_unit_serialize_cdata_error')
end)

g.after_all(function()
    for name in pairs(functions) do
        box.schema.func.drop(name)
    end
    box.schema.user.drop('storage_call_unit_user')
    box.space.storage_call_unit:drop()
    rawset(_G, 'storage_call_unit_shared_value', nil)
    rawset(_G, 'storage_call_unit_cdata_type', nil)
    rawset(_G, 'storage_call_unit_error_type', nil)
    rawset(_G, 'storage_call_unit_serialize_error_type', nil)
end)

g.before_each(function(cg)
    cg.rollback = box.rollback
    cg.get_user = utils.get_this_replica_user
    rawset(_G, 'storage_call_unit_eq_calls', 0)
    utils.get_this_replica_user = box.session.user
end)

g.after_each(function(cg)
    box.rollback = cg.rollback
    box.rollback()
    utils.get_this_replica_user = cg.get_user
    box.space.storage_call_unit:truncate()
end)

local function call(name, index)
    return {
        func_name = name, args = {}, bucket_id = 1,
        operation_index = index, skip_sharding_hash_check = true,
    }
end

g.test_open_transaction_is_rolled_back_before_next_item = function()
    local results = api.storage_call_many_on_storage('admin', {[1] = {
        call('storage_call_unit_open', 1),
        call('storage_call_unit_read', 2),
    }})
    t.assert_str_contains(results[1].error.err, 'open transaction')
    t.assert_equals(results[1].error.may_have_side_effects, true)
    t.assert_equals(results[2].returns, {})
    t.assert_not(box.is_in_txn())
    t.assert_equals(box.space.storage_call_unit:get{1}, nil)
end

g.test_return_values_are_not_copied = function()
    local results = api.storage_call_many_on_storage('admin', {[1] = {
        call('storage_call_unit_shared', 1),
        call('storage_call_unit_mutate', 2),
    }})
    t.assert_equals(results[1].returns[1], _G.storage_call_unit_shared_value)
    t.assert_equals(type(results[1].returns[1][1]), 'function')
    t.assert_equals(results[1].returns[2], box.NULL)
    t.assert_equals(results[1].returns[3], false)
    t.assert_equals(results[1].returns[4], box.NULL)
    t.assert_equals(results[2].returns, {true})
end

g.test_serializer_is_not_called_during_dispatch = function()
    local result = api.storage_call_on_storage(
        'admin', call('storage_call_unit_serialize_txn'))

    t.assert_equals(type(result.returns[1]), 'table')
    t.assert_not(box.is_in_txn())
    t.assert_equals(box.space.storage_call_unit:get{1}, nil)
end

g.test_batch_does_not_call_serializers_between_items = function()
    local results = api.storage_call_many_on_storage('admin', {[1] = {
        call('storage_call_unit_serialize_txn', 1),
        call('storage_call_unit_serialize_commit', 2),
    }})

    t.assert_equals(#results, 2)
    t.assert_equals(type(results[1].returns[1]), 'table')
    t.assert_equals(type(results[2].returns[1]), 'table')
    t.assert_not(box.is_in_txn())
    t.assert_equals(box.space.storage_call_unit:get{1}, nil)
end

g.test_serializer_exception_is_deferred = function()
    local result = api.storage_call_on_storage(
        'admin', call('storage_call_unit_serialize_error_txn'))

    t.assert_equals(type(result.returns[1]), 'table')
    t.assert_not(box.is_in_txn())
    t.assert_equals(box.space.storage_call_unit:get{2}, nil)
end

g.test_error_text_does_not_prove_absence_of_side_effects = function()
    local result = api.storage_call_on_storage('admin', call('storage_call_unit_access_error'))
    t.assert_str_contains(result.error.err, 'Execute access')
    t.assert_equals(result.error.may_have_side_effects, true)
    t.assert_equals(box.space.storage_call_unit:get{1}:totable(), {1, 'committed'})
end

g.test_cdata_return_equality_is_not_called_during_dispatch = function()
    local single = api.storage_call_on_storage(
        'storage_call_unit_user', call('storage_call_unit_cdata_result'))
    local many = api.storage_call_many_on_storage('storage_call_unit_user', {
        [1] = {call('storage_call_unit_cdata_result', 1)},
    })

    t.assert_equals(type(single.returns[1]), 'cdata')
    t.assert_equals(type(many[1].returns[1]), 'cdata')
    t.assert_equals(_G.storage_call_unit_eq_calls, 0)
    t.assert_equals(box.space.storage_call_unit:get{77}, nil)
end

g.test_cdata_error_hooks_cannot_run_as_service_user = function()
    local result = api.storage_call_on_storage(
        'storage_call_unit_user', call('storage_call_unit_cdata_error'))

    t.assert_not_equals(result.error, nil)
    t.assert_equals(result.error.may_have_side_effects, true)
    t.assert_equals(box.space.storage_call_unit:get{78}, nil)
    t.assert_equals(box.space.storage_call_unit:get{79}, nil)
end

g.test_serializer_error_hooks_are_not_called_during_dispatch = function()
    local result = api.storage_call_on_storage(
        'storage_call_unit_user',
        call('storage_call_unit_serialize_cdata_error'))

    t.assert_equals(type(result.returns[1]), 'table')
    t.assert_equals(box.space.storage_call_unit:get{80}, nil)
    t.assert_equals(box.space.storage_call_unit:get{81}, nil)
end
