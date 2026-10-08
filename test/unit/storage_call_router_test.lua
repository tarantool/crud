local t = require('luatest')
local helpers = require('test.helper')
local utils = require('crud.common.utils')
local router = require('crud.storage_call.router')
local storage_call = require('crud.storage_call')
local schema = require('crud.common.schema')
local metadata = require('crud.common.sharding.sharding_metadata')
local cache = require('crud.common.sharding.router_metadata_cache')
local sharding_utils = require('crud.common.sharding.utils')

local g = t.group('storage_call_router')

g.before_all(function()
    helpers.box_cfg()
end)

g.before_each(function(cg)
    cg.get_router = utils.get_vshard_router_instance
    cg.get_space = utils.get_space
    cg.reload_schema = schema.reload_schema
    cg.reload_sharding_cache_for_spaces = metadata.reload_sharding_cache_for_spaces
    cg.fetch_key = metadata.fetch_sharding_key_on_router
    cg.fetch_func = metadata.fetch_sharding_func_on_router
    cg.vshard_router = {
        name = 'storage_call_router_unit',
        bucket_count = function() return 3000 end,
        bucket_id_strcrc32 = function() return 1 end,
    }
    utils.get_vshard_router_instance = function() return cg.vshard_router end
end)

g.after_each(function(cg)
    utils.get_vshard_router_instance = cg.get_router
    utils.get_space = cg.get_space
    schema.reload_schema = cg.reload_schema
    metadata.reload_sharding_cache_for_spaces = cg.reload_sharding_cache_for_spaces
    metadata.fetch_sharding_key_on_router = cg.fetch_key
    metadata.fetch_sharding_func_on_router = cg.fetch_func
    cache.drop_instance(cg.vshard_router)
end)

g.test_public_methods_validate_options = function()
    local single_ok, single_err = pcall(storage_call.call, 'test', {}, {
        bucket_id = 1, timeout = 'invalid',
    })
    t.assert_not(single_ok)
    t.assert_str_contains(tostring(single_err), 'timeout')

    local batch_ok, batch_err = pcall(storage_call.call_many, {}, {
        timeout = 'invalid',
    })
    t.assert_not(batch_ok)
    t.assert_str_contains(tostring(batch_err), 'timeout')
end

g.test_single_rejects_invalid_func_name_before_routing = function(cg)
    local routes = 0
    utils.get_vshard_router_instance = function()
        routes = routes + 1
        return cg.vshard_router
    end

    for _, func_name in ipairs({box.NULL, 42, {}}) do
        local result, err = storage_call.call(func_name, {}, {bucket_id = 1})
        t.assert_equals(result, nil)
        t.assert_equals(err.class_name, 'StorageCallError')
        t.assert_str_contains(err.err, 'func_name must be a string')
        t.assert_equals(err.may_have_side_effects, false)
    end
    t.assert_equals(routes, 0)
end

g.test_routing_exception_does_not_prevent_valid_batch_item = function(cg)
    utils.get_space = function() return {index = {[0] = {parts = {}}}} end
    metadata.fetch_sharding_key_on_router = function() return {hash = 1} end
    metadata.fetch_sharding_func_on_router = function()
        return {value = function() error('invalid custom key') end}
    end
    cg.vshard_router.map_callrw = function(_, _, _, opts)
        t.assert_equals(#opts.bucket_ids[1], 1)
        t.assert_equals(opts.bucket_ids[1][1].operation_index, 2)
        return {rs = {{{operation_index = 2, returns = {'valid'}}}}}
    end
    local result, err = router.call_many({
        {func_name = 'test', space_name = 'customers', key = {1}},
        {func_name = 'test', bucket_id = 1},
    }, {})
    t.assert_equals(err, nil)
    t.assert_str_contains(result.results[1].error.err, 'invalid custom key')
    t.assert_equals(result.results[1].error.may_have_side_effects, false)
    t.assert_equals(result.results[2].returns, {'valid'})
end


g.test_single_mismatch_retries_after_dropping_cache = function(cg)
    local cached = cache.get_instance(cg.vshard_router)
    utils.get_space = function()
        return {index = {[0] = {parts = {}}}}
    end
    metadata.fetch_sharding_key_on_router = function()
        return {value = nil, hash = 1}
    end
    metadata.fetch_sharding_func_on_router = function()
        return {value = nil, hash = 2}
    end
    local calls = 0
    local reloads = 0
    metadata.reload_sharding_cache_for_spaces = function(vshard_router, space_names)
        t.assert_equals(vshard_router, cg.vshard_router)
        t.assert_equals(space_names, {customers = true})
        reloads = reloads + 1
        cache.drop_instance(vshard_router)
    end
    cg.vshard_router.callrw = function()
        calls = calls + 1
        if calls == 1 then
            return {error = sharding_utils.ShardingHashMismatchError:new(
                'sharding mismatch')}
        end
        return {returns = {'ok'}}
    end
    local result, err = router.call('test', {}, {
        space_name = 'customers', key = {1},
    })
    t.assert_equals(err, nil)
    t.assert_equals(result.returns, {'ok'})
    t.assert_equals(calls, 2)
    t.assert_equals(reloads, 1)
    t.assert_not_equals(cache.get_instance(cg.vshard_router), cached)
end

local function mock_key_routing()
    utils.get_space = function()
        return {index = {[0] = {parts = {}}}}
    end
    metadata.fetch_sharding_key_on_router = function()
        return {value = nil, hash = 1}
    end
    metadata.fetch_sharding_func_on_router = function()
        return {value = nil, hash = 2}
    end
end

g.test_batch_retries_when_every_item_has_sharding_mismatch = function(cg)
    mock_key_routing()
    local map_calls = 0
    local reloads = 0
    metadata.reload_sharding_cache_for_spaces = function(_, space_names)
        t.assert_equals(space_names, {customers = true})
        reloads = reloads + 1
    end
    cg.vshard_router.map_callrw = function()
        map_calls = map_calls + 1
        if map_calls == 1 then
            return {rs = {{{
                operation_index = 1,
                error = sharding_utils.ShardingHashMismatchError:new(
                    'sharding mismatch'),
            }}}}
        end
        return {rs = {{{operation_index = 1, returns = {'ok'}}}}}
    end
    local result, err = router.call_many({{
        func_name = 'test', space_name = 'customers', key = {1},
    }}, {})
    t.assert_equals(err, nil)
    t.assert_equals(result.results[1].returns, {'ok'})
    t.assert_equals(map_calls, 2)
    t.assert_equals(reloads, 1)
end

g.test_batch_does_not_retry_after_partial_success = function(cg)
    mock_key_routing()
    local cached = cache.get_instance(cg.vshard_router)
    local map_calls = 0
    metadata.reload_sharding_cache_for_spaces = function()
        error('sharding cache must not be reloaded after partial success')
    end
    cg.vshard_router.map_callrw = function()
        map_calls = map_calls + 1
        return {rs = {{
            {
                operation_index = 1,
                error = sharding_utils.ShardingHashMismatchError:new(
                    'sharding mismatch'),
            },
            {operation_index = 2, returns = {'done'}},
        }}}
    end
    local result, err = router.call_many({
        {func_name = 'test', space_name = 'customers', key = {1}},
        {func_name = 'test', bucket_id = 1},
    }, {})
    t.assert_equals(err, nil)
    t.assert_equals(result.results[1].error.may_have_side_effects, false)
    t.assert_equals(result.results[2].returns, {'done'})
    t.assert_equals(map_calls, 1)
    t.assert_not_equals(cache.get_instance(cg.vshard_router), cached)
end

g.test_batch_stops_after_one_sharding_reload = function(cg)
    mock_key_routing()
    local map_calls = 0
    local reloads = 0
    metadata.reload_sharding_cache_for_spaces = function()
        reloads = reloads + 1
    end
    cg.vshard_router.map_callrw = function()
        map_calls = map_calls + 1
        return {rs = {{{
            operation_index = 1,
            error = sharding_utils.ShardingHashMismatchError:new(
                'sharding mismatch'),
        }}}}
    end

    local result, err = router.call_many({{
        func_name = 'test', space_name = 'customers', key = {1},
    }}, {})

    t.assert_equals(err, nil)
    t.assert_str_contains(result.results[1].error.err, 'sharding mismatch')
    t.assert_equals(map_calls, 2)
    t.assert_equals(reloads, 1)
end

g.test_batch_reloads_missing_schema_only_once = function(cg)
    local reloads = 0
    utils.get_space = function()
        if reloads > 0 then
            return {index = {[0] = {parts = {}}}}
        end
    end
    schema.reload_schema = function()
        reloads = reloads + 1
        return true
    end
    metadata.fetch_sharding_key_on_router = function()
        return {value = nil, hash = 1}
    end
    metadata.fetch_sharding_func_on_router = function()
        return {value = nil, hash = 2}
    end
    cg.vshard_router.map_callrw = function()
        return {rs = {{
            {operation_index = 1, returns = {'first'}},
            {operation_index = 2, returns = {'second'}},
        }}}
    end

    local result, err = router.call_many({
        {func_name = 'test', space_name = 'new_space', key = {1}},
        {func_name = 'test', space_name = 'new_space', key = {2}},
    }, {})

    t.assert_equals(err, nil)
    t.assert_equals(result.results[1].returns, {'first'})
    t.assert_equals(result.results[2].returns, {'second'})
    t.assert_equals(reloads, 1)
end

g.test_batch_keeps_missing_space_as_item_error_after_reload = function(cg)
    local reloads = 0
    local map_calls = 0
    utils.get_space = function(space_name)
        if space_name == 'known_space' then
            return {index = {[0] = {parts = {}}}}
        end
    end
    schema.reload_schema = function()
        reloads = reloads + 1
        return true
    end
    metadata.fetch_sharding_key_on_router = function()
        return {value = nil, hash = 1}
    end
    metadata.fetch_sharding_func_on_router = function()
        return {value = nil, hash = 2}
    end
    cg.vshard_router.map_callrw = function(_, _, _, opts)
        map_calls = map_calls + 1
        t.assert_equals(#opts.bucket_ids[1], 1)
        t.assert_equals(opts.bucket_ids[1][1].operation_index, 2)
        return {rs = {{{operation_index = 2, returns = {'ok'}}}}}
    end

    local result, err = router.call_many({
        {func_name = 'test', space_name = 'missing_space', key = {1}},
        {func_name = 'test', space_name = 'known_space', key = {1}},
    }, {})

    t.assert_equals(err, nil)
    t.assert_str_contains(result.results[1].error.err, 'does not exist')
    t.assert_equals(result.results[2].returns, {'ok'})
    t.assert_equals(reloads, 1)
    t.assert_equals(map_calls, 1)
end
