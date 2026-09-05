local t = require('luatest')
local helpers = require('test.helper')
local utils = require('crud.common.utils')
local router = require('crud.storage_call.router')
local metadata = require('crud.common.sharding.sharding_metadata')
local cache = require('crud.common.sharding.router_metadata_cache')

local g = t.group('storage_call_router')

g.before_all(function()
    helpers.box_cfg()
end)

g.before_each(function(cg)
    cg.get_router = utils.get_vshard_router_instance
    cg.get_space = utils.get_space
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
    metadata.fetch_sharding_key_on_router = cg.fetch_key
    metadata.fetch_sharding_func_on_router = cg.fetch_func
    cache.drop_instance(cg.vshard_router)
end)

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

