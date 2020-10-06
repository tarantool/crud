local checks = require('checks')
local errors = require('errors')
local vshard = require('vshard')

local call = require('crud.common.call')
local registry = require('crud.common.registry')
local utils = require('crud.common.utils')
local dev_checks = require('crud.common.dev_checks')

local select_conditions = require('crud.select.conditions')
local select_plan = require('crud.select.plan')
local select_executor = require('crud.select.executor')
local select_comparators = require('crud.select.comparators')
local select_filters = require('crud.select.filters')

local Iterator = require('crud.select.iterator')

local SelectError = errors.new_class('SelectError')
local GetReplicasetsError = errors.new_class('GetReplicasetsError')

local select_module = {}

local SELECT_FUNC_NAME = '__select'

local DEFAULT_BATCH_SIZE = 100

local function call_select_on_storage(space_name, index_id, conditions, opts)
    dev_checks('string', 'number', '?table', {
        scan_value = 'table',
        after_tuple = '?table',
        iter = 'number',
        limit = 'number',
        scan_condition_num = '?number',
    })

    local space = box.space[space_name]
    if space == nil then
        return nil, SelectError:new("Space %s doesn't exist", space_name)
    end

    local index = space.index[index_id]
    if index == nil then
        return nil, SelectError:new("Index with ID %s doesn't exist", index_id)
    end

    local filter_func, err = select_filters.gen_func(space, conditions, {
        iter = opts.iter,
        scan_condition_num = opts.scan_condition_num,
    })
    if err ~= nil then
        return nil, SelectError:new("Failed to generate tuples filter: %s", err)
    end

    -- execute select
    local tuples, err = select_executor.execute(space, index, filter_func, {
        scan_value = opts.scan_value,
        after_tuple = opts.after_tuple,
        iter = opts.iter,
        limit = opts.limit,
    })
    if err ~= nil then
        return nil, SelectError:new("Failed to execute select: %s", err)
    end

    return tuples
end

function select_module.init()
    registry.add({
        [SELECT_FUNC_NAME] = call_select_on_storage,
    })
end

local function select_iteration(space_name, plan, opts)
    dev_checks('string', '?table', {
        after_tuple = '?table',
        replicasets = 'table',
        timeout = '?number',
        limit = 'number',
    })

    -- call select on storages
    local storage_select_opts = {
        scan_value = plan.scan_value,
        after_tuple = opts.after_tuple,
        iter = plan.iter,
        limit = opts.limit,
        scan_condition_num = plan.scan_condition_num,
    }

    local storage_select_args = {
        space_name, plan.index_id, plan.conditions, storage_select_opts,
    }

    local results, err = call.ro(SELECT_FUNC_NAME, storage_select_args, {
        replicasets = opts.replicasets,
        timeout = opts.timeout,
    })

    if err ~= nil then
        return nil, err
    end

    return results
end

local function get_replicasets_by_sharding_key(sharding_key)
    local bucket_id = vshard.router.bucket_id_strcrc32(sharding_key)
    local replicaset, err = vshard.router.route(bucket_id)
    if replicaset == nil then
        return nil, GetReplicasetsError:new("Failed to get replicaset for bucket_id %s: %s", bucket_id, err.err)
    end

    return {
        [replicaset.uuid] = replicaset,
    }
end

local function build_select_iterator(space_name, user_conditions, opts)
    dev_checks('string', '?table', {
        after = '?table',
        limit = '?number',
        timeout = '?number',
        batch_size = '?number',
    })

    opts = opts or {}

    if opts.batch_size ~= nil and opts.batch_size < 1 then
        return nil, SelectError:new("batch_size should be > 0")
    end

    local batch_size = opts.batch_size or DEFAULT_BATCH_SIZE

    if opts.limit ~= nil and opts.limit < 0 then
        return nil, SelectError:new("limit should be >= 0")
    end

    -- check conditions
    local conditions, err = select_conditions.parse(user_conditions)
    if err ~= nil then
        return nil, SelectError:new("Failed to parse conditions: %s", err)
    end

    local replicasets, err = vshard.router.routeall()
    if err ~= nil then
        return nil, SelectError:new("Failed to get all replicasets: %s", err)
    end

    local space = utils.get_space(space_name, replicasets)
    if space == nil then
        return nil, SelectError:new("Space %s doesn't exist", space_name)
    end
    local space_format = space:format()

    -- plan select
    local plan, err = select_plan.new(space, conditions, {
        limit = opts.limit,
    })

    if err ~= nil then
        return nil, SelectError:new("Failed to plan select: %s", err)
    end

    -- set limit and replicasets to select from
    local replicasets_to_select = replicasets

    if plan.sharding_key ~= nil then
        replicasets_to_select = get_replicasets_by_sharding_key(plan.sharding_key)
    end

    -- set after tuple
    local after_tuple = utils.flatten(opts.after, space_format)

    -- generate tuples comparator
    local scan_index = space.index[plan.index_id]
    local primary_index = space.index[0]
    local cmp_key_parts = utils.merge_primary_key_parts(scan_index.parts, primary_index.parts)
    local cmp_operator = select_comparators.get_cmp_operator(plan.iter)
    local tuples_comparator, err = select_comparators.gen_tuples_comparator(
        cmp_operator, cmp_key_parts
    )
    if err ~= nil then
        return nil, SelectError:new("Failed to generate comparator function: %s", err)
    end

    local iter = Iterator.new({
        space_name = space_name,
        space_format = space_format,
        iteration_func = select_iteration,
        comparator = tuples_comparator,

        plan = plan,
        after_tuple = after_tuple,

        batch_size = batch_size,
        replicasets = replicasets_to_select,

        timeout = opts.timeout,
    })

    return iter
end

function select_module.pairs(space_name, user_conditions, opts)
    checks('string', '?table', {
        after = '?table',
        limit = '?number',
        timeout = '?number',
        batch_size = '?number',
    })

    opts = opts or {}

    local iter, err = build_select_iterator(space_name, user_conditions, {
        after = opts.after,
        limit = opts.limit,
        timeout = opts.timeout,
        batch_size = opts.batch_size,
    })

    if err ~= nil then
        error(string.format("Failed to generate iterator: %s", err))
    end

    local gen = function(_, iter)
        if not iter:has_next() then
            return nil
        end

        local tuple, err = iter:get()
        if err ~= nil then
            error(string.format("Failed to get next object: %s", err))
        end

        local obj, err = utils.unflatten(tuple, iter.space_format)
        if err ~= nil then
            error(string.format("Failed to unflatten next object: %s", err))
        end

        return iter, obj
    end

    return gen, nil, iter
end

function select_module.call(space_name, user_conditions, opts)
    checks('string', '?table', {
        after = '?table',
        limit = '?number',
        timeout = '?number',
        batch_size = '?number',
    })

    opts = opts or {}

    local iter, err = build_select_iterator(space_name, user_conditions, {
        after = opts.after,
        limit = opts.limit,
        timeout = opts.timeout,
        batch_size = opts.batch_size,
    })

    if err ~= nil then
        return nil, err
    end

    local tuples = {}

    while iter:has_next() do
        local obj, err = iter:get()
        if err ~= nil then
            return nil, SelectError:new("Failed to get next object: %s", err)
        end

        if obj == nil then
            break
        end

        table.insert(tuples, obj)
    end

    return {
        metadata = table.copy(iter.space_format),
        rows = tuples,
    }
end

return select_module
