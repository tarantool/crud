local checks = require('checks')
local errors = require('errors')
local vshard = require('vshard')

local call = require('crud.common.call')
local registry = require('crud.common.registry')
local utils = require('crud.common.utils')

local select_conditions = require('crud.select.conditions')
local select_plan = require('crud.select.plan')
local select_executor = require('crud.select.executor')
local select_comparators = require('crud.select.comparators')
local select_filters = require('crud.select.filters')

local Iterator = require('crud.select.iterator')

require('crud.common.checkers')

local SelectError = errors.new_class('SelectError')
local GetReplicasetsError = errors.new_class('GetReplicasetsError')

local select_module = {}

local SELECT_FUNC_NAME = '__select'

local function call_select_on_storage(space_name, index_id, scan_value, iter, conditions, opts)
    checks('string', 'number', '?table', 'number', 'table', {
        scan_condition_num = '?number',
        after_tuple = '?table',
        batch_size = 'number',
    })

    local space = box.space[space_name]
    if space == nil then
        return nil, SelectError:new("Space %s doesn't exists", space_name)
    end

    local index = space.index[index_id]
    if index == nil then
        return nil, SelectError:new("Index with ID %s doesn't exists", index_id)
    end

    local filter_func, err = select_filters.gen_func(space, conditions, {
        iter = iter,
        scan_condition_num = opts.scan_condition_num,
    })
    if err ~= nil then
        return nil, SelectError:new("Failed to generate tuples filter: %s", err)
    end

    -- execute select
    local tuples, err = select_executor.execute(space, index, scan_value, iter, filter_func, {
        after_tuple = opts.after_tuple,
        batch_size = opts.batch_size,
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
    checks('string', '?table', {
        after_tuple = '?table',
        replicasets = 'table',
        timeout = '?number',
        batch_size = 'number',
    })

    -- call select on storages
    local storage_select_opts = {
        scan_condition_num = plan.scan_condition_num,
        after_tuple = opts.after_tuple,
        batch_size = opts.batch_size,
    }

    local storage_select_args = {
        space_name, plan.index_id, plan.scan_value, plan.iter, plan.conditions, storage_select_opts,
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

local function get_replicaset_by_scan_value(scan_value)
    local bucket_id = vshard.router.bucket_id_strcrc32(scan_value)
    local replicaset, err = vshard.router.route(bucket_id)
    if replicaset == nil then
        return nil, GetReplicasetsError:new("Failed to get replicaset for bucket_id %s: %s", bucket_id, err.err)
    end

    return {
        [replicaset.uuid] = replicaset,
    }
end

local function build_select_iterator(space_name, user_conditions, opts)
    checks('string', '?table', {
        after = '?',
        limit = '?number',
        timeout = '?number',
        batch_size = '?number',
    })

    opts = opts or {}

    if opts.batch_size ~= nil and opts.batch_size < 1 then
        return nil, SelectError:new("batch_size should be > 0")
    end

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
        return nil, SelectError:new("Space %s doesn't exists", space_name)
    end
    local space_format = space:format()

    -- plan select
    local plan, err = select_plan.gen_by_conditions(space, conditions)

    if err ~= nil then
        return nil, SelectError:new("Failed to plan select: %s", err)
    end

    -- set limit and replicasets to select from
    local limit = opts.limit
    local replicasets_to_select = replicasets

    local scan_index = space.index[plan.index_id]
    local primary_index = space.index[0]
    if select_plan.is_scan_by_full_sharding_key_eq(plan, scan_index, primary_index) then
        limit = 1
        plan.iter = box.index.REQ

        replicasets_to_select = get_replicaset_by_scan_value(plan.scan_value)
    end

    -- set after tuple
    local after_tuple = utils.flatten(opts.after, space_format)

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
        limit = limit,

        batch_size = opts.batch_size,
        replicasets = replicasets_to_select,

        timeout = opts.timeout,
    })

    return iter
end

function select_module.pairs(space_name, user_conditions, opts)
    checks('string', '?table', {
        after = '?',
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

        local obj, err = iter:get()
        if err ~= nil then
            error(string.format("Failed to get next object: %s", err))
        end

        return iter, obj
    end

    return gen, nil, iter
end

function select_module.call(space_name, user_conditions, opts)
    checks('string', '?table', {
        after = '?',
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

    local objects = {}

    while iter:has_next() do
        local obj, err = iter:get()
        if err ~= nil then
            return nil, SelectError:new("Failed to get next object: %s", err)
        end

        if obj == nil then
            break
        end

        table.insert(objects, obj)
    end

    return objects
end

return select_module
