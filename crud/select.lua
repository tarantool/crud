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

local Iterator = require('crud.select.iterator')

require('crud.common.checkers')

local SelectError = errors.new_class('SelectError')
local GetReplicasetsError = errors.new_class('GetReplicasetsError')

local select_module = {}

local SELECT_FUNC_NAME = '__select'

local function call_select_on_storage(space_name, conditions, opts)
    checks('string', '?table', {
        limit = '?number',
        after_tuple = '?table',
    })

    local space = box.space[space_name]
    if space == nil then
        return nil, SelectError:new("Space %s doesn't exists", space_name)
    end

    -- plan select
    local plan, err = select_plan.new(space, conditions, {
        limit = opts.limit,
        after_tuple = opts.after_tuple,
    })

    if err ~= nil then
        return nil, SelectError:new("Failed to plan select: %s", err)
    end

    -- execute select
    local tuples, err = select_executor.execute(plan)
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

local function select_iteration(space_name, conditions, opts)
    checks('string', '?table', {
        after_tuple = '?table',
        replicasets = 'table',
        timeout = '?number',
        batch_size = '?number',
    })

    -- call select on storages
    local storage_select_opts = {
        after_tuple = opts.after_tuple,
        limit = opts.batch_size,
    }

    local storage_select_args = {space_name, conditions, storage_select_opts}

    local results, err = call.ro(SELECT_FUNC_NAME, storage_select_args, {
        replicasets = opts.replicasets,
        timeout = opts.timeout,
    })

    if err ~= nil then
        return nil, err
    end

    return results
end

local function get_replicasets_to_select_from(plan, all_replicasets)
    if not plan.is_scan_by_full_sharding_key_eq then
        return all_replicasets
    end

    plan.scanner.limit = 1

    local bucket_id = vshard.router.bucket_id_strcrc32(plan.scanner.value)
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
        tuples_tomap = '?boolean',
    })

    opts = opts or {}
    if opts.tuples_tomap == nil then
        opts.tuples_tomap = true
    end

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

    local after_tuple = utils.flatten(opts.after, space_format)

    -- plan select
    local plan, err = select_plan.new(space, conditions, {
        limit = opts.limit,
        after_tuple = after_tuple
    })

    if err ~= nil then
        return nil, SelectError:new("Failed to plan select: %s", err)
    end

    -- get replicasets to select from
    local replicasets, err = get_replicasets_to_select_from(plan, replicasets)
    if err ~= nil then
        return nil, SelectError:new("Failed to get replicasets to select from: %s", err)
    end

    local key_parts = space.index[plan.scanner.index_id].parts
    local tuples_comparator, err = select_comparators.gen_tuples_comparator(
        plan.scanner.operator, key_parts
    )
    if err ~= nil then
        return nil, SelectError:new("Failed to generate comparator function: %s", err)
    end

    local iter = Iterator.new({
        space_name = space_name,
        space_format = space_format,
        key_parts = key_parts,
        iteration_func = select_iteration,
        comparator = tuples_comparator,

        conditions = conditions,
        after_tuple = after_tuple,
        limit = plan.scanner.limit,

        batch_size = opts.batch_size,
        replicasets = replicasets,

        timeout = opts.timeout,
        tuples_tomap = opts.tuples_tomap,
    })

    return iter
end

function select_module.pairs(space_name, user_conditions, opts)
    checks('string', '?table', {
        after = '?',
        limit = '?number',
        timeout = '?number',
        batch_size = '?number',
        tuples_tomap = '?boolean',
    })

    opts = opts or {}
    if opts.tuples_tomap == nil then
        opts.tuples_tomap = true
    end

    local iter, err = build_select_iterator(space_name, user_conditions, {
        after = opts.after,
        limit = opts.limit,
        timeout = opts.timeout,
        batch_size = opts.batch_size,
        tuples_tomap = opts.tuples_tomap,
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
        tuples_tomap = '?boolean',
    })

    opts = opts or {}
    if opts.tuples_tomap == nil then
        opts.tuples_tomap = true
    end

    local iter, err = build_select_iterator(space_name, user_conditions, {
        after = opts.after,
        limit = opts.limit,
        timeout = opts.timeout,
        batch_size = opts.batch_size,
        tuples_tomap = opts.tuples_tomap
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
