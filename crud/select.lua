local checks = require('checks')
local errors = require('errors')
local vshard = require('vshard')
local fun = require('fun')

local call = require('crud.common.call')
local utils = require('crud.common.utils')
local sharding = require('crud.common.sharding')
local dev_checks = require('crud.common.dev_checks')
local schema = require('crud.common.schema')

local select_conditions = require('crud.select.conditions')
local select_plan = require('crud.select.plan')
local select_executor = require('crud.select.executor')
local select_comparators = require('crud.select.comparators')
local select_filters = require('crud.select.filters')

local Iterator = require('crud.select.iterator')

local SelectError = errors.new_class('SelectError')
local GetReplicasetsError = errors.new_class('GetReplicasetsError')

local select_module = {}

local SELECT_FUNC_NAME = '_crud.select_on_storage'

local DEFAULT_BATCH_SIZE = 100

local function select_on_storage(space_name, index_id, conditions, opts)
    dev_checks('string', 'number', '?table', {
        scan_value = 'table',
        after_tuple = '?table',
        iter = 'number',
        limit = 'number',
        scan_condition_num = '?number',
        field_names = '?table',
    })

    local space = box.space[space_name]
    if space == nil then
        return nil, SelectError:new("Space %q doesn't exist", space_name)
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

    return schema.filter_tuples_fields(tuples, opts.field_names)
end

function select_module.init()
   _G._crud.select_on_storage = select_on_storage
end

local function select_iteration(space_name, plan, opts)
    dev_checks('string', '?table', {
        after_tuple = '?table',
        replicasets = 'table',
        timeout = '?number',
        limit = 'number',
        field_names = '?table',
    })

    -- call select on storages
    local storage_select_opts = {
        scan_value = plan.scan_value,
        after_tuple = opts.after_tuple,
        iter = plan.iter,
        limit = opts.limit,
        scan_condition_num = plan.scan_condition_num,
        field_names = opts.field_names,
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

local function get_replicasets_by_sharding_key(bucket_id)
    local replicaset, err = vshard.router.route(bucket_id)
    if replicaset == nil then
        return nil, GetReplicasetsError:new("Failed to get replicaset for bucket_id %s: %s", bucket_id, err.err)
    end

    return {
        [replicaset.uuid] = replicaset,
    }
end

-- returns result, err, need_reload
-- need_reload indicates if reloading schema could help
-- see crud.common.schema.wrap_func_reload()
local function build_select_iterator(space_name, user_conditions, opts)
    dev_checks('string', '?table', {
        after = '?table',
        first = '?number',
        timeout = '?number',
        batch_size = '?number',
        bucket_id = '?number|cdata',
        field_names = '?table',
    })

    opts = opts or {}

    if opts.batch_size ~= nil and opts.batch_size < 1 then
        return nil, SelectError:new("batch_size should be > 0")
    end

    local batch_size = opts.batch_size or DEFAULT_BATCH_SIZE

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
        return nil, SelectError:new("Space %q doesn't exist", space_name), true
    end

    local space_format = space:format()

    -- plan select
    local plan, err = select_plan.new(space, conditions, {
        first = opts.first,
        after_tuple = opts.after,
    })

    if err ~= nil then
        return nil, SelectError:new("Failed to plan select: %s", err), true
    end

    -- set replicasets to select from
    local replicasets_to_select = replicasets

    if plan.sharding_key ~= nil then
        local bucket_id = sharding.key_get_bucket_id(plan.sharding_key, opts.bucket_id)

        local err
        replicasets_to_select, err = get_replicasets_by_sharding_key(bucket_id)
        if err ~= nil then
            return nil, err, true
        end
    end

    -- generate tuples comparator
    local scan_index = space.index[plan.index_id]
    local primary_index = space.index[0]
    local cmp_key_parts = utils.merge_primary_key_parts(scan_index.parts, primary_index.parts)
    local merged_result = utils.merge_comparison_fields(space_format, cmp_key_parts, opts.field_names)
    local cmp_operator = select_comparators.get_cmp_operator(plan.iter)
    local tuples_comparator, err = select_comparators.gen_tuples_comparator(
        cmp_operator, merged_result.key_parts
    )
    if err ~= nil then
        return nil, SelectError:new("Failed to generate comparator function: %s", err)
    end

    local filtered_space_format, err = utils.get_fields_format(space_format, merged_result.field_names)

    if err ~= nil then
        return nil, err
    end

    local iter = Iterator.new({
        space_name = space_name,
        space_format = filtered_space_format,
        iteration_func = select_iteration,
        comparator = tuples_comparator,

        plan = plan,

        batch_size = batch_size,
        replicasets = replicasets_to_select,

        timeout = opts.timeout,
        field_names = merged_result.field_names,
    })

    return iter
end

function select_module.pairs(space_name, user_conditions, opts)
    checks('string', '?table', {
        after = '?table',
        first = '?number',
        timeout = '?number',
        batch_size = '?number',
        use_tomap = '?boolean',
        bucket_id = '?number|cdata',
        fields = '?table',
    })

    opts = opts or {}

    if opts.first ~= nil and opts.first < 0 then
        error(string.format("Negative first isn't allowed for pairs"))
    end

    local iterator_opts = {
        after = opts.after,
        first = opts.first,
        timeout = opts.timeout,
        batch_size = opts.batch_size,
        bucket_id = opts.bucket_id,
        field_names = opts.fields,
    }

    local iter, err = schema.wrap_func_reload(
        build_select_iterator, space_name, user_conditions, iterator_opts
    )

    if err ~= nil then
        error(string.format("Failed to generate iterator: %s", err))
    end

    local gen = function(_, iter)
        local tuple, err = iter:get()
        if tuple == nil then
            return nil
        end

        if err ~= nil then
            error(string.format("Failed to get next object: %s", err))
        end

        local space_format, err = utils.get_fields_format(iter.space_format, opts.fields)

        if err ~= nil then
            return nil, err
        end

        tuple = schema.truncate_tuple_trailing_fields(tuple, opts.fields)
        local result = tuple
        if opts.use_tomap == true then
            result, err = utils.unflatten(tuple, space_format)
            if err ~= nil then
                error(string.format("Failed to unflatten next object: %s", err))
            end
        end

        return iter, result
    end

    return fun.iter(gen, nil, iter)
end

function select_module.call(space_name, user_conditions, opts)
    checks('string', '?table', {
        after = '?table',
        first = '?number',
        timeout = '?number',
        batch_size = '?number',
        bucket_id = '?number|cdata',
        fields = '?table',
    })

    opts = opts or {}

    if opts.first ~= nil and opts.first < 0 then
        if opts.after == nil then
            return nil, SelectError:new("Negative first should be specified only with after option")
        end
    end

    local iterator_opts = {
        after = opts.after,
        first = opts.first,
        timeout = opts.timeout,
        batch_size = opts.batch_size,
        bucket_id = opts.bucket_id,
        field_names = opts.fields,
    }

    local iter, err = schema.wrap_func_reload(
        build_select_iterator, space_name, user_conditions, iterator_opts
    )
    if err ~= nil then
        return nil, err
    end

    local tuples = {}

    while iter:has_next() do
        local tuple, err = iter:get()
        if err ~= nil then
            return nil, SelectError:new("Failed to get next object: %s", err)
        end

        if tuple == nil then
            break
        end

        table.insert(tuples, tuple)
    end

    if opts.first ~= nil and opts.first < 0 then
        utils.reverse_inplace(tuples)
    end

    local filtered_space_format, err = utils.get_fields_format(iter.space_format, opts.fields)

    if err ~= nil then
        return nil, err
    end

    local rows = schema.truncate_tuples_trailing_fields(tuples, opts.fields)

    return {
        metadata = filtered_space_format,
        rows = rows,
    }
end

return select_module
