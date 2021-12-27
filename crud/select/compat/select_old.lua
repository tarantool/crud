local checks = require('checks')
local errors = require('errors')
local vshard = require('vshard')
local fun = require('fun')

local call = require('crud.common.call')
local utils = require('crud.common.utils')
local sharding = require('crud.common.sharding')
local dev_checks = require('crud.common.dev_checks')
local schema = require('crud.common.schema')
local sharding_key_module = require('crud.common.sharding_key')

local compare_conditions = require('crud.compare.conditions')
local select_plan = require('crud.compare.plan')
local select_comparators = require('crud.compare.comparators')
local common = require('crud.select.compat.common')

local Iterator = require('crud.select.iterator')

local SelectError = errors.new_class('SelectError')

local select_module = {}

local function select_iteration(space_name, plan, opts)
    dev_checks('string', '?table', {
        after_tuple = '?table',
        replicasets = 'table',
        limit = 'number',
        call_opts = 'table',
    })

    local call_opts = opts.call_opts

    -- call select on storages
    local storage_select_opts = {
        scan_value = plan.scan_value,
        after_tuple = opts.after_tuple,
        tarantool_iter = plan.tarantool_iter,
        limit = opts.limit,
        scan_condition_num = plan.scan_condition_num,
        field_names = plan.field_names,
    }

    local storage_select_args = {
        space_name, plan.index_id, plan.conditions, storage_select_opts,
    }

    local results, err = call.map(common.SELECT_FUNC_NAME, storage_select_args, {
        replicasets = opts.replicasets,
        timeout = call_opts.timeout,
        mode = call_opts.mode or 'read',
        prefer_replica = call_opts.prefer_replica,
        balance = call_opts.balance,
    })

    if err ~= nil then
        return nil, err
    end

    local tuples = {}
    for replicaset_uuid, replicaset_results in pairs(results) do
        tuples[replicaset_uuid] = replicaset_results[2]
    end

    return tuples
end

-- returns result, err, need_reload
-- need_reload indicates if reloading schema could help
-- see crud.common.schema.wrap_func_reload()
local function build_select_iterator(space_name, user_conditions, opts)
    dev_checks('string', '?table', {
        after = '?table',
        first = '?number',
        batch_size = '?number',
        bucket_id = '?number|cdata',
        force_map_call = '?boolean',
        field_names = '?table',
        call_opts = 'table',
    })

    opts = opts or {}

    if opts.batch_size ~= nil and opts.batch_size < 1 then
        return nil, SelectError:new("batch_size should be > 0")
    end

    local batch_size = opts.batch_size or common.DEFAULT_BATCH_SIZE

    -- check conditions
    local conditions, err = compare_conditions.parse(user_conditions)
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
    local sharding_key_as_index_obj, err = sharding_key_module.fetch_on_router(space_name)
    if err ~= nil then
        return nil, err
    end

    -- plan select
    local plan, err = select_plan.new(space, conditions, {
        first = opts.first,
        after_tuple = opts.after,
        field_names = opts.field_names,
        force_map_call = opts.force_map_call,
        sharding_key_as_index_obj = sharding_key_as_index_obj,
    })

    if err ~= nil then
        return nil, SelectError:new("Failed to plan select: %s", err), true
    end

    -- set replicasets to select from
    local replicasets_to_select = replicasets

    -- See explanation of this logic in
    -- crud/select/compat/select.lua.
    local perform_map_reduce = opts.force_map_call == true or
        (opts.bucket_id == nil and plan.sharding_key == nil)
    if not perform_map_reduce then
        local bucket_id = sharding.key_get_bucket_id(plan.sharding_key, opts.bucket_id)
        assert(bucket_id ~= nil)

        local err
        replicasets_to_select, err = sharding.get_replicasets_by_bucket_id(bucket_id)
        if err ~= nil then
            return nil, err, true
        end
    end

    -- generate tuples comparator
    local scan_index = space.index[plan.index_id]
    local primary_index = space.index[0]
    local cmp_key_parts = utils.merge_primary_key_parts(scan_index.parts, primary_index.parts)
    local cmp_operator = select_comparators.get_cmp_operator(plan.tarantool_iter)

    -- generator of tuples comparator needs field_names and space_format
    -- to update fieldno in each part in cmp_key_parts because storage result contains
    -- fields in order specified by field_names
    local tuples_comparator = select_comparators.gen_tuples_comparator(
        cmp_operator, cmp_key_parts, plan.field_names, space_format
    )

    local function comparator(node1, node2)
        return not tuples_comparator(node1.obj, node2.obj)
    end

    -- filter space format by plan.field_names (user defined fields + primary key + scan key)
    -- to pass it user as metadata
    local filtered_space_format, err = utils.get_fields_format(space_format, plan.field_names)
    if err ~= nil then
        return nil, err
    end

    local iter = Iterator.new({
        space_name = space_name,
        space_format = filtered_space_format,
        iteration_func = select_iteration,
        comparator = comparator,

        plan = plan,

        batch_size = batch_size,
        replicasets = replicasets_to_select,

        call_opts = opts.call_opts,
    })

    return iter
end

function select_module.pairs(space_name, user_conditions, opts)
    checks('string', '?table', {
        after = '?table',
        first = '?number',
        batch_size = '?number',
        use_tomap = '?boolean',
        bucket_id = '?number|cdata',
        force_map_call = '?boolean',
        fields = '?table',

        mode = '?vshard_call_mode',
        prefer_replica = '?boolean',
        balance = '?boolean',
        timeout = '?number',
    })

    opts = opts or {}

    if opts.first ~= nil and opts.first < 0 then
        error(string.format("Negative first isn't allowed for pairs"))
    end

    local iterator_opts = {
        after = opts.after,
        first = opts.first,
        batch_size = opts.batch_size,
        bucket_id = opts.bucket_id,
        force_map_call = opts.force_map_call,
        field_names = opts.fields,
        call_opts = {
            mode = opts.mode,
            prefer_replica = opts.prefer_replica,
            balance = opts.balance,
            timeout = opts.timeout,
        },
    }

    local iter, err = schema.wrap_func_reload(
            build_select_iterator, space_name, user_conditions, iterator_opts
    )

    if err ~= nil then
        error(string.format("Failed to generate iterator: %s", err))
    end

    local tuples_limit = opts.first
    if tuples_limit ~= nil then
        tuples_limit = math.abs(tuples_limit)
    end

    local gen = function(_, iter)
        local tuple, err = iter:get()
        if err ~= nil then
            error(string.format("Failed to get next object: %s", err))
        end

        if tuple == nil then
            return nil
        end

        local result = tuple
        if opts.use_tomap == true then
            result, err = utils.unflatten(tuple, iter.space_format)
            if err ~= nil then
                error(string.format("Failed to unflatten next object: %s", err))
            end
        end

        return iter, result
    end

    local gen, param, state = fun.iter(gen, nil, iter)

    if tuples_limit ~= nil then
        gen, param, state = gen:take_n(tuples_limit)
    end

    return gen, param, state
end

function select_module.call(space_name, user_conditions, opts)
    checks('string', '?table', {
        after = '?table',
        first = '?number',
        timeout = '?number',
        batch_size = '?number',
        bucket_id = '?number|cdata',
        force_map_call = '?boolean',
        fields = '?table',
        prefer_replica = '?boolean',
        balance = '?boolean',
        mode = '?vshard_call_mode',
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
        batch_size = opts.batch_size,
        bucket_id = opts.bucket_id,
        force_map_call = opts.force_map_call,
        field_names = opts.fields,
        call_opts = {
            mode = opts.mode,
            prefer_replica = opts.prefer_replica,
            balance = opts.balance,
            timeout = opts.timeout,
        },
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

    return {
        metadata = table.copy(iter.space_format),
        rows = tuples,
    }
end

return select_module
