local checks = require('checks')
local errors = require('errors')
local fun = require('fun')

local call = require('crud.common.call')
local const = require('crud.common.const')
local utils = require('crud.common.utils')
local sharding = require('crud.common.sharding')
local dev_checks = require('crud.common.dev_checks')
local schema = require('crud.common.schema')
local sharding_metadata_module = require('crud.common.sharding.sharding_metadata')
local stats = require('crud.stats')

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
        sharding_hash = 'table',
        vshard_router = 'table',
        yield_every = 'number',
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
        sharding_func_hash = opts.sharding_hash.sharding_func_hash,
        sharding_key_hash = opts.sharding_hash.sharding_key_hash,
        skip_sharding_hash_check = opts.sharding_hash.skip_sharding_hash_check,
        yield_every = opts.yield_every,
        fetch_latest_metadata = true,
    }

    local storage_select_args = {
        space_name, plan.index_id, plan.conditions, storage_select_opts,
    }

    local results, err, storages_info = call.map(opts.vshard_router, common.SELECT_FUNC_NAME, storage_select_args, {
        replicasets = opts.replicasets,
        timeout = call_opts.timeout,
        mode = call_opts.mode or 'read',
        prefer_replica = call_opts.prefer_replica,
        balance = call_opts.balance,
    })

    if err ~= nil then
        return nil, err, storages_info
    end

    local tuples = {}
    -- Old select works with vshard without `name_as_key` support.
    for replicaset_uuid, replicaset_results in pairs(results) do
        -- Stats extracted with callback here and not passed
        -- outside to wrapper because fetch for pairs can be
        -- called even after pairs() return from generators.
        local cursor = replicaset_results[1]
        if cursor.stats ~= nil then
            stats.update_fetch_stats(cursor.stats, space_name)
        end

        tuples[replicaset_uuid] = replicaset_results[2]
    end

    return tuples, nil, storages_info
end

-- returns result, err, need_reload
-- need_reload indicates if reloading schema could help
-- see crud.common.schema.wrap_func_reload()
local function build_select_iterator(vshard_router, space_name, user_conditions, opts)
    dev_checks('table', 'string', '?table', {
        after = '?table',
        first = '?number',
        batch_size = '?number',
        bucket_id = '?number|cdata',
        force_map_call = '?boolean',
        field_names = '?table',
        yield_every = '?number',
        call_opts = 'table',
    })

    opts = opts or {}

    if opts.batch_size ~= nil and opts.batch_size < 1 then
        return nil, SelectError:new("batch_size should be > 0")
    end

    if opts.yield_every ~= nil and opts.yield_every < 1 then
        return nil, SelectError:new("yield_every should be > 0")
    end

    local yield_every = opts.yield_every or const.DEFAULT_YIELD_EVERY

    local batch_size = opts.batch_size or common.DEFAULT_BATCH_SIZE

    -- check conditions
    local conditions, err = compare_conditions.parse(user_conditions)
    if err ~= nil then
        return nil, SelectError:new("Failed to parse conditions: %s", err)
    end

    local space, err, netbox_schema_version = utils.get_space(space_name, vshard_router)
    if err ~= nil then
        return nil, SelectError:new("An error occurred during the operation: %s", err), const.NEED_SCHEMA_RELOAD
    end
    if space == nil then
        return nil, SelectError:new("Space %q doesn't exist", space_name), const.NEED_SCHEMA_RELOAD
    end

    local sharding_hash = {}
    local sharding_key_as_index_obj = nil
    -- We don't need sharding info if bucket_id specified.
    if opts.bucket_id == nil then
        local sharding_key_data, err = sharding_metadata_module.fetch_sharding_key_on_router(vshard_router, space_name)
        if err ~= nil then
            return nil, err
        end

        sharding_key_as_index_obj = sharding_key_data.value
        sharding_hash.sharding_key_hash = sharding_key_data.hash
    else
        sharding_hash.skip_sharding_hash_check = true
    end

    -- plan select
    local plan, err = select_plan.new(space, conditions, {
        first = opts.first,
        after_tuple = opts.after,
        field_names = opts.field_names,
        force_map_call = opts.force_map_call,
        sharding_key_as_index_obj = sharding_key_as_index_obj,
        bucket_id = opts.bucket_id,
    })

    if err ~= nil then
        return nil, SelectError:new("Failed to plan select: %s", err), const.NEED_SCHEMA_RELOAD
    end

    -- set replicasets to select from
    local replicasets_to_select, err = vshard_router:routeall()
    if err ~= nil then
        return nil, SelectError:new("Failed to get router replicasets: %s", err)
    end

    -- See explanation of this logic in
    -- crud/select/compat/select.lua.
    local perform_map_reduce = opts.force_map_call == true or
        (opts.bucket_id == nil and plan.sharding_key == nil)
    if not perform_map_reduce then
        local bucket_id_data, err = sharding.key_get_bucket_id(vshard_router, space_name,
                                                               plan.sharding_key, opts.bucket_id)
        if err ~= nil then
            return nil, err
        end

        assert(bucket_id_data.bucket_id ~= nil)

        local err
        replicasets_to_select, err = sharding.get_replicasets_by_bucket_id(vshard_router, bucket_id_data.bucket_id)
        if err ~= nil then
            return nil, err, const.NEED_SCHEMA_RELOAD
        end

        sharding_hash.sharding_func_hash = bucket_id_data.sharding_func_hash
    else
        stats.update_map_reduces(space_name)
        sharding_hash.skip_sharding_hash_check = true
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
        cmp_operator, cmp_key_parts, plan.field_names, space:format()
    )

    local function comparator(node1, node2)
        return not tuples_comparator(node1.obj, node2.obj)
    end

    local iter = Iterator.new({
        space_name = space_name,
        space = space,
        netbox_schema_version = netbox_schema_version,
        iteration_func = select_iteration,
        comparator = comparator,

        plan = plan,

        batch_size = batch_size,
        replicasets = replicasets_to_select,

        call_opts = opts.call_opts,
        sharding_hash = sharding_hash,
        vshard_router = vshard_router,
        yield_every = yield_every,
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
        fetch_latest_metadata = '?boolean',

        mode = '?vshard_call_mode',
        prefer_replica = '?boolean',
        balance = '?boolean',
        timeout = '?number',

        vshard_router = '?string|table',

        yield_every = '?number',
    })

    opts = opts or {}

    if opts.first ~= nil and opts.first < 0 then
        error(string.format("Negative first isn't allowed for pairs"))
    end

    local vshard_router, err = utils.get_vshard_router_instance(opts.vshard_router)
    if err ~= nil then
        error(err)
    end

    local iterator_opts = {
        after = opts.after,
        first = opts.first,
        batch_size = opts.batch_size,
        bucket_id = opts.bucket_id,
        force_map_call = opts.force_map_call,
        field_names = opts.fields,
        yield_every = opts.yield_every,
        call_opts = {
            mode = opts.mode,
            prefer_replica = opts.prefer_replica,
            balance = opts.balance,
            timeout = opts.timeout,
            fetch_latest_metadata = opts.fetch_latest_metadata,
        },
    }

    local iter, err = schema.wrap_func_reload(
            vshard_router, build_select_iterator, space_name, user_conditions, iterator_opts
    )

    if err ~= nil then
        error(string.format("Failed to generate iterator: %s", err))
    end

    local tuples_limit = opts.first
    if tuples_limit ~= nil then
        tuples_limit = math.abs(tuples_limit)
    end

    -- filter space format by plan.field_names (user defined fields + primary key + scan key)
    -- to pass it user as metadata
    local filtered_space_format, err = utils.get_fields_format(iter.space:format(), iter.plan.field_names)
    if err ~= nil then
        return nil, err
    end

    local gen = function(_, iter)
        local tuple, err = iter:get()
        if err ~= nil then
            if sharding.result_needs_sharding_reload(err) then
                sharding_metadata_module.reload_sharding_cache(vshard_router, space_name)
            end

            error(string.format("Failed to get next object: %s", err))
        end

        if tuple == nil then
            return nil
        end

        if opts.fetch_latest_metadata then
            -- This option is temporary and is related to [1], [2].
            -- [1] https://github.com/tarantool/crud/issues/236
            -- [2] https://github.com/tarantool/crud/issues/361
            iter = utils.fetch_latest_metadata_for_select(space_name, vshard_router, opts,
                                                          iter.storages_info, iter)
            filtered_space_format, err = utils.get_fields_format(iter.space:format(), iter.plan.field_names)
            if err ~= nil then
                return nil, err
            end
        end

        local result = tuple
        if opts.use_tomap == true then
            result, err = utils.unflatten(tuple, filtered_space_format)
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

local function select_module_call_xc(vshard_router, space_name, user_conditions, opts)
    dev_checks('table', 'string', '?table', 'table')

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
        yield_every = opts.yield_every,
        call_opts = {
            mode = opts.mode,
            prefer_replica = opts.prefer_replica,
            balance = opts.balance,
            timeout = opts.timeout,
            fetch_latest_metadata = opts.fetch_latest_metadata,
        },
    }

    local iter, err = schema.wrap_func_reload(
            vshard_router, build_select_iterator, space_name, user_conditions, iterator_opts
    )
    if err ~= nil then
        return nil, err
    end
    common.check_select_safety(space_name, iter.plan, opts)

    local tuples = {}

    while iter:has_next() do
        local tuple, err = iter:get()
        if err ~= nil then
            if sharding.result_needs_sharding_reload(err) then
                return nil, err, const.NEED_SHARDING_RELOAD
            end

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

    if opts.fetch_latest_metadata then
        -- This option is temporary and is related to [1], [2].
        -- [1] https://github.com/tarantool/crud/issues/236
        -- [2] https://github.com/tarantool/crud/issues/361
        iter = utils.fetch_latest_metadata_for_select(space_name, vshard_router, opts,
                                                      iter.storages_info, iter)
    end

    -- filter space format by plan.field_names (user defined fields + primary key + scan key)
    -- to pass it user as metadata
    local filtered_space_format, err = utils.get_fields_format(iter.space:format(), iter.plan.field_names)
    if err ~= nil then
        return nil, err
    end

    return {
        metadata = table.copy(filtered_space_format),
        rows = tuples,
    }
end

function select_module.call(space_name, user_conditions, opts)
    checks('string', '?table', {
        after = '?table',
        first = '?number',
        batch_size = '?number',
        bucket_id = '?number|cdata',
        force_map_call = '?boolean',
        fields = '?table',
        fullscan = '?boolean',
        fetch_latest_metadata = '?boolean',

        mode = '?vshard_call_mode',
        prefer_replica = '?boolean',
        balance = '?boolean',
        timeout = '?number',

        vshard_router = '?string|table',

        yield_every = '?number',
    })

    opts = opts or {}

    local vshard_router, err = utils.get_vshard_router_instance(opts.vshard_router)
    if err ~= nil then
        return nil, SelectError:new(err)
    end

    return sharding.wrap_method(vshard_router, select_module_call_xc, space_name, user_conditions, opts)
end

return select_module
