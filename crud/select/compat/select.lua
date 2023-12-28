local checks = require('checks')
local errors = require('errors')

local fiber = require('fiber')
local const = require('crud.common.const')
local utils = require('crud.common.utils')
local sharding = require('crud.common.sharding')
local dev_checks = require('crud.common.dev_checks')
local common = require('crud.select.compat.common')
local schema = require('crud.common.schema')
local sharding_metadata_module = require('crud.common.sharding.sharding_metadata')
local stats = require('crud.stats')

local compare_conditions = require('crud.compare.conditions')
local select_plan = require('crud.compare.plan')

local Merger = require('crud.select.merger')

local SelectError = errors.new_class('SelectError')

local select_module = {}

local function build_select_iterator(vshard_router, space_name, user_conditions, opts)
    dev_checks('table', 'string', '?table', {
        after = '?table|cdata',
        first = '?number',
        batch_size = '?number',
        bucket_id = '?number|cdata',
        force_map_call = '?boolean',
        field_names = '?table',
        yield_every = '?number',
        call_opts = 'table',
        readview = '?boolean',
        readview_info = '?table',
    })

    opts = opts or {}

    if opts.batch_size ~= nil and opts.batch_size < 1 then
        return nil, SelectError:new("batch_size should be > 0")
    end

    if opts.yield_every ~= nil and opts.yield_every < 1 then
        return nil, SelectError:new("yield_every should be > 0")
    end

    local yield_every = opts.yield_every or const.DEFAULT_YIELD_EVERY

    -- check conditions
    local conditions, err = compare_conditions.parse(user_conditions)
    if err ~= nil then
        return nil, SelectError:new("Failed to parse conditions: %s", err)
    end

    local space, err, netbox_schema_version  = utils.get_space(space_name, vshard_router)
    if err ~= nil then
        return nil, SelectError:new("An error occurred during the operation: %s", err), const.NEED_SCHEMA_RELOAD
    end
    if space == nil then
        return nil, SelectError:new("Space %q doesn't exist", space_name), const.NEED_SCHEMA_RELOAD
    end

    local sharding_key_data = {}
    local sharding_func_hash = nil
    local skip_sharding_hash_check = nil

    -- We don't need sharding info if bucket_id specified.
    if opts.bucket_id == nil then
        sharding_key_data, err = sharding_metadata_module.fetch_sharding_key_on_router(vshard_router, space_name)
        if err ~= nil then
            return nil, err
        end
    else
        skip_sharding_hash_check = true
    end

    -- plan select
    local plan, err = select_plan.new(space, conditions, {
        first = opts.first,
        after_tuple = opts.after,
        field_names = opts.field_names,
        sharding_key_as_index_obj = sharding_key_data.value,
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

    -- Whether to call one storage replicaset or perform
    -- map-reduce?
    --
    -- If map-reduce is requested explicitly, ignore provided
    -- bucket_id and fetch data from all storage replicasets.
    --
    -- Otherwise:
    --
    -- 1. If particular replicaset is pointed by a caller (using
    --    the bucket_id option[^1]), crud MUST fetch data only
    --    from this storage replicaset: disregarding whether other
    --    storages have tuples that fit given condition.
    --
    -- 2. If a replicaset may be deduced from conditions
    --    (conditions -> sharding key -> bucket id -> replicaset),
    --    fetch data only from the replicaset. It does not change
    --    the result[^2], but significantly reduces network
    --    pressure.
    --
    -- 3. Fallback to map-reduce otherwise.
    --
    -- [^1]: We can change meaning of this option in a future,
    --       see gh-190. But now bucket_id points a storage
    --       replicaset, not a virtual bucket.
    --
    -- [^2]: It is correct statement only if we'll turn a blind
    --       eye to resharding. However, AFAIU, the optimization
    --       does not make the result less consistent (sounds
    --       weird, huh?).
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

        sharding_func_hash = bucket_id_data.sharding_func_hash
    else
        stats.update_map_reduces(space_name)
        skip_sharding_hash_check = true
    end

    local tuples_limit = opts.first
    if tuples_limit ~= nil then
        tuples_limit = math.abs(tuples_limit)
    end

    -- If opts.batch_size is missed we should specify it to min(tuples_limit, DEFAULT_BATCH_SIZE)
    local batch_size
    if opts.batch_size == nil then
        if tuples_limit ~= nil and tuples_limit < common.DEFAULT_BATCH_SIZE then
            batch_size = tuples_limit
        else
            batch_size = common.DEFAULT_BATCH_SIZE
        end
    else
        batch_size = opts.batch_size
    end

    local select_opts = {
        scan_value = plan.scan_value,
        after_tuple = plan.after_tuple,
        tarantool_iter = plan.tarantool_iter,
        limit = batch_size,
        scan_condition_num = plan.scan_condition_num,
        field_names = plan.field_names,
        sharding_func_hash = sharding_func_hash,
        sharding_key_hash = sharding_key_data.hash,
        skip_sharding_hash_check = skip_sharding_hash_check,
        yield_every = yield_every,
        fetch_latest_metadata = opts.call_opts.fetch_latest_metadata,
    }
    local merger

    if opts.readview then
        merger = Merger.new_readview(vshard_router, replicasets_to_select, opts.readview_info,
        space, plan.index_id, common.READVIEW_SELECT_FUNC_NAME,
        {space_name, plan.index_id, plan.conditions, select_opts},
        {tarantool_iter = plan.tarantool_iter, field_names = plan.field_names, call_opts = opts.call_opts}
    )
    else
        merger = Merger.new(vshard_router, replicasets_to_select, space, plan.index_id,
                common.SELECT_FUNC_NAME,
                {space_name, plan.index_id, plan.conditions, select_opts},
                {tarantool_iter = plan.tarantool_iter, field_names = plan.field_names, call_opts = opts.call_opts}
            )
    end
    return {
        tuples_limit = tuples_limit,
        merger = merger,
        plan = plan,
        space = space,
        netbox_schema_version = netbox_schema_version,
    }
end

function select_module.pairs(space_name, user_conditions, opts)
    checks('string', '?table', {
        after = '?table|cdata',
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
        readview = '?boolean',
        readview_info = '?table',

        vshard_router = '?string|table',

        yield_every = '?number',
    })

    opts = opts or {}

    if opts.readview == true then
        if opts.mode ~= nil then
            return nil, SelectError:new("Readview does not support 'mode' option")
        end

        if opts.prefer_replica ~= nil then
            return nil, SelectError:new("Readview does not support 'prefer_replica' option")
        end

        if opts.balance ~= nil then
            return nil, SelectError:new("Readview does not support 'balance' option")
        end

        if opts.vshard_router ~= nil then
            return nil, SelectError:new("Readview does not support 'vshard_router' option")
        end
    end

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
        readview = opts.readview,
        readview_info = opts.readview_info,
    }

    local iter, err = schema.wrap_func_reload(
            vshard_router, build_select_iterator, space_name, user_conditions, iterator_opts
    )

    if err ~= nil then
        error(string.format("Failed to generate iterator: %s", err))
    end

    -- filter space format by plan.field_names (user defined fields + primary key + scan key)
    -- to pass it user as metadata
    local filtered_space_format, err = utils.get_fields_format(iter.space:format(), iter.plan.field_names)
    if err ~= nil then
        return nil, err
    end

    local gen, param, state = iter.merger:pairs()
    if opts.use_tomap == true then
        gen, param, state = gen:map(function(tuple)
            if opts.fetch_latest_metadata then
                -- This option is temporary and is related to [1], [2].
                -- [1] https://github.com/tarantool/crud/issues/236
                -- [2] https://github.com/tarantool/crud/issues/361
                local storages_info = fiber.self().storage.storages_info_on_select
                iter = utils.fetch_latest_metadata_for_select(space_name, vshard_router, opts,
                                                              storages_info, iter)
                filtered_space_format, err = utils.get_fields_format(iter.space:format(), iter.plan.field_names)
                if err ~= nil then
                    return nil, err
                end
            end
            local result
            result, err = utils.unflatten(tuple, filtered_space_format)
            if err ~= nil then
                error(string.format("Failed to unflatten next object: %s", err))
            end
            return result
        end)
    end

    if iter.tuples_limit ~= nil then
        gen, param, state = gen:take_n(iter.tuples_limit)
    end

    return gen, param, state
end

local function select_module_call_xc(vshard_router, space_name, user_conditions, opts)
    checks('table', 'string', '?table', {
        after = '?table|cdata',
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
        readview = '?boolean',
        readview_info = '?table',

        vshard_router = '?string|table',

        yield_every = '?number',
    })

    if opts.readview == true then
        if opts.mode ~= nil then
            return nil, SelectError:new("Readview does not support 'mode' option")
        end

        if opts.prefer_replica ~= nil then
            return nil, SelectError:new("Readview does not support 'prefer_replica' option")
        end

        if opts.balance ~= nil then
            return nil, SelectError:new("Readview does not support 'balance' option")
        end

        if opts.vshard_router ~= nil then
            return nil, SelectError:new("Readview does not support 'vshard_router' option")
        end
    end

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
        readview = opts.readview,
        readview_info = opts.readview_info,
    }

    local iter, err = schema.wrap_func_reload(
            vshard_router, build_select_iterator, space_name, user_conditions, iterator_opts
    )

    if err ~= nil then
        return nil, err
    end
    common.check_select_safety(space_name, iter.plan, opts)

    local tuples = {}

    local count = 0
    local first = opts.first and math.abs(opts.first)
    for _, tuple in iter.merger:pairs() do
        if first ~= nil and count >= first then
            break
        end

        table.insert(tuples, tuple)
        count = count + 1
    end

    if opts.first ~= nil and opts.first < 0 then
        utils.reverse_inplace(tuples)
    end

    if opts.fetch_latest_metadata then
        -- This option is temporary and is related to [1], [2].
        -- [1] https://github.com/tarantool/crud/issues/236
        -- [2] https://github.com/tarantool/crud/issues/361
        local storages_info = fiber.self().storage.storages_info_on_select
        iter = utils.fetch_latest_metadata_for_select(space_name, vshard_router, opts,
                                                      storages_info, iter)
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
    opts = opts or {}

    local vshard_router, err = utils.get_vshard_router_instance(opts.vshard_router)
    if err ~= nil then
        return nil, SelectError:new(err)
    end

    return SelectError:pcall(sharding.wrap_select_method, vshard_router, select_module_call_xc,
                             space_name, user_conditions, opts)
end

return select_module
