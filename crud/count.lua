local checks = require('checks')
local errors = require('errors')
local fiber = require('fiber')

local call = require('crud.common.call')
local const = require('crud.common.const')
local utils = require('crud.common.utils')
local sharding = require('crud.common.sharding')
local filters = require('crud.compare.filters')
local count_plan = require('crud.compare.plan')
local dev_checks = require('crud.common.dev_checks')
local ratelimit = require('crud.ratelimit')
local schema = require('crud.common.schema')
local sharding_metadata_module = require('crud.common.sharding.sharding_metadata')

local compare_conditions = require('crud.compare.conditions')

local CountError = errors.new_class('CountError', {capture_stack = false})

local COUNT_FUNC_NAME = 'count_on_storage'
local CRUD_COUNT_FUNC_NAME = utils.get_storage_call(COUNT_FUNC_NAME)

local count = {}

local function count_on_storage(space_name, index_id, conditions, opts)
    dev_checks('string', 'number', '?table', {
        scan_value = 'table|cdata',
        tarantool_iter = 'number',
        yield_every = '?number',
        scan_condition_num = '?number',
        sharding_func_hash = '?number',
        sharding_key_hash = '?number',
        skip_sharding_hash_check = '?boolean',
    })

    opts = opts or {}

    local space = box.space[space_name]

    local index = space.index[index_id]
    if index == nil then
        return nil, CountError:new("Index with ID %s doesn't exist", index_id)
    end

    local _, err = sharding.check_sharding_hash(space_name,
                                                opts.sharding_func_hash,
                                                opts.sharding_key_hash,
                                                opts.skip_sharding_hash_check)
    if err ~= nil then
        return nil, err
    end

    local value = opts.scan_value

    local filter_func, err = filters.gen_func(space, index, conditions, {
        tarantool_iter = opts.tarantool_iter,
        scan_condition_num = opts.scan_condition_num,
    })
    if err ~= nil then
        return nil, CountError:new("Failed to generate tuples filter: %s", err)
    end

    local tuples_count = 0
    local looked_up_tuples = 0

    for _, tuple in index:pairs(value, {iterator = opts.tarantool_iter}) do
        if tuple == nil then
            break
        end

        looked_up_tuples = looked_up_tuples + 1

        local matched, early_exit = filter_func(tuple)

        if matched then
            tuples_count = tuples_count + 1

            if opts.yield_every ~= nil and looked_up_tuples % opts.yield_every == 0 then
                fiber.yield()
            end
        elseif early_exit then
            break
        end
    end

    return tuples_count
end

count.storage_api = {[COUNT_FUNC_NAME] = count_on_storage}

local check_count_safety_rl = ratelimit.new()
local function check_count_safety(space_name, plan, opts)
    if opts.fullscan == true then
        return
    end

    local iter = plan.tarantool_iter
    if iter == box.index.EQ or iter == box.index.REQ then
        return
    end

    local rl = check_count_safety_rl
    local traceback = debug.traceback()
    rl:log_crit("Potentially long count from space '%s'\n %s", space_name, traceback)
end

-- returns result, err, need_reload
-- need_reload indicates if reloading schema could help
-- see crud.common.schema.wrap_func_reload()
local function call_count_on_router(vshard_router, space_name, user_conditions, opts)
    checks('table', 'string', '?table', {
        timeout = '?number',
        bucket_id = '?number|cdata',
        force_map_call = '?boolean',
        fullscan = '?boolean',
        yield_every = '?number',
        prefer_replica = '?boolean',
        balance = '?boolean',
        mode = '?string',
        vshard_router = '?string|table',
    })

    if opts.yield_every ~= nil and opts.yield_every < 1 then
        return nil, CountError:new("yield_every should be > 0")
    end

    -- check conditions
    local conditions, err = compare_conditions.parse(user_conditions)
    if err ~= nil then
        return nil, CountError:new("Failed to parse conditions: %s", err)
    end

    local space, err = utils.get_space(space_name, vshard_router, opts.timeout)
    if err ~= nil then
        return nil, CountError:new("An error occurred during the operation: %s", err), const.NEED_SCHEMA_RELOAD
    end
    if space == nil then
        return nil, CountError:new("Space %q doesn't exist", space_name), const.NEED_SCHEMA_RELOAD
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

    -- plan count
    local plan, err = count_plan.new(space, conditions, {
        sharding_key_as_index_obj = sharding_key_data.value,
    })
    if err ~= nil then
        return nil, CountError:new("Failed to plan count: %s", err), const.NEED_SCHEMA_RELOAD
    end
    check_count_safety(space_name, plan, opts)

    -- set replicasets to count from
    local replicasets_to_count, err = vshard_router:routeall()
    if err ~= nil then
        return nil, CountError:new("Failed to get router replicasets: %s", err)
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

        sharding_func_hash = bucket_id_data.sharding_func_hash

        local err
        replicasets_to_count, err = sharding.get_replicasets_by_bucket_id(vshard_router, bucket_id_data.bucket_id)
        if err ~= nil then
            return nil, err, const.NEED_SCHEMA_RELOAD
        end
    else
        skip_sharding_hash_check = true
    end

    local yield_every = opts.yield_every or const.DEFAULT_YIELD_EVERY

    local call_opts = {
        mode = opts.mode or 'read',
        prefer_replica = opts.prefer_replica,
        balance = opts.balance,
        timeout = opts.timeout,
        replicasets = replicasets_to_count,
    }

    local count_opts = {
        scan_value = plan.scan_value,
        tarantool_iter = plan.tarantool_iter,
        yield_every = yield_every,
        scan_condition_num = plan.scan_condition_num,
        sharding_func_hash = sharding_func_hash,
        sharding_key_hash = sharding_key_data.hash,
        skip_sharding_hash_check = skip_sharding_hash_check,
    }

    local results, err = call.map(vshard_router, CRUD_COUNT_FUNC_NAME, {
        space_name, plan.index_id, plan.conditions, count_opts
    }, call_opts)

    if err ~= nil then
        local err_wrapped = CountError:new("Failed to call count on storage-side: %s", err)

        if sharding.result_needs_sharding_reload(err) then
            return nil, err_wrapped, const.NEED_SHARDING_RELOAD
        end

        return nil, err_wrapped
    end

    if results.err ~= nil then
        return nil, CountError:new("Failed to call count: %s", err)
    end

    local total_count = 0
    for _, replicaset_results in pairs(results) do
        if replicaset_results[1] ~= nil then
            total_count = total_count + replicaset_results[1]
        end
    end

    return total_count
end

--- Calculates the number of tuples by conditions
--
-- @function call
--
-- @param string space_name
--  A space name
--
-- @param ?table user_conditions
--  Conditions by which tuples are counted,
--  default value is nil
--
-- @tparam ?number opts.timeout
--  Function call timeout in seconds,
--  default value is 2 seconds
--
-- @tparam ?number opts.bucket_id
--  Bucket ID
--  default is vshard.router.bucket_id_strcrc32 of primary key
--
-- @tparam ?boolean opts.force_map_call
--  Call is performed without any optimizations
--  default is `false`
--
-- @tparam ?number opts.yield_every
--  Number of tuples processed to yield after,
--  default value is 1000
--
-- @tparam ?boolean opts.prefer_replica
--  Call on replica if it's possible,
--  default value is `nil`, which works as with `false`
--
-- @tparam ?boolean opts.balance
--  Use replica according to round-robin load balancing
--  default value is `nil`, which works as with `false`
--
-- @tparam ?string opts.mode
--  vshard call mode, default value is `read`
--
-- @tparam ?string|table opts.vshard_router
--  Cartridge vshard group name or vshard router instance.
--  Set this parameter if your space is not a part of the
--  default vshard cluster.
--
-- @return[1] number
-- @treturn[2] nil
-- @treturn[2] table Error description
--
function count.call(space_name, user_conditions, opts)
    checks('string', '?table', {
        timeout = '?number',
        bucket_id = '?number|cdata',
        force_map_call = '?boolean',
        fullscan = '?boolean',
        yield_every = '?number',
        prefer_replica = '?boolean',
        balance = '?boolean',
        mode = '?string',
        vshard_router = '?string|table',
    })

    opts = opts or {}

    local vshard_router, err = utils.get_vshard_router_instance(opts.vshard_router)
    if err ~= nil then
        return nil, CountError:new(err)
    end

    return schema.wrap_func_reload(vshard_router, sharding.wrap_method, call_count_on_router,
                                   space_name, user_conditions, opts)
end

return count
