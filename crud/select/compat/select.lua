local checks = require('checks')
local errors = require('errors')
local vshard = require('vshard')

local utils = require('crud.common.utils')
local sharding = require('crud.common.sharding')
local dev_checks = require('crud.common.dev_checks')
local common = require('crud.select.compat.common')
local schema = require('crud.common.schema')

local compare_conditions = require('crud.compare.conditions')
local select_plan = require('crud.select.plan')

local Merger = require('crud.select.merger')

local SelectError = errors.new_class('SelectError')

local select_module = {}

local function build_select_iterator(space_name, user_conditions, opts)
    dev_checks('string', '?table', {
        after = '?table|cdata',
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

    -- plan select
    local plan, err = select_plan.new(space, conditions, {
        first = opts.first,
        after_tuple = opts.after,
        field_names = opts.field_names,
    })

    if err ~= nil then
        return nil, SelectError:new("Failed to plan select: %s", err), true
    end

    -- set replicasets to select from
    local replicasets_to_select = replicasets

    if plan.sharding_key ~= nil and opts.force_map_call ~= true then
        local bucket_id = sharding.key_get_bucket_id(plan.sharding_key, opts.bucket_id)

        local err
        replicasets_to_select, err = common.get_replicasets_by_sharding_key(bucket_id)
        if err ~= nil then
            return nil, err, true
        end
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
    }

    local merger = Merger.new(replicasets_to_select, space, plan.index_id,
            common.SELECT_FUNC_NAME,
            {space_name, plan.index_id, plan.conditions, select_opts},
            {tarantool_iter = plan.tarantool_iter, field_names = plan.field_names, call_opts = opts.call_opts}
        )

    -- filter space format by plan.field_names (user defined fields + primary key + scan key)
    -- to pass it user as metadata
    local filtered_space_format, err = utils.get_fields_format(space_format, plan.field_names)
    if err ~= nil then
        return nil, err
    end

    return {
        tuples_limit = tuples_limit,
        merger = merger,
        space_format = filtered_space_format,
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
        timeout = opts.timeout,
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

    local gen, param, state = iter.merger:pairs()
    if opts.use_tomap == true then
        gen, param, state = gen:map(function(tuple)
            local result
            result, err = utils.unflatten(tuple, iter.space_format)
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

local function select_module_call_xc(space_name, user_conditions, opts)
    checks('string', '?table', {
        after = '?table|cdata',
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
        timeout = opts.timeout,
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

    return {
        metadata = table.copy(iter.space_format),
        rows = tuples,
    }
end

function select_module.call(space_name, user_conditions, opts)
    return SelectError:pcall(select_module_call_xc, space_name, user_conditions, opts)
end

return select_module
