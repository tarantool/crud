local checks = require('checks')
local errors = require('errors')
local vshard = require('vshard')

local utils = require('crud.common.utils')
local sharding = require('crud.common.sharding')
local dev_checks = require('crud.common.dev_checks')
local common = require('crud.select.compat.common')
local schema = require('crud.common.schema')

local select_conditions = require('crud.select.conditions')
local select_plan = require('crud.select.plan')

local Merger = require('crud.select.merger')

local SelectError = errors.new_class('SelectError')

local select_module = {}

local function build_select_iterator(space_name, user_conditions, opts)
    dev_checks('string', '?table', {
        after = '?table',
        first = '?number',
        timeout = '?number',
        batch_size = '?number',
        bucket_id = '?number|cdata',
    })

    opts = opts or {}

    if opts.batch_size ~= nil and opts.batch_size < 1 then
        return nil, SelectError:new("batch_size should be > 0")
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
        replicasets_to_select, err = common.get_replicasets_by_sharding_key(bucket_id)
        if err ~= nil then
            return nil, err, true
        end
    end

    -- If opts.batch_size is missed we should specify it to min(first, DEFAULT_BATCH_SIZE)
    local batch_size
    if opts.batch_size == nil then
        if opts.first ~= nil and opts.first < common.DEFAULT_BATCH_SIZE then
            batch_size = opts.first
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
    }

    local merger = Merger.new(replicasets_to_select, space_name, plan.index_id,
            common.SELECT_FUNC_NAME,
            {space_name, plan.index_id, plan.conditions, select_opts},
            {tarantool_iter = plan.tarantool_iter}
        )

    return {
        merger = merger,
        space_format = space_format,
    }
end

function select_module.pairs(space_name, user_conditions, opts)
    checks('string', '?table', {
        after = '?table',
        first = '?number',
        timeout = '?number',
        batch_size = '?number',
        use_tomap = '?boolean',
        bucket_id = '?number|cdata',
    })

    opts = opts or {}

    if opts.first ~= nil and opts.first < 0 then
        error(string.format("Negative first isn't allowed for pairs"))
    end

    local iter, err = build_select_iterator(space_name, user_conditions, {
        after = opts.after,
        first = opts.first,
        timeout = opts.timeout,
        batch_size = opts.batch_size,
        bucket_id = opts.bucket_id,
    })

    if err ~= nil then
        error(string.format("Failed to generate iterator: %s", err))
    end

    if opts.use_tomap ~= true then
        return iter.merger:pairs()
    end

    return iter.merger:pairs():map(function(tuple)
        local result
        result, err = utils.unflatten(tuple, iter.space_format)
        if err ~= nil then
            error(string.format("Failed to unflatten next object: %s", err))
        end
        return result
    end)
end

local function select_module_call_xc(space_name, user_conditions, opts)
    checks('string', '?table', {
        after = '?table',
        first = '?number',
        timeout = '?number',
        batch_size = '?number',
        bucket_id = '?number|cdata',
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
    }

    local iter, err = schema.wrap_func_reload(
            build_select_iterator, space_name, user_conditions, iterator_opts
    )

    if err ~= nil then
        return nil, err
    end

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
