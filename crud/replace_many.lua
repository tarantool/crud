local checks = require('checks')
local errors = require('errors')

local call = require('crud.common.call')
local const = require('crud.common.const')
local utils = require('crud.common.utils')
local batching_utils = require('crud.common.batching_utils')
local sharding = require('crud.common.sharding')
local dev_checks = require('crud.common.dev_checks')
local schema = require('crud.common.schema')

local BatchInsertIterator = require('crud.common.map_call_cases.batch_insert_iter')
local BatchPostprocessor = require('crud.common.map_call_cases.batch_postprocessor')

local ReplaceManyError = errors.new_class('ReplaceManyError', {capture_stack = false})

local replace_many = {}

local REPLACE_MANY_FUNC_NAME = '_crud.replace_many_on_storage'

local function replace_many_on_storage(space_name, tuples, opts)
    dev_checks('string', 'table', {
        add_space_schema_hash = '?boolean',
        fields = '?table',
        stop_on_error = '?boolean',
        rollback_on_error = '?boolean',
        sharding_key_hash = '?number',
        sharding_func_hash = '?number',
        skip_sharding_hash_check = '?boolean',
    })

    opts = opts or {}

    local space = box.space[space_name]
    if space == nil then
        return nil, {ReplaceManyError:new("Space %q doesn't exist", space_name)}
    end

    local _, err = sharding.check_sharding_hash(space_name,
                                                opts.sharding_func_hash,
                                                opts.sharding_key_hash,
                                                opts.skip_sharding_hash_check)

    if err ~= nil then
        return nil, batching_utils.construct_sharding_hash_mismatch_errors(err.err, tuples)
    end

    local inserted_tuples = {}
    local errs = {}

    box.begin()
    for i, tuple in ipairs(tuples) do
        -- add_space_schema_hash is true only in case of replace_object_many
        -- the only one case when reloading schema can avoid replace error
        -- is flattening object on router
        local insert_result = schema.wrap_box_space_func_result(space, 'replace', {tuple}, {
            add_space_schema_hash = opts.add_space_schema_hash,
            field_names = opts.fields,
        })

        table.insert(errs, err)

        if insert_result.err ~= nil then
            local err = {
                err = insert_result.err,
                space_schema_hash = insert_result.space_schema_hash,
                operation_data = tuple,
            }

            table.insert(errs, err)

            if opts.stop_on_error == true then
                local left_tuples = utils.list_slice(tuples, i + 1)
                if next(left_tuples) then
                    errs = batching_utils.complement_batching_errors(errs,
                            batching_utils.stop_on_error_msg, left_tuples)
                end

                if opts.rollback_on_error == true then
                    box.rollback()
                    if next(inserted_tuples) then
                        errs = batching_utils.complement_batching_errors(errs,
                                batching_utils.rollback_on_error_msg, inserted_tuples)
                    end

                    return nil, errs
                end

                box.commit()

                return inserted_tuples, errs
            end
        end

        table.insert(inserted_tuples, insert_result.res)
    end

    if next(errs) ~= nil then
        if opts.rollback_on_error == true then
            box.rollback()
            if next(inserted_tuples) then
                errs = batching_utils.complement_batching_errors(errs,
                        batching_utils.rollback_on_error_msg, inserted_tuples)
            end

            return nil, errs
        end

        box.commit()

        return inserted_tuples, errs
    end

    box.commit()

    return inserted_tuples
end

function replace_many.init()
    _G._crud.replace_many_on_storage = replace_many_on_storage
end

-- returns result, err, need_reload
-- need_reload indicates if reloading schema could help
-- see crud.common.schema.wrap_func_reload()
local function call_replace_many_on_router(vshard_router, space_name, original_tuples, opts)
    dev_checks('table', 'string', 'table', {
        timeout = '?number',
        fields = '?table',
        add_space_schema_hash = '?boolean',
        stop_on_error = '?boolean',
        rollback_on_error = '?boolean',
        vshard_router = '?string|table',
    })

    local space = utils.get_space(space_name, vshard_router:routeall())
    if space == nil then
        return nil, {ReplaceManyError:new("Space %q doesn't exist", space_name)}, const.NEED_SCHEMA_RELOAD
    end

    local tuples = table.deepcopy(original_tuples)

    local replace_many_on_storage_opts = {
        add_space_schema_hash = opts.add_space_schema_hash,
        fields = opts.fields,
        stop_on_error = opts.stop_on_error,
        rollback_on_error = opts.rollback_on_error,
    }

    local iter, err = BatchInsertIterator:new({
        tuples = tuples,
        space = space,
        execute_on_storage_opts = replace_many_on_storage_opts,
        vshard_router = vshard_router,
    })
    if err ~= nil then
        return nil, {err}, const.NEED_SCHEMA_RELOAD
    end

    local postprocessor = BatchPostprocessor:new(vshard_router)

    local rows, errs = call.map(vshard_router, REPLACE_MANY_FUNC_NAME, nil, {
        timeout = opts.timeout,
        mode = 'write',
        iter = iter,
        postprocessor = postprocessor,
    })

    if errs ~= nil then
        local tuples_count = table.maxn(tuples)
        if sharding.batching_result_needs_sharding_reload(errs, tuples_count) then
            return nil, errs, const.NEED_SHARDING_RELOAD
        end

        if schema.batching_result_needs_reload(space, errs, tuples_count) then
            return nil, errs, const.NEED_SCHEMA_RELOAD
        end
    end

    if next(rows) == nil then
        return nil, errs
    end

    local res, err = utils.format_result(rows, space, opts.fields)
    if err ~= nil then
        errs = errs or {}
        table.insert(errs, err)
        return nil, errs
    end

    return res, errs
end

--- Replace batch of tuples to the specified space
--
-- @function tuples
--
-- @param string space_name
--  A space name
--
-- @param table tuples
--  Tuples
--
-- @tparam ?table opts
--  Options of batch_replace.tuples_batch
--
-- @return[1] tuples
-- @treturn[2] nil
-- @treturn[2] table of tables Error description

function replace_many.tuples(space_name, tuples, opts)
    checks('string', 'table', {
        timeout = '?number',
        fields = '?table',
        add_space_schema_hash = '?boolean',
        stop_on_error = '?boolean',
        rollback_on_error = '?boolean',
        vshard_router = '?string|table',
    })

    opts = opts or {}

    local vshard_router, err = utils.get_vshard_router_instance(opts.vshard_router)
    if err ~= nil then
        return nil, {ReplaceManyError:new(err)}
    end

    return schema.wrap_func_reload(vshard_router, sharding.wrap_method, call_replace_many_on_router,
                                   space_name, tuples, opts)
end

--- Replace batch of objects to the specified space
--
-- @function objects
--
-- @param string space_name
--  A space name
--
-- @param table objs
--  Objects
--
-- @tparam ?table opts
--  Options of batch_insert.tuples_batch
--
-- @return[1] objects
-- @treturn[2] nil
-- @treturn[2] table of tables Error description

function replace_many.objects(space_name, objs, opts)
    checks('string', 'table', {
        timeout = '?number',
        fields = '?table',
        stop_on_error = '?boolean',
        rollback_on_error = '?boolean',
        vshard_router = '?string|table',
        skip_nullability_check_on_flatten = '?boolean',
    })

    opts = opts or {}

    local vshard_router, err = utils.get_vshard_router_instance(opts.vshard_router)
    if err ~= nil then
        return nil, {ReplaceManyError:new(err)}
    end

    -- insert can fail if router uses outdated schema to flatten object
    opts = utils.merge_options(opts, {add_space_schema_hash = true})

    local tuples = {}
    local format_errs = {}

    for _, obj in ipairs(objs) do

        local tuple, err = utils.flatten_obj_reload(vshard_router, space_name, obj,
                                                    opts.skip_nullability_check_on_flatten)
        if err ~= nil then
            local err_obj = ReplaceManyError:new("Failed to flatten object: %s", err)
            err_obj.operation_data = obj

            if opts.stop_on_error == true then
                return nil, {err_obj}
            end

            table.insert(format_errs, err_obj)
        end

        table.insert(tuples, tuple)
    end

    if next(tuples) == nil then
        return nil, format_errs
    end

    local res, errs = schema.wrap_func_reload(vshard_router, sharding.wrap_method, call_replace_many_on_router,
                                              space_name, tuples, opts)

    if next(format_errs) ~= nil then
        if errs == nil then
            errs = format_errs
        else
            errs = utils.list_extend(errs, format_errs)
        end
    end

    return res, errs
end

return replace_many
