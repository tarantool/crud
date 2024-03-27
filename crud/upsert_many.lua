local checks = require('checks')
local errors = require('errors')

local call = require('crud.common.call')
local const = require('crud.common.const')
local utils = require('crud.common.utils')
local batching_utils = require('crud.common.batching_utils')
local sharding = require('crud.common.sharding')
local dev_checks = require('crud.common.dev_checks')
local schema = require('crud.common.schema')

local BatchUpsertIterator = require('crud.common.map_call_cases.batch_upsert_iter')
local BatchPostprocessor = require('crud.common.map_call_cases.batch_postprocessor')

local UpsertManyError = errors.new_class('UpsertManyError', {capture_stack = false})

local upsert_many = {}

local UPSERT_MANY_FUNC_NAME = 'upsert_many_on_storage'
local CRUD_UPSERT_MANY_FUNC_NAME = utils.get_storage_call(UPSERT_MANY_FUNC_NAME)

local function upsert_many_on_storage(space_name, tuples, operations, opts)
    dev_checks('string', 'table', 'table', {
        add_space_schema_hash = '?boolean',
        stop_on_error = '?boolean',
        rollback_on_error = '?boolean',
        sharding_key_hash = '?number',
        sharding_func_hash = '?number',
        skip_sharding_hash_check = '?boolean',
        noreturn = '?boolean',
        fetch_latest_metadata = '?boolean',
    })

    opts = opts or {}

    local space = box.space[space_name]
    if space == nil then
        return nil, UpsertManyError:new("Space %q doesn't exist", space_name)
    end

    local _, err = sharding.check_sharding_hash(space_name,
                                                opts.sharding_func_hash,
                                                opts.sharding_key_hash,
                                                opts.skip_sharding_hash_check)

    if err ~= nil then
        return nil, batching_utils.construct_sharding_hash_mismatch_errors(err.err, tuples)
    end

    local processed_tuples = {}
    local errs = {}
    local replica_schema_version = nil

    box.begin()
    for i, tuple in ipairs(tuples) do
        -- add_space_schema_hash is true only in case of upsert_object_many
        -- the only one case when reloading schema can avoid upsert error
        -- is flattening object on router
        local insert_result = schema.wrap_box_space_func_result(space, 'upsert', {tuple, operations[i]}, {
            add_space_schema_hash = opts.add_space_schema_hash,
            fetch_latest_metadata = opts.fetch_latest_metadata,
        })
        if opts.fetch_latest_metadata then
            replica_schema_version = insert_result.storage_info.replica_schema_version
        end

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
                    if next(processed_tuples) then
                        errs = batching_utils.complement_batching_errors(errs,
                                batching_utils.rollback_on_error_msg, processed_tuples)
                    end

                    return nil, errs, replica_schema_version
                end

                box.commit()

                return nil, errs, replica_schema_version
            end
        else
            table.insert(processed_tuples, tuple)
        end
    end

    if next(errs) ~= nil then
        if opts.rollback_on_error == true then
            box.rollback()
            if next(processed_tuples) then
                errs = batching_utils.complement_batching_errors(errs,
                        batching_utils.rollback_on_error_msg, processed_tuples)
            end

            return nil, errs, replica_schema_version
        end

        box.commit()

        return nil, errs, replica_schema_version
    end

    box.commit()

    return nil, nil, replica_schema_version
end

upsert_many.storage_api = {[UPSERT_MANY_FUNC_NAME] = upsert_many_on_storage}

-- returns result, err, need_reload
-- need_reload indicates if reloading schema could help
-- see crud.common.schema.wrap_func_reload()
local function call_upsert_many_on_router(vshard_router, space_name, original_tuples_operation_data, opts)
    dev_checks('table', 'string', 'table', {
        timeout = '?number',
        fields = '?table',
        add_space_schema_hash = '?boolean',
        stop_on_error = '?boolean',
        rollback_on_error = '?boolean',
        vshard_router = '?string|table',
        skip_nullability_check_on_flatten = '?boolean',
        noreturn = '?boolean',
        fetch_latest_metadata = '?boolean',
    })

    local space, err, netbox_schema_version = utils.get_space(space_name, vshard_router, opts.timeout)
    if err ~= nil then
        return nil, {
            UpsertManyError:new("An error occurred during the operation: %s", err)
        }, const.NEED_SCHEMA_RELOAD
    end
    if space == nil then
        return nil, {UpsertManyError:new("Space %q doesn't exist", space_name)}, const.NEED_SCHEMA_RELOAD
    end

    local space_format = space:format()
    local tuples = {}
    local operations = {}
    for _, tuple_operation_data in ipairs(original_tuples_operation_data) do
        local tuple = table.deepcopy(tuple_operation_data[1])
        local operations_by_tuple = tuple_operation_data[2]

        if not utils.tarantool_supports_fieldpaths() then
            local converted_operations, err = utils.convert_operations(operations_by_tuple, space_format)
            if err ~= nil then
                return nil, {UpsertManyError:new("Wrong operations are specified: %s", err)}, const.NEED_SCHEMA_RELOAD
            end

            operations_by_tuple = converted_operations
        end

        table.insert(tuples, tuple)
        table.insert(operations, operations_by_tuple)
    end

    local upsert_many_on_storage_opts = {
        add_space_schema_hash = opts.add_space_schema_hash,
        stop_on_error = opts.stop_on_error,
        rollback_on_error = opts.rollback_on_error,
        fetch_latest_metadata = opts.fetch_latest_metadata,
    }

    local iter, err = BatchUpsertIterator:new({
        tuples = tuples,
        space = space,
        operations = operations,
        execute_on_storage_opts = upsert_many_on_storage_opts,
        vshard_router = vshard_router,
    })
    if err ~= nil then
        return nil, {err}, const.NEED_SCHEMA_RELOAD
    end

    local postprocessor = BatchPostprocessor:new(vshard_router)

    local _, errs, storages_info = call.map(vshard_router, CRUD_UPSERT_MANY_FUNC_NAME, nil, {
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

        if table.maxn(tuples) == table.maxn(errs) then
            return nil, errs
        end
    end

    if opts.noreturn == true then
        return nil, errs
    end

    if opts.fetch_latest_metadata == true then
        -- This option is temporary and is related to [1], [2].
        -- [1] https://github.com/tarantool/crud/issues/236
        -- [2] https://github.com/tarantool/crud/issues/361
        space = utils.fetch_latest_metadata_when_map_storages(space, space_name, vshard_router, opts,
                                                              storages_info, netbox_schema_version)
    end

    local res, err = utils.format_result(nil, space, opts.fields)
    if err ~= nil then
        errs = errs or {}
        table.insert(errs, err)
        return nil, errs
    end

    return res, errs
end

--- Update or insert batch of tuples to the specified space
--
-- @function tuples
--
-- @param string space_name
--  A space name
--
-- @param table tuples_operation_data
--  Tuples and operations in format
--  {{tuple_1, operation_1}, ..., {tuple_n, operation_n}}
--
-- @tparam ?table opts
--  Options of batch_upsert.tuples_batch
--
-- @return[1] tuples
-- @treturn[2] nil
-- @treturn[2] table of tables Error description

function upsert_many.tuples(space_name, tuples_operation_data, opts)
    checks('string', 'table', {
        timeout = '?number',
        fields = '?table',
        add_space_schema_hash = '?boolean',
        stop_on_error = '?boolean',
        rollback_on_error = '?boolean',
        vshard_router = '?string|table',
        noreturn = '?boolean',
        fetch_latest_metadata = '?boolean',
    })

    if next(tuples_operation_data) == nil then
        return nil, {UpsertManyError:new("At least one tuple expected")}
    end

    opts = opts or {}

    local vshard_router, err = utils.get_vshard_router_instance(opts.vshard_router)
    if err ~= nil then
        return nil, {UpsertManyError:new(err)}
    end

    return schema.wrap_func_reload(vshard_router, sharding.wrap_method, call_upsert_many_on_router,
                                   space_name, tuples_operation_data, opts)
end

--- Update or insert batch of objects to the specified space
--
-- @function objects
--
-- @param string space_name
--  A space name
--
-- @param table objs_operation_data
--  Objects and operations in format
--  {{obj_1, operation_1}, ..., {obj_n, operation_n}}
--
-- @tparam ?table opts
--  Options of batch_upsert.tuples_batch
--
-- @return[1] objects
-- @treturn[2] nil
-- @treturn[2] table of tables Error description

function upsert_many.objects(space_name, objs_operation_data, opts)
    checks('string', 'table', {
        timeout = '?number',
        fields = '?table',
        stop_on_error = '?boolean',
        rollback_on_error = '?boolean',
        vshard_router = '?string|table',
        noreturn = '?boolean',
        fetch_latest_metadata = '?boolean',
    })

    if next(objs_operation_data) == nil then
        return nil, {UpsertManyError:new("At least one object expected")}
    end

    opts = opts or {}

    local vshard_router, err = utils.get_vshard_router_instance(opts.vshard_router)
    if err ~= nil then
        return nil, {UpsertManyError:new(err)}
    end

    -- upsert can fail if router uses outdated schema to flatten object
    opts = utils.merge_options(opts, {add_space_schema_hash = true})

    local tuples_operation_data = {}
    local format_errs = {}

    for _, obj_operation_data in ipairs(objs_operation_data) do
        local tuple, err = utils.flatten_obj_reload(vshard_router, space_name, obj_operation_data[1])
        if err ~= nil then
            local err_obj = UpsertManyError:new("Failed to flatten object: %s", err)
            err_obj.operation_data = obj_operation_data[1]

            if opts.stop_on_error == true then
                return nil, {err_obj}
            end

            table.insert(format_errs, err_obj)
        else
            table.insert(tuples_operation_data, {tuple, obj_operation_data[2]})
        end
    end

    if next(tuples_operation_data) == nil then
        return nil, format_errs
    end

    local res, errs = schema.wrap_func_reload(vshard_router, sharding.wrap_method, call_upsert_many_on_router,
                                              space_name, tuples_operation_data, opts)

    if next(format_errs) ~= nil then
        if errs == nil then
            errs = format_errs
        else
            errs = utils.list_extend(errs, format_errs)
        end
    end

    return res, errs
end

return upsert_many
