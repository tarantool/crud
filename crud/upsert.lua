local checks = require('checks')
local errors = require('errors')

local call = require('crud.common.call')
local const = require('crud.common.const')
local utils = require('crud.common.utils')
local sharding = require('crud.common.sharding')
local dev_checks = require('crud.common.dev_checks')
local schema = require('crud.common.schema')

local UpsertError = errors.new_class('UpsertError', { capture_stack = false})

local upsert = {}

local UPSERT_FUNC_NAME = 'upsert_on_storage'
local CRUD_UPSERT_FUNC_NAME = utils.get_storage_call(UPSERT_FUNC_NAME)

local function upsert_on_storage(space_name, tuple, operations, opts)
    dev_checks('string', 'table', 'table', {
        add_space_schema_hash = '?boolean',
        fields = '?table',
        sharding_key_hash = '?number',
        sharding_func_hash = '?number',
        skip_sharding_hash_check = '?boolean',
        fetch_latest_metadata = '?boolean',
    })

    opts = opts or {}

    local space = box.space[space_name]
    if space == nil then
        return nil, UpsertError:new("Space %q doesn't exist", space_name)
    end

    local _, err = sharding.check_sharding_hash(space_name,
                                                opts.sharding_func_hash,
                                                opts.sharding_key_hash,
                                                opts.skip_sharding_hash_check)

    if err ~= nil then
        return nil, err
    end

    -- add_space_schema_hash is true only in case of upsert_object
    -- the only one case when reloading schema can avoid insert error
    -- is flattening object on router
    return schema.wrap_box_space_func_result(space, 'upsert', {tuple, operations}, {
        add_space_schema_hash = opts.add_space_schema_hash,
        fetch_latest_metadata = opts.fetch_latest_metadata,
    })
end

upsert.storage_api = {[UPSERT_FUNC_NAME] = upsert_on_storage}

-- returns result, err, need_reload
-- need_reload indicates if reloading schema could help
-- see crud.common.schema.wrap_func_reload()
local function call_upsert_on_router(vshard_router, space_name, original_tuple, user_operations, opts)
    dev_checks('table', 'string', '?', 'table', {
        timeout = '?number',
        bucket_id = '?number|cdata',
        add_space_schema_hash = '?boolean',
        fields = '?table',
        vshard_router = '?string|table',
        skip_nullability_check_on_flatten = '?boolean',
        noreturn = '?boolean',
        fetch_latest_metadata = '?boolean',
    })

    local space, err, netbox_schema_version = utils.get_space(space_name, vshard_router, opts.timeout)
    if err ~= nil then
        return nil, UpsertError:new("An error occurred during the operation: %s", err), const.NEED_SCHEMA_RELOAD
    end
    if space == nil then
        return nil, UpsertError:new("Space %q doesn't exist", space_name), const.NEED_SCHEMA_RELOAD
    end

    local space_format = space:format()
    local operations = user_operations
    if not utils.tarantool_supports_fieldpaths() then
        operations, err = utils.convert_operations(user_operations, space_format)
        if err ~= nil then
            return nil, UpsertError:new("Wrong operations are specified: %s", err), const.NEED_SCHEMA_RELOAD
        end
    end

    local tuple = table.deepcopy(original_tuple)

    local sharding_data, err = sharding.tuple_set_and_return_bucket_id(vshard_router, tuple, space, opts.bucket_id)
    if err ~= nil then
        return nil, UpsertError:new("Failed to get bucket ID: %s", err), const.NEED_SCHEMA_RELOAD
    end

    local upsert_on_storage_opts = {
        add_space_schema_hash = opts.add_space_schema_hash,
        fields = opts.fields,
        sharding_func_hash = sharding_data.sharding_func_hash,
        sharding_key_hash = sharding_data.sharding_key_hash,
        skip_sharding_hash_check = sharding_data.skip_sharding_hash_check,
        fetch_latest_metadata = opts.fetch_latest_metadata,
    }

    local call_opts = {
        mode = 'write',
        timeout = opts.timeout,
    }

    local storage_result, err = call.single(vshard_router,
        sharding_data.bucket_id, CRUD_UPSERT_FUNC_NAME,
        {space_name, tuple, operations, upsert_on_storage_opts},
        call_opts
    )

    if err ~= nil then
        local err_wrapped = UpsertError:new("Failed to call upsert on storage-side: %s", err)

        if sharding.result_needs_sharding_reload(err) then
            return nil, err_wrapped, const.NEED_SHARDING_RELOAD
        end

        return nil, err_wrapped
    end

    if storage_result.err ~= nil then
        local err_wrapped = UpsertError:new("Failed to upsert: %s", storage_result.err)

        if schema.result_needs_reload(space, storage_result) then
            return nil, err_wrapped, const.NEED_SCHEMA_RELOAD
        end

        return nil, err_wrapped
    end

    if opts.noreturn == true then
        return nil
    end

    if opts.fetch_latest_metadata == true then
        -- This option is temporary and is related to [1], [2].
        -- [1] https://github.com/tarantool/crud/issues/236
        -- [2] https://github.com/tarantool/crud/issues/361
        space = utils.fetch_latest_metadata_when_single_storage(space, space_name, netbox_schema_version,
                                                                vshard_router, opts, storage_result.storage_info)
    end

    -- upsert returns only metadata, without rows
    return utils.format_result({}, space, opts.fields)
end

--- Update or insert a tuple in the specified space
--
-- @function tuple
--
-- @param string space_name
--  A space name
--
-- @param table tuple
--  Tuple
--
-- @param table user_operations
--  user_operations to be performed.
--  See `space:upsert()` operations in Tarantool doc
--
-- @tparam ?number opts.timeout
--  Function call timeout
--
-- @tparam ?number opts.bucket_id
--  Bucket ID
--  (by default, it's vshard.router.bucket_id_strcrc32 of primary key)
--
-- @tparam ?string|table opts.vshard_router
--  Cartridge vshard group name or vshard router instance.
--  Set this parameter if your space is not a part of the
--  default vshard cluster.
--
-- @tparam ?boolean opts.noreturn
--  Suppress returning successfully processed tuple.
--
-- @return[1] tuple
-- @treturn[2] nil
-- @treturn[2] table Error description
--
function upsert.tuple(space_name, tuple, user_operations, opts)
    checks('string', '?', 'table', {
        timeout = '?number',
        bucket_id = '?number|cdata',
        add_space_schema_hash = '?boolean',
        fields = '?table',
        vshard_router = '?string|table',
        noreturn = '?boolean',
        fetch_latest_metadata = '?boolean',
    })

    opts = opts or {}

    local vshard_router, err = utils.get_vshard_router_instance(opts.vshard_router)
    if err ~= nil then
        return nil, UpsertError:new(err)
    end

    return schema.wrap_func_reload(vshard_router, sharding.wrap_method, call_upsert_on_router,
                                   space_name, tuple, user_operations, opts)
end

--- Update or insert an object in the specified space
--
-- @function object
--
-- @param string space_name
--  A space name
--
-- @param table obj
--  Object
--
-- @param table user_operations
--  user_operations to be performed.
--  See `space:upsert()` operations in Tarantool doc
--
-- @tparam ?table opts
--  Options of upsert.tuple
--
-- @return[1] object
-- @treturn[2] nil
-- @treturn[2] table Error description
--
function upsert.object(space_name, obj, user_operations, opts)
    checks('string', 'table', 'table', {
        timeout = '?number',
        bucket_id = '?number|cdata',
        add_space_schema_hash = '?boolean',
        fields = '?table',
        vshard_router = '?string|table',
        noreturn = '?boolean',
        fetch_latest_metadata = '?boolean',
    })

    opts = opts or {}

    local vshard_router, err = utils.get_vshard_router_instance(opts.vshard_router)
    if err ~= nil then
        return nil, UpsertError:new(err)
    end

    -- upsert can fail if router uses outdated schema to flatten object
    opts = utils.merge_options(opts, {add_space_schema_hash = true})

    local tuple, err = utils.flatten_obj_reload(vshard_router, space_name, obj)
    if err ~= nil then
        return nil, UpsertError:new("Failed to flatten object: %s", err)
    end

    return schema.wrap_func_reload(vshard_router, sharding.wrap_method, call_upsert_on_router,
                                   space_name, tuple, user_operations, opts)
end

return upsert
