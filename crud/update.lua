local checks = require('checks')
local errors = require('errors')

local call = require('crud.common.call')
local const = require('crud.common.const')
local utils = require('crud.common.utils')
local sharding = require('crud.common.sharding')
local sharding_key_module = require('crud.common.sharding.sharding_key')
local sharding_metadata_module = require('crud.common.sharding.sharding_metadata')
local dev_checks = require('crud.common.dev_checks')
local schema = require('crud.common.schema')

local UpdateError = errors.new_class('UpdateError', {capture_stack = false})

local update = {}

local UPDATE_FUNC_NAME = 'update_on_storage'
local CRUD_UPDATE_FUNC_NAME = utils.get_storage_call(UPDATE_FUNC_NAME)

local function update_on_storage(space_name, key, operations, field_names, opts)
    dev_checks('string', '?', 'table', '?table', {
        sharding_key_hash = '?number',
        sharding_func_hash = '?number',
        skip_sharding_hash_check = '?boolean',
        noreturn = '?boolean',
        fetch_latest_metadata = '?boolean',
    })

    opts = opts or {}

    local space = box.space[space_name]
    if space == nil then
        return nil, UpdateError:new("Space %q doesn't exist", space_name)
    end

    local _, err = sharding.check_sharding_hash(space_name,
                                                opts.sharding_func_hash,
                                                opts.sharding_key_hash,
                                                opts.skip_sharding_hash_check)

    if err ~= nil then
        return nil, err
    end

    -- add_space_schema_hash is false because
    -- reloading space format on router can't avoid update error on storage
    local res, err = schema.wrap_box_space_func_result(space, 'update', {key, operations}, {
        add_space_schema_hash = false,
        field_names = field_names,
        noreturn = opts.noreturn,
        fetch_latest_metadata = opts.fetch_latest_metadata,
    })

    if err ~= nil then
        return nil, err
    end

    if res.err == nil then
        return res, nil
    end

    -- Relevant for Tarantool older than 2.8.1.
    -- We can only add fields to end of the tuple.
    -- If schema is updated and nullable fields are added, then we will get error.
    -- Therefore, we need to add filling of intermediate nullable fields.
    -- More details: https://github.com/tarantool/tarantool/issues/3378
    if utils.is_field_not_found(res.err.code) then
        operations = utils.add_intermediate_nullable_fields(operations, space:format(), space:get(key))
        res, err = schema.wrap_box_space_func_result(space, 'update', {key, operations}, {
            add_space_schema_hash = false,
            field_names = field_names,
            noreturn = opts.noreturn,
            fetch_latest_metadata = opts.fetch_latest_metadata,
        })
    end

    return res, err
end

update.storage_api = {[UPDATE_FUNC_NAME] = update_on_storage}

-- returns result, err, need_reload
-- need_reload indicates if reloading schema could help
-- see crud.common.schema.wrap_func_reload()
local function call_update_on_router(vshard_router, space_name, key, user_operations, opts)
    dev_checks('table', 'string', '?', 'table', {
        timeout = '?number',
        bucket_id = '?number|cdata',
        fields = '?table',
        vshard_router = '?string|table',
        noreturn = '?boolean',
        fetch_latest_metadata = '?boolean',
    })

    local space, err, netbox_schema_version = utils.get_space(space_name, vshard_router, opts.timeout)
    if err ~= nil then
        return nil, UpdateError:new("An error occurred during the operation: %s", err), const.NEED_SCHEMA_RELOAD
    end
    if space == nil then
        return nil, UpdateError:new("Space %q doesn't exist", space_name), const.NEED_SCHEMA_RELOAD
    end

    local space_format = space:format()

    if box.tuple.is(key) then
        key = key:totable()
    end

    local sharding_key = key
    local sharding_key_hash = nil
    local skip_sharding_hash_check = nil

    if opts.bucket_id == nil then
        local primary_index_parts = space.index[0].parts

        local sharding_key_data, err = sharding_metadata_module.fetch_sharding_key_on_router(vshard_router, space_name)
        if err ~= nil then
            return nil, err
        end

        sharding_key, err = sharding_key_module.extract_from_pk(vshard_router,
                                                                space_name,
                                                                sharding_key_data.value,
                                                                primary_index_parts, key)
        if err ~= nil then
            return nil, err
        end

        sharding_key_hash = sharding_key_data.hash
    else
        skip_sharding_hash_check = true
    end

    local operations = user_operations
    if not utils.tarantool_supports_fieldpaths() then
        operations, err = utils.convert_operations(user_operations, space_format)
        if err ~= nil then
            return nil, UpdateError:new("Wrong operations are specified: %s", err), const.NEED_SCHEMA_RELOAD
        end
    end

    local bucket_id_data, err = sharding.key_get_bucket_id(vshard_router, space_name, sharding_key, opts.bucket_id)
    if err ~= nil then
        return nil, err
    end

    local update_on_storage_opts = {
        sharding_func_hash = bucket_id_data.sharding_func_hash,
        sharding_key_hash = sharding_key_hash,
        skip_sharding_hash_check = skip_sharding_hash_check,
        noreturn = opts.noreturn,
        fetch_latest_metadata = opts.fetch_latest_metadata,
    }

    local call_opts = {
        mode = 'write',
        timeout = opts.timeout,
    }

    local storage_result, err = call.single(vshard_router,
        bucket_id_data.bucket_id, CRUD_UPDATE_FUNC_NAME,
        {space_name, key, operations, opts.fields, update_on_storage_opts},
        call_opts
    )

    if err ~= nil then
        local err_wrapped = UpdateError:new("Failed to call update on storage-side: %s", err)

        if sharding.result_needs_sharding_reload(err) then
            return nil, err_wrapped, const.NEED_SHARDING_RELOAD
        end

        return nil, err_wrapped
    end

    if storage_result.err ~= nil then
        return nil, UpdateError:new("Failed to update: %s", storage_result.err)
    end

    if opts.noreturn == true then
        return nil
    end

    local tuple = storage_result.res

    if opts.fetch_latest_metadata == true then
        -- This option is temporary and is related to [1], [2].
        -- [1] https://github.com/tarantool/crud/issues/236
        -- [2] https://github.com/tarantool/crud/issues/361
        space = utils.fetch_latest_metadata_when_single_storage(space, space_name, netbox_schema_version,
                                                                vshard_router, opts, storage_result.storage_info)
    end

    return utils.format_result({tuple}, space, opts.fields)
end

--- Updates tuple in the specified space
--
-- @function call
--
-- @param string space_name
--  A space name
--
-- @param key
--  Primary key value
--
-- @param table user_operations
--  Operations to be performed.
--  See `space:update` operations in Tarantool doc
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
-- @return[1] object
-- @treturn[2] nil
-- @treturn[2] table Error description
--
function update.call(space_name, key, user_operations, opts)
    checks('string', '?', 'table', {
        timeout = '?number',
        bucket_id = '?number|cdata',
        fields = '?table',
        vshard_router = '?string|table',
        noreturn = '?boolean',
        fetch_latest_metadata = '?boolean',
    })

    opts = opts or {}
    local vshard_router, err = utils.get_vshard_router_instance(opts.vshard_router)
    if err ~= nil then
        return nil, UpdateError:new(err)
    end

    return schema.wrap_func_reload(vshard_router, sharding.wrap_method, call_update_on_router,
                                   space_name, key, user_operations, opts)
end

return update
