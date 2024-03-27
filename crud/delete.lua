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

local DeleteError = errors.new_class('DeleteError', {capture_stack = false})

local delete = {}

local DELETE_FUNC_NAME = 'delete_on_storage'
local CRUD_DELETE_FUNC_NAME = utils.get_storage_call(DELETE_FUNC_NAME)

local function delete_on_storage(space_name, key, field_names, opts)
    dev_checks('string', '?', '?table', {
        sharding_key_hash = '?number',
        sharding_func_hash = '?number',
        skip_sharding_hash_check = '?boolean',
        noreturn = '?boolean',
        fetch_latest_metadata = '?boolean',
    })

    opts = opts or {}

    local space = box.space[space_name]
    if space == nil then
        return nil, DeleteError:new("Space %q doesn't exist", space_name)
    end

    local _, err = sharding.check_sharding_hash(space_name,
                                                opts.sharding_func_hash,
                                                opts.sharding_key_hash,
                                                opts.skip_sharding_hash_check)

    if err ~= nil then
        return nil, err
    end

    -- add_space_schema_hash is false because
    -- reloading space format on router can't avoid delete error on storage
    return schema.wrap_box_space_func_result(space, 'delete', {key}, {
        add_space_schema_hash = false,
        field_names = field_names,
        noreturn = opts.noreturn,
        fetch_latest_metadata = opts.fetch_latest_metadata,
    })
end

delete.storage_api = {[DELETE_FUNC_NAME] = delete_on_storage}

-- returns result, err, need_reload
-- need_reload indicates if reloading schema could help
-- see crud.common.schema.wrap_func_reload()
local function call_delete_on_router(vshard_router, space_name, key, opts)
    dev_checks('table', 'string', '?', {
        timeout = '?number',
        bucket_id = '?number|cdata',
        fields = '?table',
        vshard_router = '?string|table',
        noreturn = '?boolean',
        fetch_latest_metadata = '?boolean',
    })

    local space, err, netbox_schema_version = utils.get_space(space_name, vshard_router, opts.timeout)
    if err ~= nil then
        return nil, DeleteError:new("An error occurred during the operation: %s", err), const.NEED_SCHEMA_RELOAD
    end
    if space == nil then
        return nil, DeleteError:new("Space %q doesn't exist", space_name), const.NEED_SCHEMA_RELOAD
    end

    if box.tuple.is(key) then
        key = key:totable()
    end

    local sharding_key_hash = nil
    local skip_sharding_hash_check = nil

    local sharding_key = key
    if opts.bucket_id == nil then
        if space.index[0] == nil then
            return nil, DeleteError:new("Cannot fetch primary index parts"), const.NEED_SCHEMA_RELOAD
        end
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

    local bucket_id_data, err = sharding.key_get_bucket_id(vshard_router, space_name, sharding_key, opts.bucket_id)
    if err ~= nil then
        return nil, err
    end

    local delete_on_storage_opts = {
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
        bucket_id_data.bucket_id, CRUD_DELETE_FUNC_NAME,
        {space_name, key, opts.fields, delete_on_storage_opts},
        call_opts
    )

    if err ~= nil then
        local err_wrapped = DeleteError:new("Failed to call delete on storage-side: %s", err)

        if sharding.result_needs_sharding_reload(err) then
            return nil, err_wrapped, const.NEED_SHARDING_RELOAD
        end

        return nil, err_wrapped
    end

    if storage_result.err ~= nil then
        return nil, DeleteError:new("Failed to delete: %s", storage_result.err)
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

--- Deletes tuple from the specified space by key
--
-- @function call
--
-- @param string space_name
--  A space name
--
-- @param key
--  Primary key value
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
function delete.call(space_name, key, opts)
    checks('string', '?', {
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
        return nil, DeleteError:new(err)
    end

    return schema.wrap_func_reload(vshard_router, sharding.wrap_method, call_delete_on_router,
                                   space_name, key, opts)
end

return delete
