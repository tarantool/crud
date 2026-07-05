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
local bucket_ref_unref = require('crud.common.sharding.bucket_ref_unref')

local LocateError = errors.new_class('LocateError', {capture_stack = false})

local locate = {}

local LOCATE_FUNC_NAME = 'locate_on_storage'
local CRUD_LOCATE_FUNC_NAME = utils.get_storage_call(LOCATE_FUNC_NAME)

local function locate_on_storage(space_name, key, opts)
    dev_checks('string', '?', {
        bucket_id = 'number|cdata',
        sharding_key_hash = '?number',
        sharding_func_hash = '?number',
        skip_sharding_hash_check = '?boolean',
    })

    opts = opts or {}

    -- Dynamic check for the enterprise 'cooler' module
    local has_cooler, cooler = pcall(require, 'cooler')
    if not has_cooler or type(cooler) ~= 'table' or type(cooler.locate) ~= 'function' then
        return {err = "Module 'cooler' with function 'locate' is not available on this storage"}
    end

    local space = box.space[space_name]
    if space == nil then
        return {err = string.format("Space %q doesn't exist", space_name)}
    end

    local _, sharding_err = sharding.check_sharding_hash(space_name,
                                                         opts.sharding_func_hash,
                                                         opts.sharding_key_hash,
                                                         opts.skip_sharding_hash_check)
    if sharding_err ~= nil then
        return {err = sharding_err}
    end

    local ref_ok, bucket_ref_err, unref = bucket_ref_unref.bucket_refro(opts.bucket_id, space.engine)
    if not ref_ok then
        return {err = bucket_ref_err}
    end

    local ok, locate_res, locate_err = pcall(cooler.locate, space_name, key, opts.bucket_id)

    local unref_ok, err_unref = unref(opts.bucket_id, space.engine)
    if not unref_ok then
        return {err = err_unref}
    end

    if not ok then
        return {err = string.format("Cooler locate runtime error: %s", tostring(locate_res))}
    end

    if locate_err ~= nil then
        return {err = string.format("Cooler locate failed: %s", tostring(locate_err))}
    end

    return {res = locate_res}
end

locate.storage_api = {[LOCATE_FUNC_NAME] = locate_on_storage}

local function call_locate_on_router(vshard_router, space_name, key, opts)
    dev_checks('table', 'string', '?', {
        timeout = '?number',
        request_timeout = '?number',
        bucket_id = '?',
        prefer_replica = '?boolean',
        balance = '?boolean',
        mode = '?string',
        vshard_router = '?string|table',
    })

    local space, err = utils.get_space(space_name, vshard_router, {
        timeout = opts.timeout,
        read_only = opts.mode ~= 'write',
    })
    if err ~= nil then
        return nil, LocateError:new("An error occurred during the operation: %s", err), const.NEED_SCHEMA_RELOAD
    end
    if space == nil then
        return nil, LocateError:new("Space %q doesn't exist", space_name), const.NEED_SCHEMA_RELOAD
    end

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

    local bucket_id_data, err = sharding.key_get_bucket_id(vshard_router, space_name, sharding_key, opts.bucket_id)
    if err ~= nil then
        return nil, err
    end

    sharding.fill_bucket_id_pk(space, key, bucket_id_data.bucket_id)

    local locate_on_storage_opts = {
        bucket_id = bucket_id_data.bucket_id,
        sharding_func_hash = bucket_id_data.sharding_func_hash,
        sharding_key_hash = sharding_key_hash,
        skip_sharding_hash_check = skip_sharding_hash_check,
    }

    local mode = opts.mode or 'read'

    local call_opts = {
        mode = mode,
        prefer_replica = opts.prefer_replica,
        balance = opts.balance,
        timeout = opts.timeout,
        request_timeout = mode == 'read' and opts.request_timeout or nil,
    }

    local storage_result, err = call.single(vshard_router,
        bucket_id_data.bucket_id, CRUD_LOCATE_FUNC_NAME,
        {space_name, key, locate_on_storage_opts},
        call_opts
    )

    if err ~= nil then
        local err_wrapped = LocateError:new("Failed to call locate on storage-side: %s", err)

        if sharding.result_needs_sharding_reload(err) then
            return nil, err_wrapped, const.NEED_SHARDING_RELOAD
        end

        return nil, err_wrapped
    end

    if storage_result.err ~= nil then
        return nil, LocateError:new("Failed to locate: %s", storage_result.err)
    end

    return storage_result.res
end

--- Locate tuple location by space name and primary key via cooler module
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
-- @tparam ?number opts.request_timeout
--  vshard call request_timeout
--  default is the same as opts.timeout
--
-- @tparam ?number opts.bucket_id
--  Bucket ID
--  (by default, it's vshard.router.bucket_id_strcrc32 of primary key)
--
-- @tparam ?boolean opts.prefer_replica
--  Call on replica if it's possible
--
-- @tparam ?boolean opts.balance
--  Use replica according to round-robin load balancing
--
-- @tparam ?string|table opts.vshard_router
--  Cartridge vshard group name or vshard router instance.
--  Set this parameter if your space is not a part of the
--  default vshard cluster.
--
-- @return[1] string 'memtx' or 'vinyl'
-- @return[2] nil
-- @treturn[3] table Error description
--
function locate.call(space_name, key, opts)
    checks('string', '?', {
        timeout = '?number',
        request_timeout = '?number',
        bucket_id = '?',
        prefer_replica = '?boolean',
        balance = '?boolean',
        mode = '?string',
        vshard_router = '?string|table',
    })

    opts = opts or {}

    local vshard_router, err = utils.get_vshard_router_instance(opts.vshard_router)
    if err ~= nil then
        return nil, LocateError:new(err)
    end

    return schema.wrap_func_reload(vshard_router, sharding.wrap_method, call_locate_on_router,
                                   space_name, key, opts)
end

return locate
