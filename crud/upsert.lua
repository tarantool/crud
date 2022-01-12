---- Module
-- @module crud.upsert
--
local checks = require('checks')
local errors = require('errors')
local vshard = require('vshard')

local call = require('crud.common.call')
local utils = require('crud.common.utils')
local sharding = require('crud.common.sharding')
local dev_checks = require('crud.common.dev_checks')
local schema = require('crud.common.schema')

local UpsertError = errors.new_class('UpsertError', { capture_stack = false})

local upsert = {}

local UPSERT_FUNC_NAME = '_crud.upsert_on_storage'

local function upsert_on_storage(space_name, tuple, operations, opts)
    dev_checks('string', 'table', 'table', {
        add_space_schema_hash = '?boolean',
    })

    opts = opts or {}

    local space = box.space[space_name]
    if space == nil then
        return nil, UpsertError:new("Space %q doesn't exist", space_name)
    end

    -- add_space_schema_hash is true only in case of upsert_object
    -- the only one case when reloading schema can avoid insert error
    -- is flattening object on router
    return schema.wrap_box_space_func_result(space, 'upsert', {tuple, operations}, {
        add_space_schema_hash = opts.add_space_schema_hash,
    })
end

function upsert.init()
   _G._crud.upsert_on_storage = upsert_on_storage
end

-- returns result, err, need_reload
-- need_reload indicates if reloading schema could help
-- see crud.common.schema.wrap_func_reload()
local function call_upsert_on_router(space_name, tuple, user_operations, opts)
    dev_checks('string', '?', 'table', {
        timeout = '?number',
        bucket_id = '?number|cdata',
        add_space_schema_hash = '?boolean',
        fields = '?table',
    })

    opts = opts or {}

    local space, err = utils.get_space(space_name, vshard.router.routeall())
    if err ~= nil then
        return nil, UpsertError:new("Failed to get space %q: %s", space_name, err), true
    end

    if space == nil then
        return nil, UpsertError:new("Space %q doesn't exist", space_name), true
    end

    local space_format = space:format()
    local operations = user_operations
    if not utils.tarantool_supports_fieldpaths() then
        operations, err = utils.convert_operations(user_operations, space_format)
        if err ~= nil then
            return nil, UpsertError:new("Wrong operations are specified: %s", err), true
        end
    end

    local bucket_id, err = sharding.tuple_set_and_return_bucket_id(tuple, space, opts.bucket_id)
    if err ~= nil then
        return nil, UpsertError:new("Failed to get bucket ID: %s", err), true
    end

    local call_opts = {
        mode = 'write',
        timeout = opts.timeout,
    }
    local storage_result, err = call.single(
        bucket_id, UPSERT_FUNC_NAME,
        {space_name, tuple, operations},
        call_opts
    )

    if err ~= nil then
        return nil, UpsertError:new("Failed to call upsert on storage-side: %s", err)
    end

    if storage_result.err ~= nil then
        local need_reload = schema.result_needs_reload(space, storage_result)
        return nil, UpsertError:new("Failed to upsert: %s", storage_result.err), need_reload
    end

    -- upsert always returns nil
    return utils.format_result({}, space, opts.fields)
end

-- Update or insert a tuple in the specified space
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
    })

    return schema.wrap_func_reload(call_upsert_on_router, space_name, tuple, user_operations, opts)
end

-- Update or insert an object in the specified space
--
-- @function object
--
-- @string space_name
--  A space name
--
-- @table obj
--  Object
--
-- @table user_operations
--  user_operations to be performed.
--  See `space:upsert()` operations in Tarantool doc
--
-- @table[opt] opts
--  Options of upsert.tuple
--
-- @return[1] object
-- @treturn[2] nil
-- @treturn[2] table Error description
--
function upsert.object(space_name, obj, user_operations, opts)
    checks('string', 'table', 'table', '?table')

    -- upsert can fail if router uses outdated schema to flatten object
    opts = utils.merge_options(opts, {add_space_schema_hash = true})

    local tuple, err = utils.flatten_obj_reload(space_name, obj)
    if err ~= nil then
        return nil, UpsertError:new("Failed to flatten object: %s", err)
    end

    return upsert.tuple(space_name, tuple, user_operations, opts)
end

return upsert
