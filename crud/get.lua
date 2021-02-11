local checks = require('checks')
local errors = require('errors')
local vshard = require('vshard')

local call = require('crud.common.call')
local utils = require('crud.common.utils')
local sharding = require('crud.common.sharding')
local dev_checks = require('crud.common.dev_checks')
local schema = require('crud.common.schema')

local GetError = errors.new_class('Get',  {capture_stack = false})

local get = {}

local GET_FUNC_NAME = '_crud.get_on_storage'

local function get_field_metadata(full_metadata, field)
    dev_checks('table', 'string')

    for _, tuple in ipairs(full_metadata) do
        if tuple['name'] == field then
            return tuple
        end
    end
end

local function format_metadata(full_metadata, fields)
    dev_checks('table', '?table')

    local metadata = {}

    for i, field in ipairs(fields) do
        metadata[i] = get_field_metadata(full_metadata, field)
    end

    return metadata
end

local function format_result_by_fields(formatted_result, fields)
    dev_checks('table', '?table')

    local result = {}

    result.rows = formatted_result.rows
    if fields ~= nil then
        result.metadata = format_metadata(formatted_result.metadata, fields)
    else
        result.metadata = formatted_result.metadata
    end

    return result
end

local function get_partial_result(func_get_res, fields)
    dev_checks('table', '?table')

    local result = {}

    result.err = func_get_res.err
    if func_get_res.res ~= nil then
        if fields ~= nil then
            result.res = {}
            for i, field in ipairs(fields) do
                result.res[i] = func_get_res.res[field]
            end
        else
            result.res = func_get_res.res
        end
    end

    return result
end

local function get_on_storage(space_name, key, fields)
    dev_checks('string', '?', '?table')

    local space = box.space[space_name]
    if space == nil then
        return nil, GetError:new("Space %q doesn't exist", space_name)
    end

    -- add_space_schema_hash is false because
    -- reloading space format on router can't avoid get error on storage
    local func_res = schema.wrap_box_space_func_result(false, space, 'get', key)

    return get_partial_result(func_res, fields)
end

function get.init()
   _G._crud.get_on_storage = get_on_storage
end

-- returns result, err, need_reload
-- need_reload indicates if reloading schema could help
-- see crud.common.schema.wrap_func_reload()
local function call_get_on_router(space_name, key, opts)
    dev_checks('string', '?', {
        timeout = '?number',
        bucket_id = '?number|cdata',
        fields = '?table',
    })

    opts = opts or {}

    local space = utils.get_space(space_name, vshard.router.routeall())
    if space == nil then
        return nil, GetError:new("Space %q doesn't exist", space_name), true
    end

    if box.tuple.is(key) then
        key = key:totable()
    end

    local bucket_id = sharding.key_get_bucket_id(key, opts.bucket_id)
    -- We don't use callro() here, because if the replication is
    -- async, there could be a lag between master and replica, so a
    -- connector which sequentially calls put() and then get() may get
    -- a stale result.
    local storage_result, err = call.rw_single(
        bucket_id, GET_FUNC_NAME,
        {space_name, key},
        {timeout = opts.timeout}
    )

    if err ~= nil then
        return nil, GetError:new("Failed to call get on storage-side: %s", err)
    end

    if storage_result.err ~= nil then
        return nil, GetError:new("Failed to get: %s", storage_result.err)
    end

    local tuple = storage_result.res

    -- protect against box.NULL
    if tuple == nil then
        tuple = nil
    end

    local formatted_result = utils.format_result({tuple}, space)

    return format_result_by_fields(formatted_result, opts.fields)
end

--- Get tuple from the specified space by key
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
-- @return[1] object
-- @treturn[2] nil
-- @treturn[2] table Error description
--
function get.call(space_name, key, opts)
    checks('string', '?', {
        timeout = '?number',
        bucket_id = '?number|cdata',
        fields = '?table',
    })

    return schema.wrap_func_reload(call_get_on_router, space_name, key, opts)
end

return get
