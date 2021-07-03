local checks = require('checks')
local errors = require('errors')
local vshard = require('vshard')

local call = require('crud.common.call')
local utils = require('crud.common.utils')
local sharding = require('crud.common.sharding')
local dev_checks = require('crud.common.dev_checks')
local schema = require('crud.common.schema')

local DeleteError = errors.new_class('DeleteError',  {capture_stack = false})

local delete = {}

local DELETE_FUNC_NAME = '_crud.delete_on_storage'

local function delete_on_storage(space_name, key, field_names)
    dev_checks('string', '?', '?table')

    local space = box.space[space_name]
    if space == nil then
        return nil, DeleteError:new("Space %q doesn't exist", space_name)
    end

    -- add_space_schema_hash is false because
    -- reloading space format on router can't avoid delete error on storage
    return schema.wrap_box_space_func_result(space, 'delete', {key}, {
        add_space_schema_hash = false,
        field_names = field_names,
    })
end

function delete.init()
   _G._crud.delete_on_storage = delete_on_storage
end

-- returns result, err, need_reload
-- need_reload indicates if reloading schema could help
-- see crud.common.schema.wrap_func_reload()
local function call_delete_on_router(space_name, key, opts)
    dev_checks('string', '?', {
        timeout = '?number',
        bucket_id = '?number|cdata',
        fields = '?table',
    })

    opts = opts or {}

    local space = utils.get_space(space_name, vshard.router.routeall())
    if space == nil then
        return nil, DeleteError:new("Space %q doesn't exist", space_name), true
    end

    if box.tuple.is(key) then
        key = key:totable()
    end

    local bucket_id = sharding.key_get_bucket_id(key, opts.bucket_id)
    local call_opts = {
        mode = 'write',
        timeout = opts.timeout,
    }
    local storage_result, err = call.single(
        bucket_id, DELETE_FUNC_NAME,
        {space_name, key, opts.fields},
        call_opts
    )

    if err ~= nil then
        return nil, DeleteError:new("Failed to call delete on storage-side: %s", err)
    end

    if storage_result.err ~= nil then
        return nil, DeleteError:new("Failed to delete: %s", storage_result.err)
    end

    local tuple = storage_result.res

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
-- @return[1] object
-- @treturn[2] nil
-- @treturn[2] table Error description
--
function delete.call(space_name, key, opts)
    checks('string', '?', {
        timeout = '?number',
        bucket_id = '?number|cdata',
        fields = '?table',
    })

    return schema.wrap_func_reload(call_delete_on_router, space_name, key, opts)
end

return delete
