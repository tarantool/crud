local checks = require('checks')
local errors = require('errors')
local vshard = require('vshard')

local call = require('crud.common.call')
local utils = require('crud.common.utils')
local sharding = require('crud.common.sharding')
local dev_checks = require('crud.common.dev_checks')
local schema = require('crud.common.schema')

local ReplaceError = errors.new_class('Replace', { capture_stack = false })

local replace = {}

local REPLACE_FUNC_NAME = '_crud.replace_on_storage'

local function replace_on_storage(space_name, tuple, opts)
    dev_checks('string', 'table', {
        add_space_schema_hash = '?boolean',
        fields = '?table',
    })

    opts = opts or {}

    local space = box.space[space_name]
    if space == nil then
        return nil, ReplaceError:new("Space %q doesn't exist", space_name)
    end

    -- add_space_schema_hash is true only in case of replace_object
    -- the only one case when reloading schema can avoid insert error
    -- is flattening object on router
    return schema.wrap_box_space_func_result(space, 'replace', {tuple}, {
        add_space_schema_hash = opts.add_space_schema_hash,
        fields = opts.fields,
    })
end

function replace.init()
   _G._crud.replace_on_storage = replace_on_storage
end

-- returns result, err, need_reload
-- need_reload indicates if reloading schema could help
-- see crud.common.schema.wrap_func_reload()
local function call_replace_on_router(space_name, tuple, opts)
    dev_checks('string', 'table', {
        timeout = '?number',
        bucket_id = '?number|cdata',
        add_space_schema_hash = '?boolean',
        fields = '?table',
    })

    opts = opts or {}

    local space, err = utils.get_space(space_name, vshard.router.routeall())
    if err ~= nil then
        return nil, ReplaceError:new("Failed to get space %q: %s", space_name, err), true
    end

    if space == nil then
        return nil, ReplaceError:new("Space %q doesn't exist", space_name), true
    end

    local bucket_id, err = sharding.tuple_set_and_return_bucket_id(tuple, space, opts.bucket_id)
    if err ~= nil then
        return nil, ReplaceError:new("Failed to get bucket ID: %s", err), true
    end

    local insert_on_storage_opts = {
        add_space_schema_hash = opts.add_space_schema_hash,
        fields = opts.fields,
    }

    local storage_result, err = call.rw_single(
        bucket_id, REPLACE_FUNC_NAME,
        {space_name, tuple, insert_on_storage_opts},
        {timeout = opts.timeout}
    )

    if err ~= nil then
        return nil, ReplaceError:new("Failed to call replace on storage-side: %s", err)
    end

    if storage_result.err ~= nil then
        local need_reload = schema.result_needs_reload(space, storage_result)
        return nil, ReplaceError:new("Failed to replace: %s", storage_result.err), need_reload
    end

    local tuple = storage_result.res

    return utils.format_result({tuple}, space, opts.fields)
end

--- Insert or replace a tuple in the specified space
--
-- @function tuple
--
-- @param string space_name
--  A space name
--
-- @param table tuple
--  Tuple
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
function replace.tuple(space_name, tuple, opts)
    checks('string', 'table', {
        timeout = '?number',
        bucket_id = '?number|cdata',
        add_space_schema_hash = '?boolean',
        fields = '?table',
    })

    return schema.wrap_func_reload(call_replace_on_router, space_name, tuple, opts)
end

--- Insert or replace an object in the specified space
--
-- @function object
--
-- @param string space_name
--  A space name
--
-- @param table obj
--  Object
--
-- @tparam ?table opts
--  Options of replace.tuple
--
-- @return[1] object
-- @treturn[2] nil
-- @treturn[2] table Error description
--
function replace.object(space_name, obj, opts)
    checks('string', 'table', '?table')

    opts = opts or {}

    -- replace can fail if router uses outdated schema to flatten object
    opts.add_space_schema_hash = true

    local tuple, err = utils.flatten_obj_reload(space_name, obj)
    if err ~= nil then
        return nil, ReplaceError:new("Failed to flatten object: %s", err)
    end

    return replace.tuple(space_name, tuple, opts)
end

return replace
