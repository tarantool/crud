local checks = require('checks')
local errors = require('errors')
local vshard = require('vshard')

local call = require('crud.common.call')
local registry = require('crud.common.registry')
local utils = require('crud.common.utils')
local dev_checks = require('crud.common.dev_checks')

local ReplaceError = errors.new_class('Replace', { capture_stack = false })

local replace = {}

local REPLACE_FUNC_NAME = '__replace'

local function call_replace_on_storage(space_name, tuple)
    dev_checks('string', 'table')

    local space = box.space[space_name]
    if space == nil then
        return nil, ReplaceError:new("Space %q doesn't exist", space_name)
    end

    return space:replace(tuple)
end

function replace.init()
    registry.add({
        [REPLACE_FUNC_NAME] = call_replace_on_storage,
    })
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
-- @tparam ?number opts.show_bucket_id
--  Flag indicating whether to add bucket_id into return dataset or not (default is false)
--
-- @return[1] object
-- @treturn[2] nil
-- @treturn[2] table Error description
--
function replace.tuple(space_name, tuple, opts)
    checks('string', 'table', {
        timeout = '?number',
        show_bucket_id = '?boolean',
    })

    opts = opts or {}

    local space = utils.get_space(space_name, vshard.router.routeall())
    if space == nil then
        return nil, ReplaceError:new("Space %q doesn't exist", space_name)
    end

    local key = utils.extract_key(tuple, space.index[0].parts)

    local bucket_id = vshard.router.bucket_id_strcrc32(key)
    local replicaset, err = vshard.router.route(bucket_id)
    if replicaset == nil then
        return nil, ReplaceError:new("Failed to get replicaset for bucket_id %s: %s", bucket_id, err.err)
    end

    local bucket_id_fieldno, err = utils.get_bucket_id_fieldno(space)
    if err ~= nil then
        return nil, err
    end

    if tuple[bucket_id_fieldno] ~= nil then
        return nil, ReplaceError:new("Unexpected value (%s) at field %s (sharding key)",
                tuple[bucket_id_fieldno], bucket_id_fieldno)
    end

    tuple[bucket_id_fieldno] = bucket_id
    local results, err = call.rw(REPLACE_FUNC_NAME, {space_name, tuple}, {
        replicasets = {replicaset},
        timeout = opts.timeout,
    })

    if err ~= nil then
        return nil, ReplaceError:new("Failed to replace: %s", err)
    end

    local tuple = results[replicaset.uuid]
    local metadata = table.copy(space:format())

    if not opts.show_bucket_id then
        if tuple then
            table.remove(tuple, bucket_id_fieldno)
        end
        table.remove(metadata, bucket_id_fieldno)
    end

    return {
        metadata = metadata,
        rows = {tuple},
    }
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
-- @tparam ?number opts.timeout
--  Function call timeout
--
-- @return[1] object
-- @treturn[2] nil
-- @treturn[2] table Error description
--
function replace.object(space_name, obj, opts)
    checks('string', 'table', {
        timeout = '?number',
        show_bucket_id = '?boolean',
    })

    opts = opts or {}

    local space = utils.get_space(space_name, vshard.router.routeall())
    if space == nil then
        return nil, ReplaceError:new("Space %q doesn't exist", space_name)
    end

    local space_format = space:format()
    local tuple, err = utils.flatten(obj, space_format)
    if err ~= nil then
        return nil, ReplaceError:new("Object is specified in bad format: %s", err)
    end

    return replace.tuple(space_name, tuple, opts)
end

return replace
