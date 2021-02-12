local fiber = require('fiber')
local msgpack = require('msgpack')
local digest = require('digest')
local vshard = require('vshard')
local errors = require('errors')
local log = require('log')

local ReloadSchemaError = errors.new_class('ReloadSchemaError',  {capture_stack = false})

local const = require('crud.common.const')

local schema = {}

local function table_len(t)
    local len = 0
    for _ in pairs(t) do
        len = len + 1
    end
    return len
end

local function call_reload_schema_on_replicaset(replicaset, channel)
    replicaset.master.conn:reload_schema()
    channel:put(true)
end

local function call_reload_schema(replicasets)
    local replicasets_num = table_len(replicasets)
    local channel = fiber.channel(replicasets_num)

    local fibers = {}
    for _, replicaset in pairs(replicasets) do
        local f = fiber.new(call_reload_schema_on_replicaset, replicaset, channel)
        table.insert(fibers, f)
    end

    for _ = 1,replicasets_num do
        if channel:get(const.RELOAD_SCHEMA_TIMEOUT) == nil then
            for _, f in ipairs(fibers) do
                if fiber:status() ~= 'dead' then
                    f:cancel()
                end
            end
            return nil, ReloadSchemaError:new("Reloading schema timed out")
        end
    end

    return true
end

local reload_in_progress = false
local reload_schema_cond = fiber.cond()

local function reload_schema(replicasets)
    if reload_in_progress then
        if not reload_schema_cond:wait(const.RELOAD_SCHEMA_TIMEOUT) then
            return nil, ReloadSchemaError:new('Waiting for schema to be reloaded is timed out')
        end
    else
        reload_in_progress = true

        local ok, err = call_reload_schema(replicasets)
        if not ok then
            return nil, err
        end

        reload_schema_cond:broadcast()
        reload_in_progress = false
    end

    return true
end

-- schema.wrap_func_reload calls func with specified arguments.
-- func should return `res, err, need_reload`
-- If function returned error and `need_reload` is true,
-- then schema is reloaded and one more attempt is performed
-- (but no more than RELOAD_RETRIES_NUM).
-- This wrapper is used for functions that can fail if router uses outdated
-- space schema. In case of such errors these functions returns `need_reload`
-- for schema-dependent errors.
function schema.wrap_func_reload(func, ...)
    local i = 0

    local res, err, need_reload
    while true do
        res, err, need_reload = func(...)

        if err == nil or not need_reload then
            break
        end

        local ok, reload_schema_err = reload_schema(vshard.router.routeall())
        if not ok then
            log.warn("Failed to reload schema: %s", reload_schema_err)
            break
        end

        i = i + 1
        if i > const.RELOAD_RETRIES_NUM then
            break
        end
    end

    return res, err
end

local function get_space_schema_hash(space)
    if space == nil then
        return ''
    end

    local indexes_info = {}
    for i = 0, table.maxn(space.index) do
        local index = space.index[i]
        if index ~= nil then
            indexes_info[i] = {
                unique = index.unique,
                parts = index.parts,
                id = index.id,
                type = index.type,
                name = index.name,
                path = index.path,
            }
        end
    end

    local space_info = {
        format = space:format(),
        indexes = indexes_info,
    }

    return digest.murmur(msgpack.encode(space_info))
end

local function get_partial_result(func_get_res, fields)
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
-- schema.wrap_box_space_func_result pcalls some box.space function
-- and returns its result as a table
-- `{res = ..., err = ..., space_schema_hash = ...}`
-- space_schema_hash is computed if function failed and
-- `add_space_schema_hash` is true
function schema.wrap_box_space_func_result(space, func_name, args, opts)
    local result = {}

    opts = opts or {}

    local ok, func_res = pcall(space[func_name], space, unpack(args))
    if not ok then
        result.err = func_res
        if opts.add_space_schema_hash then
            result.space_schema_hash = get_space_schema_hash(space)
        end
    else
        result.res = func_res
    end

    return get_partial_result(result, opts.fields)
end

-- schema.result_needs_reload checks that schema reload can
-- be helpful to avoid storage error.
-- It checks if space_schema_hash returned by storage
-- is the same as hash of space used on router.
-- Note, that storage returns `space_schema_hash = nil`
-- if reloading space format can't avoid the error.
function schema.result_needs_reload(space, result)
    if result.space_schema_hash == nil then
        return false
    end
    return result.space_schema_hash ~= get_space_schema_hash(space)
end

return schema
