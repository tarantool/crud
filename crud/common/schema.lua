local fiber = require('fiber')
local msgpack = require('msgpack')
local digest = require('digest')
local errors = require('errors')
local log = require('log')

local ReloadSchemaError = errors.new_class('ReloadSchemaError', {capture_stack = false})

local const = require('crud.common.const')
local dev_checks = require('crud.common.dev_checks')
local utils = require('crud.common.vshard_utils')

local schema = {}

local function table_len(t)
    local len = 0
    for _ in pairs(t) do
        len = len + 1
    end
    return len
end

local function call_reload_schema_on_replicaset(replicaset, channel)
    local master = utils.get_replicaset_master(replicaset, {cached = false})
    master.conn:reload_schema()
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
                if f:status() ~= 'dead' then
                    f:cancel()
                end
            end
            return nil, ReloadSchemaError:new("Reloading schema timed out")
        end
    end

    return true
end

local reload_in_progress = {}
local reload_schema_cond = {}

function schema.reload_schema(vshard_router)
    local replicasets = vshard_router:routeall()
    local vshard_router_name = vshard_router.name

    if reload_in_progress[vshard_router_name] == true then
        if not reload_schema_cond[vshard_router_name]:wait(const.RELOAD_SCHEMA_TIMEOUT) then
            return nil, ReloadSchemaError:new('Waiting for schema to be reloaded is timed out')
        end
    else
        reload_in_progress[vshard_router_name] = true
        if reload_schema_cond[vshard_router_name] == nil then
            reload_schema_cond[vshard_router_name] = fiber.cond()
        end

        local ok, err = call_reload_schema(replicasets)
        if not ok then
            return nil, err
        end

        reload_schema_cond[vshard_router_name]:broadcast()
        reload_in_progress[vshard_router_name] = false
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
function schema.wrap_func_reload(vshard_router, func, ...)
    local i = 0

    local res, err, need_reload
    while true do
        res, err, need_reload = func(vshard_router, ...)

        if err == nil or need_reload ~= const.NEED_SCHEMA_RELOAD then
            break
        end

        local ok, reload_schema_err = schema.reload_schema(vshard_router)
        if not ok then
            log.warn("Failed to reload schema: %s", reload_schema_err)
            break
        end

        i = i + 1
        if i > const.RELOAD_RETRIES_NUM then
            local warn_msg = "Number of attempts to reload schema has been ended: %s"
            log.warn(warn_msg, const.RELOAD_RETRIES_NUM)
            break
        end
    end

    return res, err
end

schema.get_normalized_space_schema = function(space)
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

    return {
        format = space:format(),
        indexes = indexes_info,
    }
end

local function get_space_schema_hash(space)
    if space == nil then
        return ''
    end

    local sch = schema.get_normalized_space_schema(space)
    return digest.murmur(msgpack.encode(sch))
end

function schema.filter_obj_fields(obj, field_names)
    if field_names == nil or obj == nil then
        return obj
    end

    local result = {}

    for _, field_name in ipairs(field_names) do
        result[field_name] = obj[field_name]
    end

    return result
end

local function filter_tuple_fields(tuple, field_names)
    if field_names == nil or tuple == nil then
        return tuple
    end

    local result = {}

    for i, field_name in ipairs(field_names) do
        result[i] = tuple[field_name]
    end

    return result
end

function schema.filter_tuples_fields(tuples, field_names)
    dev_checks('?table', '?table')

    if field_names == nil then
        return tuples
    end

    local result = {}

    for _, tuple in ipairs(tuples) do
        local filtered_tuple = filter_tuple_fields(tuple, field_names)
        table.insert(result, filtered_tuple)
    end

    return result
end

function schema.truncate_row_trailing_fields(tuple, field_names)
    dev_checks('table|tuple', 'table')

    local count_names = #field_names
    local index = count_names + 1
    local len_tuple = #tuple

    if box.tuple.is(tuple) then
        return tuple:transform(index, len_tuple - count_names)
    end

    for i = index, len_tuple do
        tuple[i] = nil
    end

    return tuple
end

function schema.wrap_func_result(space, func, args, opts)
    dev_checks('table', 'function', 'table', 'table')

    local result = {}

    opts = opts or {}

    local ok, func_res = pcall(func, unpack(args))
    if not ok then
        result.err = func_res
        if opts.add_space_schema_hash then
            result.space_schema_hash = get_space_schema_hash(space)
        end
    else
        if opts.noreturn ~= true then
            result.res = filter_tuple_fields(func_res, opts.field_names)
        end
    end

    if opts.fetch_latest_metadata == true then
        local replica_schema_version
        if box.info.schema_version ~= nil then
            replica_schema_version = box.info.schema_version
        else
            replica_schema_version = box.internal.schema_version()
        end
        result.storage_info = {
            replica_uuid = box.info().uuid, -- Backward compatibility.
            replica_id = utils.get_self_vshard_replica_id(), -- Replacement for replica_uuid.
            replica_schema_version = replica_schema_version,
        }
    end

    return result
end

-- schema.wrap_box_space_func_result pcalls some box.space function
-- and returns its result as a table
-- `{res = ..., err = ..., space_schema_hash = ...}`
-- space_schema_hash is computed if function failed and
-- `add_space_schema_hash` is true
function schema.wrap_box_space_func_result(space, box_space_func_name, box_space_func_args, opts)
    dev_checks('table', 'string', 'table', 'table')
    local function func(space, box_space_func_name, box_space_func_args)
        return space[box_space_func_name](space, unpack(box_space_func_args))
    end

    return schema.wrap_func_result(space, func, {space, box_space_func_name, box_space_func_args}, opts)
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

function schema.batching_result_needs_reload(space, results, tuples_count)
    local storage_errs_count = 0
    local space_schema_hash = get_space_schema_hash(space)
    for _, result in ipairs(results) do
        if result.space_schema_hash ~= nil and result.space_schema_hash ~= space_schema_hash then
            storage_errs_count = storage_errs_count + 1
        end
    end

    return storage_errs_count == tuples_count
end

return schema
