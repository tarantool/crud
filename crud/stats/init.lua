---- CRUD statistics module.
-- @module crud.stats
--

local clock = require('clock')
local checks = require('checks')
local fun = require('fun')
local log = require('log')
local vshard = require('vshard')

local dev_checks = require('crud.common.dev_checks')
local stash = require('crud.common.stash')
local utils = require('crud.common.utils')
local op_module = require('crud.stats.operation')
local registry = require('crud.stats.local_registry')

local stats = {}
local internal = stash.get(stash.name.stats_internal)

--- Check if statistics module was enabled.
--
-- @function is_enabled
--
-- @treturn boolean Returns `true` or `false`.
--
function stats.is_enabled()
    return internal.is_enabled == true
end

--- Initializes statistics registry, enables callbacks and wrappers.
--
--  If already enabled, do nothing.
--
-- @function enable
--
-- @treturn boolean Returns `true`.
--
function stats.enable()
    if stats.is_enabled() then
        return true
    end

    internal.is_enabled = true
    registry.init()

    return true
end

--- Resets statistics registry.
--
--  After reset collectors are the same as right
--  after initial `stats.enable()`.
--
-- @function reset
--
-- @treturn boolean Returns true.
--
function stats.reset()
    if not stats.is_enabled() then
        return true
    end

    registry.destroy()
    registry.init()

    return true
end

--- Destroys statistics registry and disable callbacks.
--
--  If already disabled, do nothing.
--
-- @function disable
--
-- @treturn boolean Returns true.
--
function stats.disable()
    if not stats.is_enabled() then
        return true
    end

    registry.destroy()
    internal.is_enabled = false

    return true
end

--- Get statistics on CRUD operations.
--
-- @function get
--
-- @string[opt] space_name
--  If specified, returns table with statistics
--  of operations on space, separated by operation type and
--  execution status. If there wasn't any requests of "op" type
--  for space, there won't be corresponding collectors.
--  If not specified, returns table with statistics
--  about all observed spaces.
--
-- @treturn table Statistics on CRUD operations.
--  If statistics disabled, returns `{}`.
--
function stats.get(space_name)
    checks('?string')

    if not stats.is_enabled() then
        return {}
    end

    return registry.get(space_name)
end

local function resolve_space_name(space_id)
    local replicasets = vshard.router.routeall()
    if next(replicasets) == nil then
        log.warn('Failed to resolve space name for stats: no replicasets found')
        return nil
    end

    local space = utils.get_space(space_id, replicasets)
    if space == nil then
        log.warn('Failed to resolve space name for stats: no space found for id %d', space_id)
        return nil
    end

    return space.name
end

-- Hack to set __gc for a table in Lua 5.1
-- See https://stackoverflow.com/questions/27426704/lua-5-1-workaround-for-gc-metamethod-for-tables
-- or https://habr.com/ru/post/346892/
local function setmt__gc(t, mt)
    local prox = newproxy(true)
    getmetatable(prox).__gc = function() mt.__gc(t) end
    t[prox] = true
    return setmetatable(t, mt)
end

-- If jit will be enabled here, gc_observer usage
-- may be optimized so our __gc hack will not work.
local function keep_observer_alive(gc_observer) --luacheck: ignore
end
jit.off(keep_observer_alive)

local function wrap_pairs_gen(build_latency, space_name, op, gen, param, state)
    local total_latency = build_latency

    -- If pairs() cycle will be interrupted with break,
    -- we'll never get a proper obervation.
    -- We create an object with the same lifespan as gen()
    -- function so if someone break pairs cycle,
    -- it still will be observed.
    local observed = false

    local gc_observer = setmt__gc({}, {
        __gc = function()
            if observed == false then
                registry.observe(total_latency, space_name, op, 'ok')
                observed = true
            end
        end
    })

    local wrapped_gen = function(param, state)
        -- Mess with gc_observer so its lifespan will
        -- be the same as wrapped_gen() function.
        keep_observer_alive(gc_observer)

        local start_time = clock.monotonic()

        local status, next_state, var = pcall(gen, param, state)

        local finish_time = clock.monotonic()

        total_latency = total_latency + (finish_time - start_time)

        if status == false then
            registry.observe(total_latency, space_name, op, 'error')
            observed = true
            error(next_state, 2)
        end

        -- Observe stats in the end of pairs cycle
        if var == nil then
            registry.observe(total_latency, space_name, op, 'ok')
            observed = true
            return nil
        end

        return next_state, var
    end

    return fun.wrap(wrapped_gen, param, state)
end

local function wrap_tail(space_name, op, pairs, start_time, call_status, ...)
    dev_checks('string|number', 'string', 'boolean', 'number', 'boolean')

    local finish_time = clock.monotonic()
    local latency = finish_time - start_time

    -- If space id is provided instead of name, try to resolve name.
    -- If resolve have failed, use id as string to observe space.
    -- If using space id will be deprecated, remove this code as well,
    -- see https://github.com/tarantool/crud/issues/255
    if type(space_name) ~= 'string' then
        local name = resolve_space_name(space_name)
        if name ~= nil then
            space_name = name
        else
            space_name = tostring(space_name)
        end
    end

    if call_status == false then
        registry.observe(latency, space_name, op, 'error')
        error((...), 2)
    end

    if pairs == false then
        if select(2, ...) ~= nil then
            -- If not `pairs` call, return values `nil, err`
            -- treated as error case.
            registry.observe(latency, space_name, op, 'error')
            return ...
        else
            registry.observe(latency, space_name, op, 'ok')
            return ...
        end
    else
        return wrap_pairs_gen(latency, space_name, op, ...)
    end
end

--- Wrap CRUD operation call to collect statistics.
--
--  Approach based on `box.atomic()`:
--  https://github.com/tarantool/tarantool/blob/b9f7204b5e0d10b443c6f198e9f7f04e0d16a867/src/box/lua/schema.lua#L369
--
-- @function wrap
--
-- @func func
--  Function to wrap. First argument is expected to
--  be a space name string. If statistics enabled,
--  errors are caught and thrown again.
--
-- @string op
--  Label of registry collectors.
--  Use `require('crud.stats').op` to pick one.
--
-- @tab[opt] opts
--
-- @bool[opt=false] opts.pairs
--  If false, wraps only function passed as argument.
--  Second return value of wrapped function is treated
--  as error (`nil, err` case).
--  If true, also wraps gen() function returned by
--  call. Statistics observed on cycle end (last
--  element was fetched or error was thrown). If pairs
--  cycle was interrupted with `break`, statistics will
--  be collected when pairs objects are cleaned up with
--  Lua garbage collector.
--
-- @return Wrapped function output.
--
function stats.wrap(func, op, opts)
    dev_checks('function', 'string', { pairs = '?boolean' })

    local pairs
    if type(opts) == 'table' and opts.pairs ~= nil then
        pairs = opts.pairs
    else
        pairs = false
    end

    return function(space_name, ...)
        if not stats.is_enabled() then
            return func(space_name, ...)
        end

        local start_time = clock.monotonic()

        return wrap_tail(
            space_name, op, pairs, start_time,
            pcall(func, space_name, ...)
        )
    end
end

local storage_stats_schema = { tuples_fetched = 'number', tuples_lookup = 'number' }
--- Callback to collect storage tuples stats (select/pairs).
--
-- @function update_fetch_stats
--
-- @tab storage_stats
--  Statistics from select storage call.
--
-- @number storage_stats.tuples_fetched
--  Count of tuples fetched during storage call.
--
-- @number storage_stats.tuples_lookup
--  Count of tuples looked up on storages while collecting response.
--
-- @string space_name
--  Name of space.
--
-- @treturn boolean Returns `true`.
--
function stats.update_fetch_stats(storage_stats, space_name)
    dev_checks(storage_stats_schema, 'string')

    if not stats.is_enabled() then
        return true
    end

    registry.observe_fetch(
        storage_stats.tuples_fetched,
        storage_stats.tuples_lookup,
        space_name
    )

    return true
end

--- Callback to collect planned map reduces stats (select/pairs).
--
-- @function update_map_reduces
--
-- @string space_name
--  Name of space.
--
-- @treturn boolean Returns `true`.
--
function stats.update_map_reduces(space_name)
    dev_checks('string')

    if not stats.is_enabled() then
        return true
    end

    registry.observe_map_reduces(1, space_name)

    return true
end

--- Table with CRUD operation lables.
--
-- @tfield string INSERT
--  Identifies both `insert` and `insert_object`.
--
-- @tfield string GET
--
-- @tfield string REPLACE
--  Identifies both `replace` and `replace_object`.
--
-- @tfield string UPDATE
--
-- @tfield string UPSERT
--  Identifies both `upsert` and `upsert_object`.
--
-- @tfield string DELETE
--
-- @tfield string SELECT
--  Identifies both `pairs` and `select`.
--
-- @tfield string TRUNCATE
--
-- @tfield string LEN
--
-- @tfield string COUNT
--
-- @tfield string BORDERS
--  Identifies both `min` and `max`.
--
stats.op = op_module

--- Stats module internal state (for debug/test).
--
-- @tfield[opt] boolean is_enabled Is currently enabled.
stats.internal = internal

return stats
