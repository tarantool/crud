---- CRUD statistics module.
-- @module crud.stats
--

local clock = require('clock')
local checks = require('checks')
local errors = require('errors')
local fiber = require('fiber')
local fun = require('fun')

local dev_checks = require('crud.common.dev_checks')
local stash = require('crud.common.stash')
local op_module = require('crud.stats.operation')

local StatsError = errors.new_class('StatsError', {capture_stack = false})

local stats = {}
local internal = stash.get(stash.name.stats_internal)

local local_registry = require('crud.stats.local_registry')
local metrics_registry = require('crud.stats.metrics_registry')

local drivers = {
    ['local'] = local_registry,
}
if metrics_registry.is_supported() then
    drivers['metrics'] = metrics_registry
end

function internal:get_registry()
    if self.driver == nil then
        return nil
    end
    return drivers[self.driver]
end

--- Check if statistics module was enabled.
--
-- @function is_enabled
--
-- @treturn boolean Returns `true` or `false`.
--
function stats.is_enabled()
    return internal.driver ~= nil
end

--- Get default statistics driver name.
--
-- @function get_default_driver
--
-- @treturn string `metrics` if supported, `local` if unsupported.
--
function stats.get_default_driver()
    if drivers.metrics ~= nil then
        return 'metrics'
    else
        return 'local'
    end
end

--- Check if provided driver is supported.
--
-- @function is_driver_supported
--
-- @string opts.driver
--
-- @treturn boolean Returns `true` or `false`.
--
function stats.is_driver_supported(driver)
    return drivers[driver] ~= nil
end

--- Initializes statistics registry, enables callbacks and wrappers.
--
--  If already enabled, do nothing.
--
-- @function enable
--
-- @tab[opt] opts
--
-- @string[opt] opts.driver
--  `'local'` or `'metrics'`.
--  If `'local'`, stores statistics in local registry (some Lua tables)
--  and computes latency as overall average. `'metrics'` requires
--  `metrics >= 0.9.0` installed and stores statistics in
--  global metrics registry (integrated with exporters).
--  `'metrics'` driver supports computing latency as 0.99 quantile with aging.
--  If `'metrics'` driver is available, it is used by default,
--  otherwise `'local'` is used.
--
-- @bool[opt=false] opts.quantiles
--  If `'metrics'` driver used, you can enable
--  computing requests latency as 0.99 quantile with aging.
--  Performance overhead for enabling is near 10%.
--
-- @number[opt=1e-3] opts.quantile_tolerated_error
--  See tarantool/metrics summary API for details:
--  https://www.tarantool.io/ru/doc/latest/book/monitoring/api_reference/#summary
--  If quantile value is -Inf, try to decrease quantile tolerated error.
--  See https://github.com/tarantool/metrics/issues/189 for issue details.
--
-- @number[opt=2] opts.quantile_age_buckets_count
--  Count of summary quantile buckets.
--  See tarantool/metrics summary API for details:
--  https://www.tarantool.io/ru/doc/latest/book/monitoring/api_reference/#summary
--  Increasing the value smoothes time window move,
--  but consumes additional memory and CPU.
--
-- @number[opt=60] opts.quantile_max_age_time
--  Duration of each bucket’s lifetime in seconds.
--  See tarantool/metrics summary API for details:
--  https://www.tarantool.io/ru/doc/latest/book/monitoring/api_reference/#summary
--  Smaller bucket lifetime results in smaller time window for quantiles,
--  but more CPU is spent on bucket rotation. If your application has low request
--  frequency, increase the value to reduce the amount of `-nan` gaps in quantile values.
--
-- @treturn boolean Returns `true`.
--
function stats.enable(opts)
    checks({
        driver = '?string',
        quantiles = '?boolean',
        quantile_tolerated_error = '?number',
        quantile_age_buckets_count = '?number',
        quantile_max_age_time = '?number',
    })

    StatsError:assert(
        rawget(_G, 'crud') ~= nil,
        'Can be enabled only on crud router'
    )

    opts = table.deepcopy(opts) or {}
    if opts.driver == nil then
        opts.driver = stats.get_default_driver()
    end

    StatsError:assert(
        stats.is_driver_supported(opts.driver),
        'Unsupported driver: %s', opts.driver)

    if opts.quantiles == nil then
        opts.quantiles = false
    end

    if opts.quantile_tolerated_error == nil then
        opts.quantile_tolerated_error = stats.DEFAULT_QUANTILE_TOLERATED_ERROR
    end

    if opts.quantile_age_buckets_count == nil then
        opts.quantile_age_buckets_count = stats.DEFAULT_QUANTILE_AGE_BUCKET_COUNT
    end

    if opts.quantile_max_age_time == nil then
        opts.quantile_max_age_time = stats.DEFAULT_QUANTILE_MAX_AGE_TIME
    end

    -- Do not reinit if called with same options.
    if internal.driver == opts.driver
    and internal.quantiles == opts.quantiles
    and internal.quantile_tolerated_error == opts.quantile_tolerated_error
    and internal.quantile_age_buckets_count == opts.quantile_age_buckets_count
    and internal.quantile_max_age_time == opts.quantile_max_age_time then
        return true
    end

    -- Disable old driver registry, if another one was requested.
    stats.disable()

    internal.driver = opts.driver

    internal:get_registry().init{
        quantiles = opts.quantiles,
        quantile_tolerated_error = opts.quantile_tolerated_error,
        quantile_age_buckets_count = opts.quantile_age_buckets_count,
        quantile_max_age_time = opts.quantile_max_age_time,
    }

    internal.quantiles = opts.quantiles
    internal.quantile_tolerated_error = opts.quantile_tolerated_error
    internal.quantile_age_buckets_count = opts.quantile_age_buckets_count
    internal.quantile_max_age_time = opts.quantile_max_age_time

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

    internal:get_registry().destroy()
    internal:get_registry().init{
        quantiles = internal.quantiles,
        quantile_tolerated_error = internal.quantile_tolerated_error,
        quantile_age_buckets_count = internal.quantile_age_buckets_count,
        quantile_max_age_time = internal.quantile_max_age_time,
    }

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

    internal:get_registry().destroy()
    internal.driver = nil
    internal.quantiles = nil
    internal.quantile_tolerated_error = nil
    internal.quantile_age_buckets_count = nil
    internal.quantile_max_age_time = nil

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

    return internal:get_registry().get(space_name)
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

    local registry = internal:get_registry()

    -- If pairs() cycle will be interrupted with break,
    -- we'll never get a proper obervation.
    -- We create an object with the same lifespan as gen()
    -- function so if someone break pairs cycle,
    -- it still will be observed.
    local observed = false

    local gc_observer = setmt__gc({}, {
        __gc = function()
            if observed == false then
                -- Do not call observe directly because metrics
                -- collectors may yield, for example
                -- https://github.com/tarantool/metrics/blob/a23f8d49779205dd45bd211e21a1d34f26010382/metrics/collectors/shared.lua#L85
                -- Calling fiber.yield is prohibited in gc.
                fiber.new(registry.observe, total_latency, space_name, op, 'ok')
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
    dev_checks('string', 'string', 'boolean', 'number', 'boolean')

    local finish_time = clock.monotonic()
    local latency = finish_time - start_time

    local registry = internal:get_registry()

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

    internal:get_registry().observe_fetch(
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

    internal:get_registry().observe_map_reduces(1, space_name)

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
-- @tfield[opt] string driver Current statistics registry driver (if nil, stats disabled).
--
-- @tfield[opt] boolean quantiles Is quantiles computed.
stats.internal = internal

--- Default metrics quantile precision.
stats.DEFAULT_QUANTILE_TOLERATED_ERROR = 1e-3

--- Default metrics quantile bucket count.
stats.DEFAULT_QUANTILE_AGE_BUCKET_COUNT = 2

--- Default metrics quantile bucket lifetime.
stats.DEFAULT_QUANTILE_MAX_AGE_TIME = 60

return stats
