---- Module for CRUD configuration.
-- @module crud.cfg
--

local checks = require('checks')
local errors = require('errors')

local stash = require('crud.common.stash')
local stats = require('crud.stats')

local CfgError = errors.new_class('CfgError', {capture_stack = false})

local cfg_module = {}

local function set_defaults_if_empty(cfg)
    if cfg.stats == nil then
        cfg.stats = false
    end

    if cfg.stats_driver == nil then
        cfg.stats_driver = stats.get_default_driver()
    end

    if cfg.stats_quantiles == nil then
        cfg.stats_quantiles = false
    end

    if cfg.stats_quantile_tolerated_error == nil then
        cfg.stats_quantile_tolerated_error = stats.DEFAULT_QUANTILE_TOLERATED_ERROR
    end

    if cfg.stats_quantile_age_buckets_count == nil then
        cfg.stats_quantile_age_buckets_count = stats.DEFAULT_QUANTILE_AGE_BUCKET_COUNT
    end

    if cfg.stats_quantile_max_age_time == nil then
        cfg.stats_quantile_max_age_time = stats.DEFAULT_QUANTILE_MAX_AGE_TIME
    end

    return cfg
end

local cfg = set_defaults_if_empty(stash.get(stash.name.cfg))

local function configure_stats(cfg, opts)
    if  (opts.stats == nil)
    and (opts.stats_driver == nil)
    and (opts.stats_quantiles == nil)
    and (opts.stats_quantile_tolerated_error == nil)
    and (opts.stats_quantile_age_buckets_count == nil)
    and (opts.stats_quantile_max_age_time == nil) then
        return
    end

    if opts.stats == nil then
        opts.stats = cfg.stats
    end

    if opts.stats_driver == nil then
        opts.stats_driver = cfg.stats_driver
    end

    if opts.stats_quantiles == nil then
        opts.stats_quantiles = cfg.stats_quantiles
    end

    if opts.stats_quantile_tolerated_error == nil then
        opts.stats_quantile_tolerated_error = cfg.stats_quantile_tolerated_error
    end

    if opts.stats_quantile_age_buckets_count == nil then
        opts.stats_quantile_age_buckets_count = cfg.stats_quantile_age_buckets_count
    end

    if opts.stats_quantile_max_age_time == nil then
        opts.stats_quantile_max_age_time = cfg.stats_quantile_max_age_time
    end

    if opts.stats == true then
        stats.enable{
            driver = opts.stats_driver,
            quantiles = opts.stats_quantiles,
            quantile_tolerated_error = opts.stats_quantile_tolerated_error,
            quantile_age_buckets_count = opts.stats_quantile_age_buckets_count,
            quantile_max_age_time = opts.stats_quantile_max_age_time,
        }
    else
        stats.disable()
    end

    rawset(cfg, 'stats', opts.stats)
    rawset(cfg, 'stats_driver', opts.stats_driver)
    rawset(cfg, 'stats_quantiles', opts.stats_quantiles)
    rawset(cfg, 'stats_quantile_tolerated_error', opts.stats_quantile_tolerated_error)
    rawset(cfg, 'stats_quantile_age_buckets_count', opts.stats_quantile_age_buckets_count)
    rawset(cfg, 'stats_quantile_max_age_time', opts.stats_quantile_max_age_time)
end

--- Configure CRUD module.
--
-- @function __call
--
-- @tab self
--
-- @tab[opt] opts
--
-- @bool[opt] opts.stats
--  Enable or disable statistics collect.
--  Statistics are observed only on router instances.
--
-- @string[opt] opts.stats_driver
--  `'local'` or `'metrics'`.
--  If `'local'`, stores statistics in local registry (some Lua tables)
--  and computes latency as overall average. `'metrics'` requires
--  `metrics >= 0.10.0` installed and stores statistics in
--  global metrics registry (integrated with exporters).
--  `'metrics'` driver supports computing latency as 0.99 quantile with aging.
--  If `'metrics'` driver is available, it is used by default,
--  otherwise `'local'` is used.
--
-- @bool[opt] opts.stats_quantiles
--  Enable or disable statistics quantiles (only for metrics driver).
--  Quantiles computations increases performance overhead up to 10%.
--
-- @number[opt=1e-3] opts.stats_quantile_tolerated_error
--  See tarantool/metrics summary API for details:
--  https://www.tarantool.io/ru/doc/latest/book/monitoring/api_reference/#summary
--  If quantile value is -Inf, try to decrease quantile tolerated error.
--  See https://github.com/tarantool/metrics/issues/189 for issue details.
--  Decreasing the value increases computational load.
--
-- @number[opt=2] opts.stats_quantile_age_buckets_count
--  Count of summary quantile buckets.
--  See tarantool/metrics summary API for details:
--  https://www.tarantool.io/ru/doc/latest/book/monitoring/api_reference/#summary
--  Increasing the value smoothes time window move,
--  but consumes additional memory and CPU.
--
-- @number[opt=60] opts.stats_quantile_max_age_time
--  Duration of each bucket’s lifetime in seconds.
--  See tarantool/metrics summary API for details:
--  https://www.tarantool.io/ru/doc/latest/book/monitoring/api_reference/#summary
--  Smaller bucket lifetime results in smaller time window for quantiles,
--  but more CPU is spent on bucket rotation. If your application has low request
--  frequency, increase the value to reduce the amount of `-nan` gaps in quantile values.
--
-- @return Configuration table.
--
local function __call(self, opts)
    checks('table', {
        stats = '?boolean',
        stats_driver = '?string',
        stats_quantiles = '?boolean',
        stats_quantile_tolerated_error = '?number',
        stats_quantile_age_buckets_count = '?number',
        stats_quantile_max_age_time = '?number',
    })
    -- Value validation would be performed in stats checks, if required.

    opts = table.deepcopy(opts) or {}
    -- opts from Cartridge clusterwide configuration is read-only,
    -- but we want to work with copy anyway.
    setmetatable(opts, {})

    configure_stats(cfg, opts)

    return self
end

local function __newindex()
    CfgError:assert(false, 'Use crud.cfg{} instead')
end

-- Iterating through `crud.cfg` with pairs is not supported
-- yet, refer to tarantool/crud#265.
cfg_module.cfg = setmetatable({}, {
    __index = cfg,
    __newindex = __newindex,
    __call = __call,
    __serialize = function() return cfg end
})

return cfg_module
