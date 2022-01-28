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

    return cfg
end

local cfg = set_defaults_if_empty(stash.get(stash.name.cfg))

local function configure_stats(cfg, opts)
    if  (opts.stats == nil)
    and (opts.stats_driver == nil)
    and (opts.stats_quantiles == nil) then
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

    if opts.stats == true then
        stats.enable{ driver = opts.stats_driver, quantiles = opts.stats_quantiles }
    else
        stats.disable()
    end

    rawset(cfg, 'stats', opts.stats)
    rawset(cfg, 'stats_driver', opts.stats_driver)
    rawset(cfg, 'stats_quantiles', opts.stats_quantiles)
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
-- @return Configuration table.
--
local function __call(self, opts)
    checks('table', {
        stats = '?boolean',
        stats_driver = '?string',
        stats_quantiles = '?boolean'
    })

    opts = table.deepcopy(opts) or {}

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
