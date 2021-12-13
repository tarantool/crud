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

    return cfg
end

local cfg = set_defaults_if_empty(stash.get(stash.name.cfg))

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
-- @return Configuration table.
--
local function __call(self, opts)
    checks('table', { stats = '?boolean' })

    opts = opts or {}

    if opts.stats ~= nil then
        if opts.stats == true then
            stats.enable()
        else
            stats.disable()
        end

        rawset(cfg, 'stats', opts.stats)
    end

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
