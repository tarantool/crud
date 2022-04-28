-- Mostly it's a copy-paste from tarantool/tarantool log.lua:
-- https://github.com/tarantool/tarantool/blob/29654ffe3638e5a218dd32f1788830ff05c1c05c/src/lua/log.lua
--
-- We have three reasons for the copy-paste:
-- 1. Tarantool has not log.crit() (a function for logging with CRIT level).
-- 2. Only new versions of Tarantool have Ratelimit type.
-- 3. We want own copy of Ratelimit in case the implementation in Tarantool
-- changes. Less pain between Tarantool versions.
local ffi = require('ffi')

local S_CRIT = ffi.C.S_CRIT
local S_WARN = ffi.C.S_WARN

local function say(level, fmt, ...)
    if ffi.C.log_level < level then
        -- don't waste cycles on debug.getinfo()
        return
    end
    local type_fmt = type(fmt)
    local format = "%s"
    if select('#', ...) ~= 0 then
        local stat
        stat, fmt = pcall(string.format, fmt, ...)
        if not stat then
            error(fmt, 3)
        end
    elseif type_fmt == 'table' then
        -- An implementation in tarantool/tarantool supports encoding a table in
        -- JSON, but it requires more dependencies from FFI. So we just deleted
        -- it because we don't need such encoding in the module.
        error("table not supported", 3)
    elseif type_fmt ~= 'string' then
        fmt = tostring(fmt)
    end

    local debug = require('debug')
    local frame = debug.getinfo(3, "Sl")
    local line, file = 0, 'eval'
    if type(frame) == 'table' then
        line = frame.currentline or 0
        file = frame.short_src or frame.src or 'eval'
    end

    ffi.C._say(level, file, line, nil, format, fmt)
end

local ratelimit_enabled = true

local function ratelimit_enable()
    ratelimit_enabled = true
end

local function ratelimit_disable()
    ratelimit_enabled = false
end

local Ratelimit = {
    interval = 60,
    burst = 10,
    emitted = 0,
    suppressed = 0,
    start = 0,
}

local function ratelimit_new(object)
    return Ratelimit:new(object)
end

function Ratelimit:new(object)
    object = object or {}
    setmetatable(object, self)
    self.__index = self
    return object
end

function Ratelimit:check()
    if not ratelimit_enabled then
        return 0, true
    end

    local clock = require('clock')
    local now = clock.monotonic()
    local saved_suppressed = 0
    if now > self.start + self.interval then
        saved_suppressed = self.suppressed
        self.suppressed = 0
        self.emitted = 0
        self.start = now
    end

    if self.emitted < self.burst then
        self.emitted = self.emitted + 1
        return saved_suppressed, true
    end
    self.suppressed = self.suppressed + 1
    return saved_suppressed, false
end

function Ratelimit:log_check(lvl)
    local suppressed, ok = self:check()
    if lvl >= S_WARN and suppressed > 0 then
        say(S_WARN, '%d messages suppressed due to rate limiting', suppressed)
    end
    return ok
end

function Ratelimit:log(lvl, fmt, ...)
    if self:log_check(lvl) then
        say(lvl, fmt, ...)
    end
end

local function log_ratelimited_closure(lvl)
    return function(self, fmt, ...)
        self:log(lvl, fmt, ...)
    end
end

Ratelimit.log_crit = log_ratelimited_closure(S_CRIT)

return {
    new = ratelimit_new,
    enable = ratelimit_enable,
    disable = ratelimit_disable,
}
