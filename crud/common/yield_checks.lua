--- module used to check that fiber has no yields
--- during execution in tests
local fiber = require('fiber')

local registry = {}

local function fast_finish(...)
    return ...
end

local function fast_start()
    return fast_finish
end

local M = {
    register_fiber = function() end,
    check_no_yields = function() end,
    unregister_fiber = function() end,
    start = fast_start,
}

local function register_fiber()
    local info = fiber.self():info()
    assert(registry[info.fid] == nil, "fiber already registered, check register_fiber calls")
    registry[info.fid] = info
end

local function check_no_yields()
    local info_curr = fiber.self():info()
    local info_prev = registry[info_curr.fid]
    assert(info_curr.csw == info_prev.csw, "yield happened during fiber execution")
end

local function unregister_fiber()
    local fid = fiber.self():id()
    assert(registry[fid] ~= nil, "fiber is not registered")
    registry[fid] = nil
end

local function finish(...)
    unregister_fiber()
    return ...
end

local function start()
    register_fiber()
    return finish
end

if os.getenv('TARANTOOL_CRUD_ENABLE_INTERNAL_CHECKS') == 'ON' then
    M.check_no_yields = check_no_yields
    M.start = start
end

return M
