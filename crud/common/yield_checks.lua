--- module used to check that fiber has no yields
--- during execution in tests
local fiber = require("fiber")

local registry = {}

local yield_checks = {
	check_no_yields = function() end,
	guard = function(f, ...)
		return f(...)
	end,
}

local function register_fiber()
	local info = fiber.self():info()
	assert(registry[info.fid] == nil, "fiber already registered, check register_fiber calls")
	registry[info.fid] = info
end

local function check_no_yields()
	local info_curr = fiber.self():info()
	local info_prev = registry[info_curr.fid]
	assert(info_prev ~= nil, "fiber is not registered")
	assert(info_curr.csw == info_prev.csw, "yield happened during fiber execution")
end

local function unregister_fiber()
	local fid = fiber.self():id()
	assert(registry[fid] ~= nil, "fiber is not registered")
	registry[fid] = nil
end

local function finish(ok, res_err, ...)
	unregister_fiber()
	if not ok then
		error(res_err)
	end
	return res_err, ...
end

local function guard(f, ...)
	register_fiber()
	return finish(pcall(f, ...))
end

if os.getenv("TARANTOOL_CRUD_ENABLE_INTERNAL_CHECKS") == "ON" then
	yield_checks.check_no_yields = check_no_yields
	yield_checks.guard = guard
end

return yield_checks
