local ratelimit = require('crud.ratelimit')
local rebalance = require('crud.common.rebalance')

local WARN_INTERVAL_SECONDS = 60
local WARN_BURST_COUNT = 1

local compat_rl = ratelimit.new({
    interval = WARN_INTERVAL_SECONDS,
    burst = WARN_BURST_COUNT,
})

--- Logs a warning when bucket_id is missing (old router compatibility mode).
local function log_nil_bucket_id(operation, space_name, engine)
    local msg = string.format(
        "crud.%s_on_storage called without bucket_id. " ..
        "Old router compatibility mode is active: bucket_ref is skipped, " ..
        "rebalance safety is reduced. Please upgrade routers to restore full guarantees. " ..
        "(space=%q engine=%q)",
        operation, space_name, engine
    )

    compat_rl:log_warn(msg)

    rebalance.inc_nil_bucket_id_compat(operation, engine)
end

return {
    log_nil_bucket_id = log_nil_bucket_id,
}
