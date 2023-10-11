local ratelimit = require('crud.ratelimit')
local utils = require('crud.common.utils')
local check_select_safety_rl = ratelimit.new()

local common = {}

common.SELECT_FUNC_NAME = utils.get_storage_call('select_on_storage')
common.READVIEW_SELECT_FUNC_NAME = utils.get_storage_call('select_readview_on_storage')
common.DEFAULT_BATCH_SIZE = 100

common.check_select_safety = function(space_name, plan, opts)
    if opts.fullscan == true then
        return
    end

    if opts.first ~= nil and math.abs(opts.first) <= 1000 then
        return
    end

    local iter = plan.tarantool_iter
    if iter == box.index.EQ or iter == box.index.REQ then
        return
    end

    local rl = check_select_safety_rl
    local traceback = debug.traceback()
    rl:log_crit("Potentially long select from space '%s'\n %s", space_name, traceback)
end

return common
