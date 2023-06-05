local dev_checks = require('crud.common.dev_checks')
local utils = require('crud.common.utils')
local sharding_utils = require('crud.common.sharding.utils')

local BasePostprocessor = require('crud.common.map_call_cases.base_postprocessor')

local BatchPostprocessor = {}
-- inheritance from BasePostprocessor
setmetatable(BatchPostprocessor, {__index = BasePostprocessor})

--- Collect data after call
--
-- @function collect
--
-- @tparam[opt] table result_info
-- Data of function call result
-- @tparam[opt] result_info.key
-- Key for collecting result
-- @tparam[opt] result_info.value
-- Value for collecting result by result_info.key
--
-- @tparam[opt] table err_info
-- Data of function call error
-- @tparam[opt] function|table err_info.err_wrapper
-- Wrapper for error formatting
-- @tparam[opt] table|cdata err_info.err
-- Err of function call
-- @tparam[opt] table err_info.wrapper_args
-- Additional args for error wrapper
--
-- @return[1] boolean early_exit
function BatchPostprocessor:collect(result_info, err_info)
    dev_checks('table', {
        key = '?',
        value = '?',
    },{
        err_wrapper = 'function|table',
        err = '?table|cdata',
        wrapper_args = '?table',
    })

    if result_info.value ~= nil then
        self.storage_info[result_info.key] = {replica_schema_version = result_info.value[3]}
    end

    local errs = {err_info.err}
    if err_info.err == nil then
        errs = result_info.value[2]
    end

    if errs ~= nil then
        for _, err in pairs(errs) do
            local err_to_wrap = err
            if err.class_name ~= sharding_utils.ShardingHashMismatchError.name and err.err then
                err_to_wrap = err.err
            end

            local err_obj = err_info.err_wrapper(self.vshard_router, err_to_wrap, unpack(err_info.wrapper_args))
            err_obj.operation_data = err.operation_data
            err_obj.space_schema_hash = err.space_schema_hash

            self.errs = self.errs or {}
            table.insert(self.errs, err_obj)
        end
    end

    if result_info.value ~= nil and result_info.value[1] ~= nil then
        self.results = utils.list_extend(self.results, result_info.value[1])
    end

    return self.early_exit
end

return BatchPostprocessor
