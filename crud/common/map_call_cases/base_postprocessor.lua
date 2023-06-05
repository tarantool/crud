local dev_checks = require('crud.common.dev_checks')

local BasePostprocessor = {}

--- Create new base postprocessor for map call
--
-- @function new
--
-- @return[1] table postprocessor
function BasePostprocessor:new(vshard_router)
    local postprocessor = {
        results = {},
        early_exit = false,
        errs = nil,
        vshard_router = vshard_router,
        storage_info = {},
    }

    setmetatable(postprocessor, self)
    self.__index = self

    return postprocessor
end

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
function BasePostprocessor:collect(result_info, err_info)
    dev_checks('table', {
        key = '?',
        value = '?',
    },{
        err_wrapper = 'function|table',
        err = '?table|cdata',
        wrapper_args = '?table',
    })

    if result_info.value ~= nil and type(result_info.value[1]) == 'table' then
        if result_info.value[1].storage_info ~= nil then
            self.storage_info[result_info.key] = {
                replica_schema_version = result_info.value[1].storage_info.replica_schema_version
            }
        end
    end

    local err = err_info.err
    if err == nil and result_info.value[1] == nil then
        err = result_info.value[2]
    end

    if err ~= nil then
        self.results = nil
        self.errs = err_info.err_wrapper(self.vshard_router, err, unpack(err_info.wrapper_args))
        self.early_exit = true

        return self.early_exit
    end

    if self.early_exit ~= true then
        self.results[result_info.key] = result_info.value
    end

    return self.early_exit
end

--- Get collected data
--
-- @function get
--
-- @return[1] table results
-- @return[2] table errs
-- @return[3] table storage_info
function BasePostprocessor:get()
    return self.results, self.errs, self.storage_info
end

return BasePostprocessor
