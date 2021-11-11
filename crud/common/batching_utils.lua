local errors = require('errors')
local dev_checks = require('crud.common.dev_checks')
local sharding_utils = require('crud.common.sharding.utils')

local NotPerformedError = errors.new_class('NotPerformedError', {capture_stack = false})

local batching_utils = {}

batching_utils.stop_on_error_msg = "Operation with tuple was not performed"
batching_utils.rollback_on_error_msg = "Operation with tuple was rollback"

function batching_utils.construct_sharding_hash_mismatch_errors(err_msg, tuples)
    dev_checks('string', 'table')

    local errs = {}

    for _, tuple in ipairs(tuples) do
        local err_obj = sharding_utils.ShardingHashMismatchError:new(err_msg)
        err_obj.operation_data = tuple
        table.insert(errs, err_obj)
    end

    return errs
end

function batching_utils.complement_batching_errors(errs, err_msg, tuples)
    dev_checks('table', 'string', 'table')

    for _, tuple in ipairs(tuples) do
        local err_obj = NotPerformedError:new(err_msg)
        err_obj.operation_data = tuple
        table.insert(errs, err_obj)
    end

    return errs
end

return batching_utils
