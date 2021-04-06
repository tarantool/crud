local utils = require('crud.common.utils')
local dev_checks = require('crud.common.dev_checks')
local schema = require('crud.common.schema')

local cut_rows = {}

function cut_rows.call(select_result, field_names)
    dev_checks({
        metadata = 'table',
        rows = 'table',
    }, 'table')

    local truncated_metadata, err = utils.truncate_tuple_metadata(select_result.metadata, field_names)

    if err ~= nil then
        return nil, err
    end

    local truncated_rows = {}

    for _, row in ipairs(select_result.rows) do
        local truncated_row = schema.truncate_row_trailing_fields(row, field_names)
        table.insert(truncated_rows, truncated_row)
    end

    return {
        metadata = truncated_metadata,
        rows = truncated_rows,
    }
end

return cut_rows
