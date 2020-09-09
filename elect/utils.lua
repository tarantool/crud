local checks = require('checks')

local utils = {}

function utils.table_count(table)
    checks("table")

    local cnt = 0
    for _, _ in pairs(table) do
        cnt = cnt + 1
    end

    return cnt
end

function utils.format_replicaset_error(replicaset_uuid, msg, ...)
    checks("string", "string")

    return string.format(
        "Failed for %s: %s",
        replicaset_uuid,
        string.format(msg, ...)
    )
end

return utils
