local digest = require('digest')
local errors = require('errors')
local msgpack = require('msgpack')

local utils = {}

utils.SPACE_NAME_FIELDNO = 1
utils.SPACE_SHARDING_KEY_FIELDNO = 2
utils.SPACE_SHARDING_FUNC_NAME_FIELDNO = 2
utils.SPACE_SHARDING_FUNC_BODY_FIELDNO = 3

utils.ShardingHashMismatchError = errors.new_class("ShardingHashMismatchError", {capture_stack = false})

function utils.extract_sharding_func_def(tuple)
    if not tuple then
        return nil
    end

    if tuple[utils.SPACE_SHARDING_FUNC_BODY_FIELDNO] ~= nil then
        return {body = tuple[utils.SPACE_SHARDING_FUNC_BODY_FIELDNO]}
    end

    if tuple[utils.SPACE_SHARDING_FUNC_NAME_FIELDNO] ~= nil then
        return tuple[utils.SPACE_SHARDING_FUNC_NAME_FIELDNO]
    end

    return nil
end

function utils.compute_hash(val)
    return digest.murmur(msgpack.encode(val))
end

function utils.combine_errors(ErrorClass, errs)
    if #errs == 0 then
        return nil
    end
    if #errs == 1 then
        return errs[1]
    end

    local messages = {}
    for _, err in ipairs(errs) do
        table.insert(messages, err.err)
    end
    return ErrorClass:new('%s', table.concat(messages, '; '))
end

return utils
