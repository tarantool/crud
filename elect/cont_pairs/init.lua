local errors = require('errors')

local ContPairsError = errors.new_class('ContPairsError', {capture_stack = false})

local TARANTOOL_VERSION_ERR_MSG = 'Tarantool version should be >= 1.10 < 2 or >= 2.3, found %s'

local cont_pairs = {}

local _TARANTOOL = _G._TARANTOOL

function cont_pairs.init()
    local version_parts = _TARANTOOL:split('.', 2)

    local major = tonumber(version_parts[1])
    local minor = tonumber(version_parts[2])

    -- XXX: both versions of cont_pairs should be tested on different Tarantool versions
    -- to make version checks better

    if major == 1 then
        if minor < 10 then
            return false, ContPairsError:new(TARANTOOL_VERSION_ERR_MSG, _TARANTOOL)
        end

        require('elect.cont_pairs.1x')
    elseif major == 2 then
        if minor < 3 then
            return false, ContPairsError:new(TARANTOOL_VERSION_ERR_MSG, _TARANTOOL)
        end

        require('elect.cont_pairs.2x')
    else
        return false, ContPairsError:new("Unknown Tarantool version: %s", _TARANTOOL)
    end

    return true
end

return cont_pairs
