local fun = require('fun')
local json = require('json')

local utils = {}

local function default_cfg()
    return {
        work_dir = os.getenv('TARANTOOL_WORKDIR'),
        listen = os.getenv('TARANTOOL_LISTEN'),
    }
end

local function env_cfg()
    local src = os.getenv('TARANTOOL_BOX_CFG')
    if src == nil then
        return {}
    end
    local res = json.decode(src)
    assert(type(res) == 'table')
    return res
end

function utils.box_cfg(cfg)
    return fun.chain(default_cfg(), env_cfg(), cfg or {}):tomap()
end

return utils
