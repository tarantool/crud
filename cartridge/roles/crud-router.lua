local crud = require('crud')

-- removes routes that changed in config and adds new routes
local function init()
    crud.init_router()
end

local function stop()
    crud.stop_router()
end

return {
    role_name = 'crud-router',
    init = init,
    stop = stop,
    dependencies = {'cartridge.roles.vshard-router'}
}
