local crud = require('crud')
local stash = require('crud.common.stash')

-- removes routes that changed in config and adds new routes
local function init()
    crud.init_router()
    stash.setup_cartridge_reload()
end

local function stop()
    crud.stop_router()
end

return {
    role_name = 'crud-router',
    init = init,
    stop = stop,
    implies_router = true,
    dependencies = {'cartridge.roles.vshard-router'},
}
