local crud = require('crud')
local stash = require('crud.common.stash')

local function init()
    crud.init_storage()
    stash.setup_cartridge_reload()
end

local function stop()
    crud.stop_storage()
end

return {
    role_name = 'crud-storage',
    init = init,
    stop = stop,
    implies_storage = true,
    dependencies = {'cartridge.roles.vshard-storage'},
}
