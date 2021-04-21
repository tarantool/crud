local crud = require('crud')

local function init()
    crud.init_storage()
end

local function stop()
    crud.stop_storage()
end

return {
    role_name = 'crud-storage',
    init = init,
    stop = stop,
    dependencies = {'cartridge.roles.vshard-storage'}
}
