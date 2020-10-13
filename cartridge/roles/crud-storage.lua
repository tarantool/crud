local crud = require('crud')

-- removes routes that changed in config and adds new routes
local function init()
    crud.init_storage()
end

return {
    role_name = 'crud-storage',
    init = init,
    dependencies = {'cartridge.roles.vshard-storage'}
}
