local crud = require('crud')

local function init()
    crud.init_storage()
end

return {
    role_name = 'crud-storage',
    init = init,
    dependencies = {'cartridge.roles.vshard-storage'}
}
