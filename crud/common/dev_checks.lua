local checks = require('checks')

local dev_checks = function() end

if os.getenv('TARANTOOL_CRUD_ENABLE_INTERNAL_CHECKS') == 'ON' then
    dev_checks = checks
end

return dev_checks
