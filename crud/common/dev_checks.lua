local checks = require('checks')

local dev_checks = function() end

if os.getenv('DEV') == 'ON' then
    dev_checks = checks
end

return dev_checks
