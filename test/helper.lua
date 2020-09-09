require('strict').on()

local log = require('log')
local checks = require('checks')

local fio = require('fio')
local ok, cartridge_helpers = pcall(require, 'cartridge.test-helpers')
if not ok then
    log.error('Please, install cartridge rock to run tests')
    os.exit(1)
end

local helpers = table.copy(cartridge_helpers)

helpers.project_root = fio.dirname(debug.sourcedir())

function helpers.entrypoint(name)
    local path = fio.pathjoin(
        helpers.project_root,
        'test', 'entrypoint',
        string.format('%s.lua', name)
    )
    if not fio.path.exists(path) then
        error(path .. ': no such entrypoint', 2)
    end
    return path
end

function helpers.table_keys(t)
    checks('table')
    local keys = {}
    for key in pairs(t) do
        table.insert(keys, key)
    end
    return keys
end

function helpers.get_results_list(results_map)
    checks('table')
    local results_list = {}
    for _, res in pairs(results_map) do
        table.insert(results_list, res)
    end
    return results_list
end

return helpers
