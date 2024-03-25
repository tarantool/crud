---- Module for preserving data between reloads.
-- @module crud.common.stash
--
local dev_checks = require('crud.common.dev_checks')
local utils = require('crud.common.utils')

local stash = {}

--- Available stashes list.
--
-- @tfield string cfg
--  Stash for CRUD module configuration.
--
-- @tfield string stats_internal
--  Stash for main stats module.
--
-- @tfield string stats_local_registry
--  Stash for local metrics registry.
--
-- @tfield string stats_metrics_registry
--  Stash for metrics rocks statistics registry.
--
-- @tfield string select_module_compat_info
--  Stash for select compatability version registry.
--
-- @tfield string storage_init
--  Stash for storage initializing tools.
--
stash.name = {
    cfg = '__crud_cfg',
    stats_internal = '__crud_stats_internal',
    stats_local_registry = '__crud_stats_local_registry',
    stats_metrics_registry = '__crud_stats_metrics_registry',
    ddl_triggers = '__crud_ddl_spaces_triggers',
    select_module_compat_info = '__select_module_compat_info',
    storage_readview = '__crud_storage_readview',
    storage_init = '__crud_storage_init',
}

--- Setup Tarantool Cartridge reload.
--
--  Call on Tarantool Cartridge roles that are expected
--  to use stashes.
--
-- @function setup_cartridge_reload
--
-- @return Returns
--
function stash.setup_cartridge_reload()
    local hotreload_supported, hotreload = utils.is_cartridge_hotreload_supported()
    if not hotreload_supported then
        return
    end

    for _, name in pairs(stash.name) do
        hotreload.whitelist_globals({ name })
    end
end

--- Get a stash instance, initialize if needed.
--
--  Stashes are persistent to package reload.
--  To use them with Cartridge roles reload,
--  call `stash.setup_cartridge_reload` in role.
--
-- @function get
--
-- @string name
--  Stash identifier. Use one from `stash.name` table.
--
-- @treturn table A stash instance.
--
function stash.get(name)
    dev_checks('string')

    local instance = rawget(_G, name) or {}
    rawset(_G, name, instance)

    return instance
end

return stash
