local errors = require('errors')

local crud = require('crud')
local stash = require('crud.common.stash')
local stats = require('crud.stats')

local RoleConfigurationError = errors.new_class('RoleConfigurationError', {capture_stack = false})

local function init()
    crud.init_router()
    stash.setup_cartridge_reload()
end

local function stop()
    crud.stop_router()
end

local cfg_types = {
    stats = 'boolean',
    stats_driver = 'string',
    stats_quantiles = 'boolean',
    stats_quantile_tolerated_error = 'number',
    stats_quantile_age_buckets_count = 'number',
    stats_quantile_max_age_time = 'number',
}

local cfg_values = {
    stats_driver = function(value)
        RoleConfigurationError:assert(
            stats.is_driver_supported(value),
            'Invalid crud configuration field "stats_driver" value: %q is not supported',
            value
        )
    end,
}

local function validate_config(conf_new, _)
    local crud_cfg = conf_new['crud']

    if crud_cfg == nil then
        return true
    end

    RoleConfigurationError:assert(
        type(crud_cfg) == 'table',
        'Configuration "crud" section must be a table'
    )

    RoleConfigurationError:assert(
        crud_cfg.crud == nil,
        '"crud" section is already presented as a name of "crud.yml", ' ..
        'do not use it as a top-level section name'
    )

    for name, value in pairs(crud_cfg) do
        RoleConfigurationError:assert(
            cfg_types[name] ~= nil,
            'Unknown crud configuration field %q', name
        )

        RoleConfigurationError:assert(
            type(value) == cfg_types[name],
            'Invalid crud configuration field %q type: expected %s, got %s',
            name, cfg_types[name], type(value)
        )

        if cfg_values[name] ~= nil then
            cfg_values[name](value)
        end
    end

    return true
end

local function apply_config(conf)
    crud.cfg(conf['crud'])
end

return {
    role_name = 'crud-router',
    init = init,
    stop = stop,
    validate_config = validate_config,
    apply_config = apply_config,
    implies_router = true,
    dependencies = {'cartridge.roles.vshard-router'},
}
