local errors = require('errors')

local crud = require('crud')
local common_role_utils = require('crud.common.roles')
local common_utils = require('crud.common.utils')
local stats = require('crud.stats')

local TarantoolRoleConfigurationError = errors.new_class('TarantoolRoleConfigurationError')

local tarantool_version = rawget(_G, '_TARANTOOL')
TarantoolRoleConfigurationError:assert(
    common_utils.tarantool_supports_config_get_inside_roles(),
    ('Tarantool 3 role is not supported for Tarantool %s, use 3.0.2 or newer'):format(tarantool_version)
)


local function validate_enabled_on_sharding_router()
    TarantoolRoleConfigurationError:assert(
        common_role_utils.is_sharding_role_enabled('router'),
        'Instance must be a sharding router to enable roles.crud-router'
    )
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
        TarantoolRoleConfigurationError:assert(
            stats.is_driver_supported(value),
            'Invalid "stats_driver" field value: %q is not supported',
            value
        )
    end,
}

local function validate_roles_cfg(roles_cfg)
    if roles_cfg == nil then
        return
    end

    TarantoolRoleConfigurationError:assert(
        type(roles_cfg) == 'table',
        'roles_cfg must be a table'
    )

    for name, value in pairs(roles_cfg) do
        TarantoolRoleConfigurationError:assert(
            cfg_types[name] ~= nil,
            'Unknown field %q', name
        )

        TarantoolRoleConfigurationError:assert(
            type(value) == cfg_types[name],
            'Invalid %q field type: expected %s, got %s',
            name, cfg_types[name], type(value)
        )

        if cfg_values[name] ~= nil then
            cfg_values[name](value)
        end
    end
end

local function validate(roles_cfg)
    validate_enabled_on_sharding_router()

    validate_roles_cfg(roles_cfg)
end

local function apply(roles_cfg)
    crud.init_router()

    crud.cfg(roles_cfg)
end

local function stop()
    crud.stop_router()
end

return {
    validate = validate,
    apply = apply,
    stop = stop,
}
