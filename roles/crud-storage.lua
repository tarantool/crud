local errors = require('errors')

local crud = require('crud')
local common_role_utils = require('crud.common.roles')
local common_utils = require('crud.common.utils')

local TarantoolRoleConfigurationError = errors.new_class('TarantoolRoleConfigurationError', {capture_stack = false})

local tarantool_version = rawget(_G, '_TARANTOOL')
TarantoolRoleConfigurationError:assert(
    common_utils.tarantool_supports_config_get_inside_roles()
    and common_utils.tarantool_role_privileges_not_revoked(),
    ('Tarantool 3 role is not supported for Tarantool %s, use 3.0.2 or newer'):format(tarantool_version)
)

local function validate()
    TarantoolRoleConfigurationError:assert(
        common_role_utils.is_sharding_role_enabled('storage'),
        'instance must be a sharding storage to enable roles.crud-storage'
    )
end

local function apply()
    crud.init_storage{async = true}
end

local function stop()
    crud.stop_storage()
end

return {
    validate = validate,
    apply = apply,
    stop = stop,
}
