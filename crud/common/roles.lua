local config = require('config')

local function is_sharding_role_enabled(expected_sharding_role)
    -- Works only for versions newer than 3.0.1-10 (3.0.2 and following)
    -- and newer than 3.1.0-entrypoint-77 (3.1.0 and following).
    -- https://github.com/tarantool/tarantool/commit/ebb170cb8cf2b9c4634bcf0178665909f578c335
    local actual_sharding_roles = config:get('sharding.roles')

    for _, actual_sharding_role in ipairs(actual_sharding_roles or {}) do
        if actual_sharding_role == expected_sharding_role then
            return true
        end
    end

    return false
end

return {
    is_sharding_role_enabled = is_sharding_role_enabled,
}
