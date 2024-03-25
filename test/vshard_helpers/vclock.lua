local fiber = require('fiber')
local log = require('log')
local yaml = require('yaml')

-- Simple implementation without metatables.
local function extend_with(t, new_fields)
    for k, v in pairs(new_fields) do
        t[k] = v
    end

    return t
end

local function get_vclock(self)
    return self:exec(function() return box.info.vclock end)
end

local function wait_vclock(self, to_vclock)
    while true do
        local vclock = self:get_vclock()
        local ok = true

        for server_id, to_lsn in pairs(to_vclock) do
            local lsn = vclock[server_id]
            if lsn == nil or lsn < to_lsn then
                ok = false
                break
            end
        end

        if ok then
            return
        end

        log.info("wait vclock: %s to %s",
            yaml.encode(vclock), yaml.encode(to_vclock))
        fiber.sleep(0.001)
    end
end

local function wait_vclock_of(self, other_server)
    local vclock = other_server:get_vclock()
    -- First component is for local changes.
    vclock[0] = nil
    return self:wait_vclock(vclock)
end

local function extend_with_vclock_methods(server)
    return extend_with(server, {
        get_vclock = get_vclock,
        wait_vclock = wait_vclock,
        wait_vclock_of = wait_vclock_of,
    })
end

return {
    extend_with_vclock_methods = extend_with_vclock_methods,
}
