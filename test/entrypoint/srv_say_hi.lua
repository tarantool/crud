#!/usr/bin/env tarantool

require('strict').on()
_G.is_initialized = function() return false end

local log = require('log')
local errors = require('errors')
local cartridge = require('cartridge')
local membership = require('membership')
local fiber = require('fiber')

local ok, err

ok, err = errors.pcall('CartridgeCfgError', cartridge.cfg, {
    advertise_uri = 'localhost:3301',
    http_port = 8081,
    bucket_count = 3000,
    roles = {
        'cartridge.roles.crud-storage',
        'cartridge.roles.crud-router',
    },
})

if not ok then
    log.error('%s', err)
    os.exit(1)
end


rawset(_G, 'say_hi_politely', function (to_name)
   to_name = to_name or "handsome"
   local my_alias = membership.myself().payload.alias
   return string.format("HI, %s! I am %s", to_name, my_alias)
end)

rawset(_G, 'say_hi_sleepily', function (time_to_sleep)
   if time_to_sleep ~= nil then
      fiber.sleep(time_to_sleep)
   end

   return "HI"
end)

rawset(_G, 'vshard_calls', {})

rawset(_G, 'clear_vshard_calls', function()
    table.clear(_G.vshard_calls)
end)

rawset(_G, 'patch_vshard_calls', function(vshard_call_names)
    local vshard = require('vshard')

    local replicasets = vshard.router.routeall()

    local _, replicaset = next(replicasets)
    local replicaset_mt = getmetatable(replicaset)

    for _, vshard_call_name in ipairs(vshard_call_names) do
        local old_func = replicaset_mt.__index[vshard_call_name]
        assert(old_func ~= nil, vshard_call_name)

        replicaset_mt.__index[vshard_call_name] = function(...)
            local func_name = select(2, ...)
            if not string.startswith(func_name, 'vshard.') or func_name == 'vshard.storage.call' then
                table.insert(_G.vshard_calls, vshard_call_name)
            end
            return old_func(...)
        end
    end
end)

_G.is_initialized = cartridge.is_healthy
