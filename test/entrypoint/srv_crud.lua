#!/usr/bin/env tarantool

require('strict').on()
_G.is_initialized = function() return false end

local log = require('log')
local errors = require('errors')
local cartridge = require('cartridge')
local membership = require('membership')
local fiber = require('fiber')
local elect = require('elect')

package.preload['customers-storage'] = function()
    return {
        role_name = 'customers-storage',
        init = function()
            local customers_space = box.schema.space.create('customers', {
                format = {
                    {'id', 'unsigned'},
                    {'bucket_id', 'unsigned'},
                    {'name', 'string'},
                    {'age', 'number'},
                },
                if_not_exists = true,
            })
            customers_space:create_index('id', {
                parts = {'id'},
                if_not_exists = true,
            })
            customers_space:create_index('bucket_id', {
                parts = {'bucket_id'},
                unique = false,
                if_not_exists = true,
            })
        end,
        dependencies = {'cartridge.roles.vshard-storage'},
    }
end

local ok, err = errors.pcall('CartridgeCfgError', cartridge.cfg, {
    advertise_uri = 'localhost:3301',
    http_port = 8081,
    bucket_count = 3000,
    roles = {
        'cartridge.roles.vshard-storage',
        'cartridge.roles.vshard-router',
        'customers-storage',
    },
})

if not ok then
    log.error('%s', err)
    os.exit(1)
end

-- initialize elect
elect.init()

local ok, err = elect.register({
    say_hi_politely = function(to_name)
        to_name = to_name or "handsome"
        local my_alias = membership.myself().payload.alias
        return string.format("HI, %s! I am %s", to_name, my_alias)
    end,

    say_hi_sleepily = function(time_to_sleep)
        if time_to_sleep ~= nil then
            fiber.sleep(time_to_sleep)
        end

        return "HI"
    end,
})

if not ok then
    log.error('%s', err)
    os.exit(1)
end


_G.is_initialized = cartridge.is_healthy
