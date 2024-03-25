local fiber = require('fiber')
local helper = require('test.helper')

-- Adds execution rights to a function for a vshard storage user.
local function add_storage_execute(func_name)
    if box.schema.user.exists('storage') then
        box.schema.func.create(func_name, {setuid = true, if_not_exists = true})
        box.schema.user.grant('storage', 'execute', 'function', func_name, {if_not_exists = true})
    end
end

return {
    init = function()
        rawset(_G, 'say_hi_politely', function (to_name)
           to_name = to_name or "handsome"
           local my_alias = box.info.id
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

        if type(box.cfg) ~= 'table' then
            -- Cartridge, unit tests.
            return
        else
            helper.wrap_schema_init(function()
                add_storage_execute('clear_vshard_calls')
                add_storage_execute('say_hi_sleepily')
                add_storage_execute('say_hi_politely')
                add_storage_execute('patch_vshard_calls')
                add_storage_execute('non_existent_func')
            end)()
        end
    end,
    wait_until_ready = helper.wait_schema_init,
}
