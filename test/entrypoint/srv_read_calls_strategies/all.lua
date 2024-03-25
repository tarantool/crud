return {
    init = function()
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
    end,
}
