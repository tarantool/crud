-- Exports a function via IPROTO.
--
-- Uses box.iproto.export if available because adding a function to _G has
-- no effect with application threads.
local export = box.iproto and box.iproto.export or
               function(name, func) rawset(_G, name, func) end

return {
    init = function()
        rawset(_G, 'vshard_calls', {})

        export('clear_vshard_calls', function()
            table.clear(_G.vshard_calls)
        end)

        export('patch_vshard_calls', function(vshard_call_names)
            local vshard = require('vshard')

            local replicasets = vshard.router.routeall()

            local _, replicaset = next(replicasets)
            local replicaset_mt = getmetatable(replicaset)

            if rawget(_G, 'vshard_call_originals') == nil then
                rawset(_G, 'vshard_call_originals', {})
            end
            local originals = rawget(_G, 'vshard_call_originals')

            for _, vshard_call_name in ipairs(vshard_call_names) do
                if originals[vshard_call_name] == nil then
                    local old_func = replicaset_mt.__index[vshard_call_name]
                    assert(old_func ~= nil, vshard_call_name)

                    originals[vshard_call_name] = old_func

                    replicaset_mt.__index[vshard_call_name] = function(...)
                        local func_name = select(2, ...)
                        if func_name == nil
                            or not string.startswith(func_name, 'vshard.')
                            or func_name == 'vshard.storage.call' then
                            table.insert(_G.vshard_calls, vshard_call_name)
                        end
                        return old_func(...)
                    end
                end
            end
        end)

        export('unpatch_vshard_calls', function(vshard_call_names)
            local vshard = require('vshard')

            local replicasets = vshard.router.routeall()

            local _, replicaset = next(replicasets)
            local replicaset_mt = getmetatable(replicaset)

            local originals = rawget(_G, 'vshard_call_originals')
            if originals == nil then
                return
            end

            for _, vshard_call_name in ipairs(vshard_call_names) do
                local old_func = originals[vshard_call_name]
                if old_func ~= nil then
                    replicaset_mt.__index[vshard_call_name] = old_func
                    originals[vshard_call_name] = nil
                end
            end
        end)
    end,
}
