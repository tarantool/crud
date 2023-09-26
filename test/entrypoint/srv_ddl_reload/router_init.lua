return function()
    local customers_module = {
        sharding_func_default = function(key)
            local id = key[1]
            assert(id ~= nil)

            return id % 3000 + 1
        end,
        sharding_func_new = function(key)
            local id = key[1]
            assert(id ~= nil)

            return (id + 42) % 3000 + 1
        end,
    }
    rawset(_G, 'customers_module', customers_module)
end
