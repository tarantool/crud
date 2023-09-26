return function()
    local some_module = {
        sharding_func =
        function(key)
            if key ~= nil and key[1] ~= nil then
                return key[1] % 10
            end
        end
    }
    rawset(_G, 'some_module', some_module)
end
