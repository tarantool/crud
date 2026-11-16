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

        rawset(_G, 'bucket_ref_rw', function(bucket_id)
            local bucket_ref_unref = require('crud.common.sharding.bucket_ref_unref')

            local ref_ok, err, unref = bucket_ref_unref.bucket_refrw(bucket_id)
            if not ref_ok then
                return nil, err
            end

            local res, err = unref(bucket_id)
            return res, err
        end)

        if type(box.cfg) ~= 'table' then
            -- Cartridge, unit tests.
            return
        else
            helper.wrap_schema_init(function()
                add_storage_execute('clear_vshard_calls')
                add_storage_execute('say_hi_sleepily')
                add_storage_execute('say_hi_politely')
                add_storage_execute('bucket_ref_rw')
                add_storage_execute('patch_vshard_calls')
                add_storage_execute('unpatch_vshard_calls')
                add_storage_execute('non_existent_func')
            end)()
        end
    end,
    wait_until_ready = helper.wait_schema_init,
}
