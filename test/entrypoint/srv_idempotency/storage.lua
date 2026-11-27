local ddl = require('ddl')
local helper = require('test.helper')

return {
    init = function()
        local engine = os.getenv('ENGINE') or 'memtx'
        local customers = {
            engine = engine,
            is_local = false,
            temporary = false,
            sharding_key = {'id'},
            format = {
                {name = 'id', is_nullable = false, type = 'unsigned'},
                {name = 'bucket_id', is_nullable = false, type = 'unsigned'},
                {name = 'balance', is_nullable = false, type = 'number'},
            },
            indexes = {
                {name = 'id', type = 'TREE', unique = true,
                 parts = {{path = 'id', is_nullable = false, type = 'unsigned'}}},
                {name = 'bucket_id', type = 'TREE', unique = false,
                 parts = {{path = 'bucket_id', is_nullable = false, type = 'unsigned'}}},
            },
        }
        helper.wrap_schema_init(function()
            local ok, err = ddl.set_schema({spaces = {customers = customers}})
            if not ok then
                error(err)
            end
        end)()

        -- A stored procedure demonstrating the crud.idempotency.mark usage
        -- (ADR 4.5): the marker and the DML are committed by one transaction.
        -- On an idempotency rejection the transaction is rolled back and the
        -- original error object is returned as data, so a client can inspect
        -- its fields (class_name, key, seq, applied_max, mode).
        box.schema.func.create('process_payment', {
            if_not_exists = true,
            body = [[ function(id, amount, idem)
                local crud = require('crud')
                local balance
                local ok, err = pcall(function()
                    box.atomic(function()
                        local account = box.space.customers:get(id)
                        if account == nil then
                            error('account not found', 0)
                        end
                        -- Routing fields belong to the procedure, the rest of
                        -- the fields are forwarded for crud.idempotency to
                        -- validate.
                        local mark_opts = {}
                        if type(idem) == 'table' then
                            for k, v in pairs(idem) do
                                mark_opts[k] = v
                            end
                        end
                        mark_opts.bucket_id = account.bucket_id
                        mark_opts.engine = box.space.customers.engine
                        local _, mark_err = crud.idempotency.mark(mark_opts)
                        if mark_err ~= nil then
                            error(mark_err, 0)
                        end
                        balance = box.space.customers:update(
                            id, {{'+', 'balance', amount}}).balance
                    end)
                end)
                if not ok then
                    return err
                end
                return balance
            end ]],
        })
    end,
    wait_until_ready = helper.wait_schema_init,
}
