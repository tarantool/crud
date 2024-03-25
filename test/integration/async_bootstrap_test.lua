local t = require('luatest')

local helpers = require('test.helper')
local vtest = require('test.vshard_helpers.vtest')

local g = t.group('async_bootstrap')

local function prepare_clean_cluster(cg)
    local cfg = {
        sharding = {
            ['s-1'] = {
                replicas = {
                    ['s1-master'] = {
                        instance_uuid = helpers.uuid('b', 1),
                        master = true,
                    },
                },
            },
        },
        bucket_count = 3000,
        storage_entrypoint = nil,
        router_entrypoint = nil,
        all_entrypoint = nil,
        crud_init = false,
    }

    cg.cfg = vtest.config_new(cfg)
    vtest.cluster_new(cg, cg.cfg)
end


g.test_async_storage_bootstrap = function(cg)
    helpers.skip_if_box_watch_unsupported()

    -- Prepare a clean vshard cluster with 1 router and 1 storage.
    prepare_clean_cluster(cg)

    -- Sync bootstrap router.
    cg.cluster:server('router'):exec(function()
        require('crud').init_router()
    end)

    -- Async bootstrap storage.
    cg.cluster:server('s1-master'):exec(function()
        require('crud').init_storage{async = true}
    end)

    -- Assert storage is ready after some time.
    cg.router = cg.cluster:server('router')
    helpers.wait_crud_is_ready_on_cluster(cg, {backend = helpers.backend.VSHARD})
end

g.after_test('test_async_storage_bootstrap', function(cg)
    if cg.cluster ~= nil then
        cg.cluster:drop()
    end
end)


g.test_async_storage_bootstrap_unsupported = function(cg)
    helpers.skip_if_box_watch_supported()

    -- Prepare a clean vshard cluster with 1 router and 1 storage.
    prepare_clean_cluster(cg)

    -- Async bootstrap storage (fails).
    cg.cluster:server('s1-master'):exec(function()
        t.assert_error_msg_contains(
            'async start is supported only for Tarantool versions with box.watch support',
            function()
                require('crud').init_storage{async = true}
            end
        )
    end)
end

g.after_test('test_async_storage_bootstrap_unsupported', function(cg)
    if cg.cluster ~= nil then
        cg.cluster:drop()
    end
end)
