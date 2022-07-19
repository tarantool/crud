local fio = require('fio')

local t = require('luatest')

local helpers = require('test.helper')

local pgroup = t.group('migration', {
    {engine = 'memtx'},
    {engine = 'vinyl'},
})

pgroup.before_all(function(g)
    g.cluster = helpers.Cluster:new({
        datadir = fio.tempdir(),
        server_command = helpers.entrypoint('srv_migration'),
        use_vshard = true,
        replicasets = helpers.get_test_replicasets(),
        env = {
            ['ENGINE'] = g.params.engine,
        },
    })
    g.cluster:start()
end)

pgroup.after_all(function(g) helpers.stop_cluster(g.cluster) end)

pgroup.test_gh_308_select_after_improper_ddl_space_drop = function(g)
    -- Create a space sharded by key with ddl tools.
    helpers.call_on_storages(g.cluster, function(server)
        server.net_box:eval([[
            local migrator_utils = require('migrator.utils')

            if not box.info.ro then
                box.schema.space.create('customers_v2')

                box.space['customers_v2']:format{
                    {name = 'id_v2',        is_nullable = false, type = 'unsigned'},
                    {name = 'bucket_id',    is_nullable = false, type = 'unsigned'},
                    {name = 'sharding_key', is_nullable = false, type = 'unsigned'},
                }

                box.space['customers_v2']:create_index('pk',        {parts = { 'id_v2' }})
                box.space['customers_v2']:create_index('bucket_id', {parts = { 'bucket_id' }})

                migrator_utils.register_sharding_key('customers_v2', {'sharding_key'})
            end
        ]])
    end)

    -- Do not do any requests to refresh sharding metadata.

    -- Drop space, but do not clean up ddl sharding data.
    helpers.call_on_storages(g.cluster, function(server)
        server.net_box:eval([[
            if not box.info.ro then
                box.space['customers_v2']:drop()
            end
        ]])
    end)

    -- Ensure that crud request for existing space is ok.
    local _, err = g.cluster.main_server.net_box:call('crud.select',
                                                      {'customers', nil, { first = 1 }})
    t.assert_equals(err, nil)
end
