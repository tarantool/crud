local fio = require('fio')

local t = require('luatest')

local helpers = require('test.helper')

local pgroup = t.group('vshard_custom', {
    {engine = 'memtx', option = 'group_name'},
    {engine = 'memtx', option = 'router_object'},
    {engine = 'vinyl', option = 'group_name'},
    {engine = 'vinyl', option = 'router_object'},
})

pgroup.before_all(function(g)
    helpers.skip_cartridge_unsupported()

    g.cluster = helpers.Cluster:new({
        datadir = fio.tempdir(),
        server_command = helpers.entrypoint_cartridge('srv_vshard_custom'),
        use_vshard = true,
        replicasets = {
            {
                alias = 'router',
                uuid = helpers.uuid('a'),
                roles = { 'crud-router' },
                servers = {{
                    alias = 'router',
                    http_port = 8081,
                    advertise_port = 13301,
                    instance_uuid = helpers.uuid('a', 'a', 1)
                }},
            },
            {
                alias = 'customers-storage-1',
                uuid = helpers.uuid('b1'),
                roles = { 'customers-storage', 'customers-storage-ddl' },
                vshard_group = 'customers',
                servers = {{
                    alias = 'customers-storage-1',
                    http_port = 8082,
                    advertise_port = 13302,
                    instance_uuid = helpers.uuid('b1', 'b1', 2)
                }},
            },
            {
                alias = 'customers-storage-2',
                uuid = helpers.uuid('b2'),
                roles = { 'customers-storage', 'customers-storage-ddl' },
                vshard_group = 'customers',
                servers = {{
                    alias = 'customers-storage-2',
                    http_port = 8083,
                    advertise_port = 13303,
                    instance_uuid = helpers.uuid('b2', 'b2', 2)
                }},
            },
            {
                alias = 'locations-storage-1',
                uuid = helpers.uuid('c1'),
                roles = { 'locations-storage', 'locations-storage-ddl' },
                vshard_group = 'locations',
                servers = {{
                    alias = 'locations-storage-1',
                    http_port = 8084,
                    advertise_port = 13304,
                    instance_uuid = helpers.uuid('c1', 'c1', 2)
                }},
            },
            {
                alias = 'locations-storage-2',
                uuid = helpers.uuid('c2'),
                roles = { 'locations-storage', 'locations-storage-ddl' },
                vshard_group = 'locations',
                servers = {{
                    alias = 'locations-storage-2',
                    http_port = 8085,
                    advertise_port = 13305,
                    instance_uuid = helpers.uuid('c2', 'c2', 2)
                }},
            },
        },
        env = {
            ['ENGINE'] = g.params.engine,
        },
    })

    g.cluster:start()
    g.router = g.cluster:server('router').net_box
end)

pgroup.after_all(function(g) helpers.stop_cartridge_cluster(g.cluster) end)

pgroup.before_all(function(g)
    g.router:eval([[
        local checks = require('checks')
        local cartridge = require('cartridge')
        local router_service = cartridge.service_get('vshard-router')

        local function prepare_data(space_name, vshard_router, tuple)
            checks('string', 'string', 'table')

            local router = router_service.get(vshard_router)
            assert(router ~= nil)

            local bucket_id_field = 2
            local sharding_field
            if space_name:find('ddl') ~= nil then
                sharding_field = 3
            else
                sharding_field = 1
            end

            local bucket_id = router:bucket_id_strcrc32(tuple[sharding_field])
            tuple[bucket_id_field] = bucket_id

            local replicaset = router:route(bucket_id)
            assert(replicaset ~= nil)

            local space = replicaset.master.conn.space[space_name]
            assert(space ~= nil)

            local res, err = space:replace(tuple)

            if err ~= nil then
                error(err)
            end

            return res
        end

        rawset(_G, 'prepare_data', prepare_data)

        local function call_wrapper_opts2(option, func, space_name, opts)
            if option == 'router_object' and opts ~= nil and type(opts.vshard_router) == 'string' then
                local router = router_service.get(opts.vshard_router)
                opts.vshard_router = router
            end

            return crud[func](space_name, opts)
        end

        rawset(_G, 'call_wrapper_opts2', call_wrapper_opts2)

        local function call_wrapper_opts3(option, func, space_name, arg, opts)
            if option == 'router_object' and opts ~= nil and type(opts.vshard_router) == 'string' then
                local router = router_service.get(opts.vshard_router)
                opts.vshard_router = router
            end

            return crud[func](space_name, arg, opts)
        end

        rawset(_G, 'call_wrapper_opts3', call_wrapper_opts3)

        local function call_wrapper_opts4(option, func, space_name, arg1, arg2, opts)
            if option == 'router_object' and opts ~= nil and type(opts.vshard_router) == 'string' then
                local router = router_service.get(opts.vshard_router)
                opts.vshard_router = router
            end

            return crud[func](space_name, arg1, arg2, opts)
        end

        rawset(_G, 'call_wrapper_opts4', call_wrapper_opts4)

        local function call_pairs_wrapper(option, space_name, arg1, opts)
            if option == 'router_object' and opts ~= nil and type(opts.vshard_router) == 'string' then
                local router = router_service.get(opts.vshard_router)
                opts.vshard_router = router
            end

            local result = {}
            for _, v in crud.pairs(space_name, arg1, opts) do
                table.insert(result, v)
            end

            return result
        end

        rawset(_G, 'call_pairs_wrapper', call_pairs_wrapper)
    ]])

    g.call_router_opts2 = function(g, ...)
        return g.router:call('call_wrapper_opts2', {g.params.option, ...})
    end

    g.call_router_opts3 = function(g, ...)
        return g.router:call('call_wrapper_opts3', {g.params.option, ...})
    end

    g.call_router_opts4 = function(g, ...)
        return g.router:call('call_wrapper_opts4', {g.params.option, ...})
    end

    g.call_router_pairs = function(g, ...)
        return g.router:call('call_pairs_wrapper', {g.params.option, ...})
    end
end)

pgroup.before_each(function(g)
    helpers.truncate_space_on_cluster(g.cluster, 'customers')
    helpers.truncate_space_on_cluster(g.cluster, 'customers_ddl')
    helpers.truncate_space_on_cluster(g.cluster, 'locations')
    helpers.truncate_space_on_cluster(g.cluster, 'locations_ddl')

    g.router:call('prepare_data', {'customers', 'customers', {1, box.NULL, 'Akiyama Shun', 32}})
    g.router:call('prepare_data', {'customers', 'customers', {2, box.NULL, 'Kazuma Kiryu', 41}})
    g.router:call('prepare_data', {'customers_ddl', 'customers', {1, box.NULL, 'Akiyama Shun', 32}})
    g.router:call('prepare_data', {'customers_ddl', 'customers', {2, box.NULL, 'Kazuma Kiryu', 41}})

    g.router:call('prepare_data', {'locations', 'locations', {'Sky Finance', box.NULL, 'Credit company', 2}})
    g.router:call('prepare_data', {'locations', 'locations', {'Sunflower', box.NULL, 'Orphanage', 1}})
    g.router:call('prepare_data', {'locations_ddl', 'locations', {'Sky Finance', box.NULL, 'Credit company', 2}})
    g.router:call('prepare_data', {'locations_ddl', 'locations', {'Sunflower', box.NULL, 'Orphanage', 1}})
end)

pgroup.test_call_min = function(g)
    local result, err = g:call_router_opts3(
        'min', 'customers', 'age', {vshard_router = 'customers'})

    t.assert_equals(err, nil)
    t.assert_items_equals(result.rows, {{1, 12477, 'Akiyama Shun', 32}})
end

pgroup.test_call_min_wrong_router = function(g)
    local result, err = g:call_router_opts3(
        'min', 'customers', 'age', {vshard_router = 'locations'})

    t.assert_equals(result, nil)
    t.assert_str_contains(err.err, "Space \"customers\" doesn't exist")
end

pgroup.test_call_min_no_router = function(g)
    local result, err = g:call_router_opts3(
        'min', 'customers', 'age')

    t.assert_equals(result, nil)
    t.assert_str_contains(err.err,
                          "Default vshard group is not found and custom is not specified with opts.vshard_router")
end

pgroup.test_call_min_wrong_option = function(g)
    local result, err = g:call_router_opts3(
        'min', 'customers', 'age', {vshard_router = {group = 'customers'}})

    t.assert_equals(result, nil)
    t.assert_str_contains(err.err,
                          "Invalid opts.vshard_router table value, a vshard router instance has been expected")
end

pgroup.test_call_max = function(g)
    local result, err = g:call_router_opts3(
        'max', 'locations', 'workers', {vshard_router = 'locations'})

    t.assert_equals(err, nil)
    t.assert_items_equals(result.rows, {{'Sky Finance', 26826, 'Credit company', 2}})
end

pgroup.test_call_max_wrong_router = function(g)
    local result, err = g:call_router_opts3(
        'max', 'locations', 'workers', {vshard_router = 'customers'})

    t.assert_equals(result, nil)
    t.assert_str_contains(err.err, "Space \"locations\" doesn't exist")
end

pgroup.test_call_max_no_router = function(g)
    local result, err = g:call_router_opts3(
        'max', 'locations', 'workers')

    t.assert_equals(result, nil)
    t.assert_str_contains(err.err,
                          "Default vshard group is not found and custom is not specified with opts.vshard_router")
end

pgroup.test_call_max_wrong_option = function(g)
    local result, err = g:call_router_opts3(
        'max', 'locations', 'workers', {vshard_router = {group = 'locations'}})

    t.assert_equals(result, nil)
    t.assert_str_contains(err.err,
                          "Invalid opts.vshard_router table value, a vshard router instance has been expected")
end

pgroup.test_call_count_with_default_sharding = function(g)
    local result, err = g:call_router_opts3(
        'count', 'locations', {{'=', 'name', 'Sunflower'}}, {vshard_router = 'locations'})

    t.assert_equals(err, nil)
    t.assert_equals(result, 1)
end

pgroup.test_call_count_with_ddl_sharding = function(g)
    local result, err = g:call_router_opts3(
        'count', 'customers_ddl', {{'=', 'age', 41}}, {vshard_router = 'customers'})

    t.assert_equals(err, nil)
    t.assert_equals(result, 1)
end

pgroup.test_call_count_wrong_router = function(g)
    local result, err = g:call_router_opts3(
        'count', 'locations', {{'=', 'name', 'Sunflower'}}, {vshard_router = 'customers'})

    t.assert_equals(result, nil)
    t.assert_str_contains(err.err, "Space \"locations\" doesn't exist")
end

pgroup.test_call_count_no_router = function(g)
    local result, err = g:call_router_opts3(
        'count', 'locations', {{'=', 'name', 'Sunflower'}})

    t.assert_equals(result, nil)
    t.assert_str_contains(err.err,
                          "Default vshard group is not found and custom is not specified with opts.vshard_router")
end

pgroup.test_call_count_wrong_option = function(g)
    local result, err = g:call_router_opts3(
        'count', 'locations', {{'=', 'name', 'Sunflower'}}, {vshard_router = {group = 'locations'}})

    t.assert_equals(result, nil)
    t.assert_str_contains(err.err,
                          "Invalid opts.vshard_router table value, a vshard router instance has been expected")
end

pgroup.before_test('test_call_delete_with_default_sharding', function(g)
    local storage = g.cluster:server('locations-storage-1').net_box
    local storage_result = storage.space['locations']:get{'Sky Finance'}
    t.assert_equals(storage_result, {'Sky Finance', 26826, 'Credit company', 2})
end)

pgroup.test_call_delete_with_default_sharding = function(g)
    local result, err = g:call_router_opts3(
        'delete', 'locations', {'Sky Finance'}, {vshard_router = 'locations'})

    t.assert_equals(err, nil)
    if g.params.engine == 'memtx' then
        t.assert_items_equals(result.rows, {{'Sky Finance', 26826, 'Credit company', 2}})
    else
        t.assert_equals(#result.rows, 0)
    end

    local storage = g.cluster:server('locations-storage-1').net_box
    local storage_result = storage.space['locations']:get{'Sky Finance'}
    t.assert_equals(storage_result, nil)
end

pgroup.before_test('test_call_delete_with_ddl_sharding', function(g)
    local storage = g.cluster:server('customers-storage-2').net_box
    local storage_result = storage.space['customers_ddl']:get{2, 'Kazuma Kiryu'}
    t.assert_equals(storage_result, {2, 8768, 'Kazuma Kiryu', 41})
end)

pgroup.test_call_delete_with_ddl_sharding = function(g)
    local result, err = g:call_router_opts3(
        'delete', 'customers_ddl', {2, 'Kazuma Kiryu'}, {vshard_router = 'customers'})

    t.assert_equals(err, nil)
    if g.params.engine == 'memtx' then
        t.assert_items_equals(result.rows, {{2, 8768, 'Kazuma Kiryu', 41}})
    else
        t.assert_equals(#result.rows, 0)
    end

    local storage = g.cluster:server('customers-storage-2').net_box
    local storage_result = storage.space['customers_ddl']:get{2, 'Kazuma Kiryu'}
    t.assert_equals(storage_result, nil)
end

pgroup.test_call_delete_wrong_router = function(g)
    local result, err = g:call_router_opts3(
        'delete', 'locations', {'Sky Finance'}, {vshard_router = 'customers'})

    t.assert_equals(result, nil)
    t.assert_str_contains(err.err, "Space \"locations\" doesn't exist")
end

pgroup.test_call_delete_no_router = function(g)
    local result, err = g:call_router_opts3(
        'delete', 'locations', {'Sky Finance'})

    t.assert_equals(result, nil)
    t.assert_str_contains(err.err,
                          "Default vshard group is not found and custom is not specified with opts.vshard_router")
end

pgroup.test_call_delete_wrong_option = function(g)
    local result, err = g:call_router_opts3(
        'delete', 'locations', {'Sky Finance'}, {vshard_router = {group = 'locations'}})

    t.assert_equals(result, nil)
    t.assert_str_contains(err.err,
                          "Invalid opts.vshard_router table value, a vshard router instance has been expected")
end

pgroup.test_call_get_with_default_sharding = function(g)
    local result, err = g:call_router_opts3(
        'get', 'locations', {'Sky Finance'}, {vshard_router = 'locations'})

    t.assert_equals(err, nil)
    t.assert_items_equals(result.rows, {{'Sky Finance', 26826, 'Credit company', 2}})
end

pgroup.test_call_get_with_ddl_sharding = function(g)
    local result, err = g:call_router_opts3(
        'get', 'customers_ddl', {2, 'Kazuma Kiryu'}, {vshard_router = 'customers'})

    t.assert_equals(err, nil)
    t.assert_items_equals(result.rows, {{2, 8768, 'Kazuma Kiryu', 41}})
end

pgroup.test_call_get_wrong_router = function(g)
    local result, err = g:call_router_opts3(
        'get', 'locations', {'Sky Finance'}, {vshard_router = 'customers'})

    t.assert_equals(result, nil)
    t.assert_str_contains(err.err, "Space \"locations\" doesn't exist")
end

pgroup.test_call_get_no_router = function(g)
    local result, err = g:call_router_opts3(
        'get', 'locations', {'Sky Finance'})

    t.assert_equals(result, nil)
    t.assert_str_contains(err.err,
                          "Default vshard group is not found and custom is not specified with opts.vshard_router")
end

pgroup.test_call_get_wrong_option = function(g)
    local result, err = g:call_router_opts3(
        'get', 'locations', {'Sky Finance'}, {vshard_router = {group = 'locations'}})

    t.assert_equals(result, nil)
    t.assert_str_contains(err.err,
                          "Invalid opts.vshard_router table value, a vshard router instance has been expected")
end

pgroup.test_call_insert_with_default_sharding = function(g)
    local result, err = g:call_router_opts3(
        'insert',
        'customers_ddl',
        {4, box.NULL, 'Taiga Saejima', 45},
        {vshard_router = 'customers'})

    t.assert_equals(err, nil)
    t.assert_items_equals(result.rows, {{4, 4344, 'Taiga Saejima', 45}})

    local storage = g.cluster:server('customers-storage-2').net_box
    local storage_result = storage.space['customers_ddl']:get{4, 'Taiga Saejima'}
    t.assert_equals(storage_result, {4, 4344, 'Taiga Saejima', 45})
end

pgroup.test_call_insert_with_ddl_sharding = function(g)
    local result, err = g:call_router_opts3(
        'insert',
        'locations',
        {'Okinawa Penitentiary No. 2', box.NULL, 'Prison', 100},
        {vshard_router = 'locations'})

    t.assert_equals(err, nil)
    t.assert_items_equals(result.rows, {{'Okinawa Penitentiary No. 2', 19088, 'Prison', 100}})

    local storage = g.cluster:server('locations-storage-1').net_box
    local storage_result = storage.space['locations']:get{'Okinawa Penitentiary No. 2'}
    t.assert_equals(storage_result, {'Okinawa Penitentiary No. 2', 19088, 'Prison', 100})
end

pgroup.test_call_insert_wrong_router = function(g)
    local result, err = g:call_router_opts3(
        'insert',
        'locations',
        {'Okinawa Penitentiary No. 2', box.NULL, 'Prison', 100},
        {vshard_router = 'customers'})

    t.assert_equals(result, nil)
    t.assert_str_contains(err.err, "Space \"locations\" doesn't exist")
end

pgroup.test_call_insert_no_router = function(g)
    local result, err = g:call_router_opts3(
        'insert',
        'locations',
        {'Okinawa Penitentiary No. 2', box.NULL, 'Prison', 100})

    t.assert_equals(result, nil)
    t.assert_str_contains(err.err,
                          "Default vshard group is not found and custom is not specified with opts.vshard_router")
end

pgroup.test_call_insert_wrong_option = function(g)
    local result, err = g:call_router_opts3(
        'insert',
        'locations',
        {'Okinawa Penitentiary No. 2', box.NULL, 'Prison', 100},
        {vshard_router = {group = 'locations'}})

    t.assert_equals(result, nil)
    t.assert_str_contains(err.err,
                          "Invalid opts.vshard_router table value, a vshard router instance has been expected")
end

pgroup.test_call_insert_object_with_default_sharding = function(g)
    local result, err = g:call_router_opts3(
        'insert_object',
        'customers_ddl',
        {id = 4, bucket_id = box.NULL, name = 'Taiga Saejima', age = 45},
        {vshard_router = 'customers'})

    t.assert_equals(err, nil)
    t.assert_items_equals(result.rows, {{4, 4344, 'Taiga Saejima', 45}})

    local storage = g.cluster:server('customers-storage-2').net_box
    local storage_result = storage.space['customers_ddl']:get{4, 'Taiga Saejima'}
    t.assert_equals(storage_result, {4, 4344, 'Taiga Saejima', 45})
end

pgroup.test_call_insert_object_with_ddl_sharding = function(g)
    local result, err = g:call_router_opts3(
        'insert_object',
        'locations',
        {name = 'Okinawa Penitentiary No. 2', bucket_id = box.NULL, type = 'Prison', workers = 100},
        {vshard_router = 'locations'})

    t.assert_equals(err, nil)
    t.assert_items_equals(result.rows, {{'Okinawa Penitentiary No. 2', 19088, 'Prison', 100}})

    local storage = g.cluster:server('locations-storage-1').net_box
    local storage_result = storage.space['locations']:get{'Okinawa Penitentiary No. 2'}
    t.assert_equals(storage_result, {'Okinawa Penitentiary No. 2', 19088, 'Prison', 100})
end

pgroup.test_call_insert_object_wrong_router = function(g)
    local result, err = g:call_router_opts3(
        'insert_object',
        'locations',
        {name = 'Okinawa Penitentiary No. 2', bucket_id = box.NULL, type = 'Prison', workers = 100},
        {vshard_router = 'customers'})

    t.assert_equals(result, nil)
    t.assert_str_contains(err.err, "Space \"locations\" doesn't exist")
end

pgroup.test_call_insert_object_no_router = function(g)
    local result, err = g:call_router_opts3(
        'insert_object',
        'locations',
        {name = 'Okinawa Penitentiary No. 2', bucket_id = box.NULL, type = 'Prison', workers = 100})

    t.assert_equals(result, nil)
    t.assert_str_contains(err.err,
                          "Default vshard group is not found and custom is not specified with opts.vshard_router")
end

pgroup.test_call_insert_object_wrong_option = function(g)
    local result, err = g:call_router_opts3(
        'insert_object',
        'locations',
        {name = 'Okinawa Penitentiary No. 2', bucket_id = box.NULL, type = 'Prison', workers = 100},
        {vshard_router = {group = 'locations'}})

    t.assert_equals(result, nil)
    t.assert_str_contains(err.err,
                          "Invalid opts.vshard_router table value, a vshard router instance has been expected")
end

pgroup.test_call_insert_many_with_default_sharding = function(g)
    local result, errs = g:call_router_opts3(
        'insert_many',
        'customers',
        {
            {3, box.NULL, 'Masayoshi Tanimura', 29},
            {4, box.NULL, 'Taiga Saejima', 45},
        },
        {vshard_router = 'customers'})

    t.assert_equals(errs, nil)
    t.assert_items_equals(result.rows,
                         {
                             {3, 11804, 'Masayoshi Tanimura', 29},
                             {4, 28161, 'Taiga Saejima', 45}
                         })

    local storage = g.cluster:server('customers-storage-2').net_box
    local storage_result = storage.space['customers']:get{3}
    t.assert_equals(storage_result, {3, 11804, 'Masayoshi Tanimura', 29})

    local storage = g.cluster:server('customers-storage-1').net_box
    local storage_result = storage.space['customers']:get{4}
    t.assert_equals(storage_result, {4, 28161, 'Taiga Saejima', 45})
end

pgroup.test_call_insert_many_with_ddl_sharding = function(g)
    local result, errs = g:call_router_opts3(
        'insert_many',
        'locations_ddl',
        {
            {'Tokyo Police Department', box.NULL, 'Police', 40000},
            {'Okinawa Penitentiary No. 2', box.NULL, 'Prison', 100}
        },
        {vshard_router = 'locations'})

    t.assert_equals(errs, nil)
    t.assert_items_equals(result.rows,
                    {
                        {'Tokyo Police Department', 28259, 'Police', 40000},
                        {'Okinawa Penitentiary No. 2', 6427, 'Prison', 100}
                    })

    local storage = g.cluster:server('locations-storage-1').net_box
    local storage_result = storage.space['locations_ddl']:get{'Tokyo Police Department', 'Police'}
    t.assert_equals(storage_result, {'Tokyo Police Department', 28259, 'Police', 40000})

    local storage = g.cluster:server('locations-storage-2').net_box
    local storage_result = storage.space['locations_ddl']:get{'Okinawa Penitentiary No. 2', 'Prison'}
    t.assert_equals(storage_result, {'Okinawa Penitentiary No. 2', 6427, 'Prison', 100})
end

pgroup.test_call_insert_many_wrong_router = function(g)
    local result, errs = g:call_router_opts3(
        'insert_many',
        'locations',
        {
            {'Tokyo Police Department', box.NULL, 'Police', 40000},
            {'Okinawa Penitentiary No. 2', box.NULL, 'Prison', 100},
        },
        {vshard_router = 'customers'})

    t.assert_equals(result, nil)
    t.assert_str_contains(errs[1].err, "Space \"locations\" doesn't exist")
end

pgroup.test_call_insert_many_no_router = function(g)
    local result, errs = g:call_router_opts3(
        'insert_many',
        'locations',
        {
            {'Tokyo Police Department', box.NULL, 'Police', 40000},
            {'Okinawa Penitentiary No. 2', box.NULL, 'Prison', 100}
        })

    t.assert_equals(result, nil)
    t.assert_str_contains(errs[1].err,
                          "Default vshard group is not found and custom is not specified with opts.vshard_router")
end

pgroup.test_call_insert_many_wrong_option = function(g)
    local result, errs = g:call_router_opts3(
        'insert_many',
        'locations',
        {
            {'Tokyo Police Department', box.NULL, 'Police', 40000},
            {'Okinawa Penitentiary No. 2', box.NULL, 'Prison', 100}
        },
        {vshard_router = {group = 'locations'}})

    t.assert_equals(result, nil)
    t.assert_str_contains(errs[1].err,
                          "Invalid opts.vshard_router table value, a vshard router instance has been expected")
end

pgroup.test_call_insert_object_many_with_default_sharding = function(g)
    local result, errs = g:call_router_opts3(
        'insert_object_many',
        'locations',
        {
            {name = 'Tokyo Police Department', bucket_id = box.NULL, type = 'Police', workers = 40000},
            {name = 'Okinawa Penitentiary No. 2', bucket_id = box.NULL, type = 'Prison', workers = 100}
        },
        {vshard_router = 'locations'}
    )

    t.assert_equals(errs, nil)
    t.assert_items_equals(result.rows,
                          {
                              {'Tokyo Police Department', 9017, 'Police', 40000},
                              {'Okinawa Penitentiary No. 2', 19088, 'Prison', 100},
                          })

    local storage = g.cluster:server('locations-storage-2').net_box
    local storage_result = storage.space['locations']:get{'Tokyo Police Department'}
    t.assert_equals(storage_result, {'Tokyo Police Department', 9017, 'Police', 40000})

    local storage = g.cluster:server('locations-storage-1').net_box
    local storage_result = storage.space['locations']:get{'Okinawa Penitentiary No. 2'}
    t.assert_equals(storage_result, {'Okinawa Penitentiary No. 2', 19088, 'Prison', 100})
end

pgroup.test_call_insert_object_many_with_ddl_sharding = function(g)
    local result, errs = g:call_router_opts3(
        'insert_object_many',
        'customers_ddl',
        {
            {id = 3, bucket_id = box.NULL, name = 'Masayoshi Tanimura', age = 29},
            {id = 4, bucket_id = box.NULL, name = 'Taiga Saejima', age = 45},
        },
        {vshard_router = 'customers'}
    )

    t.assert_equals(errs, nil)
    t.assert_items_equals(result.rows,
                          {
                              {3, 2900, 'Masayoshi Tanimura', 29},
                              {4, 4344, 'Taiga Saejima', 45}
                          })

    local storage = g.cluster:server('customers-storage-2').net_box
    local storage_result = storage.space['customers_ddl']:get{3, 'Masayoshi Tanimura'}
    t.assert_equals(storage_result, {3, 2900, 'Masayoshi Tanimura', 29})

    local storage = g.cluster:server('customers-storage-2').net_box
    local storage_result = storage.space['customers_ddl']:get{4, 'Taiga Saejima'}
    t.assert_equals(storage_result, {4, 4344, 'Taiga Saejima', 45})
end

pgroup.test_call_insert_object_many_wrong_router = function(g)
    local result, errs = g:call_router_opts3(
        'insert_object_many',
        'customers',
        {
            {id = 3, bucket_id = box.NULL, name = 'Masayoshi Tanimura', age = 29},
            {id = 4, bucket_id = box.NULL, name = 'Taiga Saejima', age = 45},
        },
        {vshard_router = 'locations'})

    t.assert_equals(result, nil)
    t.assert_str_contains(errs[1].err, "Space \"customers\" doesn't exist")
end

pgroup.test_call_insert_object_many_no_router = function(g)
    local result, errs = g:call_router_opts3(
        'insert_object_many',
        'customers',
        {
            {id = 3, bucket_id = box.NULL, name = 'Masayoshi Tanimura', age = 29},
            {id = 4, bucket_id = box.NULL, name = 'Taiga Saejima', age = 45},
        })

    t.assert_equals(result, nil)
    t.assert_str_contains(errs[1].err,
                          "Default vshard group is not found and custom is not specified with opts.vshard_router")
end

pgroup.test_call_insert_object_many_wrong_option = function(g)
    local result, errs = g:call_router_opts3(
        'insert_object_many',
        'customers',
        {
            {id = 3, bucket_id = box.NULL, name = 'Masayoshi Tanimura', age = 29},
            {id = 4, bucket_id = box.NULL, name = 'Taiga Saejima', age = 45},
        },
        {vshard_router = {group = 'customers'}})

    t.assert_equals(result, nil)
    t.assert_str_contains(errs[1].err,
                          "Invalid opts.vshard_router table value, a vshard router instance has been expected")
end

pgroup.test_call_len = function(g)
    local result, err = g:call_router_opts2(
        'len', 'customers', {vshard_router = 'customers'})

    t.assert_equals(err, nil)
    t.assert_equals(result, 2)
end

pgroup.test_call_len_wrong_router = function(g)
    local result, err = g:call_router_opts2(
        'len', 'customers', {vshard_router = 'locations'})

    t.assert_equals(result, nil)
    t.assert_str_contains(err.err, "Space \"customers\" doesn't exist")
end

pgroup.test_call_len_no_router = function(g)
    local result, err = g:call_router_opts2(
        'len', 'customers')

    t.assert_equals(result, nil)
    t.assert_str_contains(err.err,
                          "Default vshard group is not found and custom is not specified with opts.vshard_router")
end

pgroup.test_call_len_wrong_option = function(g)
    local result, err = g:call_router_opts2(
        'len', 'customers', {vshard_router = {group = 'customers'}})

    t.assert_equals(result, nil)
    t.assert_str_contains(err.err,
                          "Invalid opts.vshard_router table value, a vshard router instance has been expected")
end

pgroup.test_call_replace_with_default_sharding = function(g)
    local result, err = g:call_router_opts3(
        'replace',
        'customers_ddl',
        {4, box.NULL, 'Taiga Saejima', 45},
        {vshard_router = 'customers'})

    t.assert_equals(err, nil)
    t.assert_items_equals(result.rows, {{4, 4344, 'Taiga Saejima', 45}})

    local storage = g.cluster:server('customers-storage-2').net_box
    local storage_result = storage.space['customers_ddl']:get{4, 'Taiga Saejima'}
    t.assert_equals(storage_result, {4, 4344, 'Taiga Saejima', 45})
end

pgroup.test_call_replace_with_ddl_sharding = function(g)
    local result, err = g:call_router_opts3(
        'replace',
        'locations',
        {'Okinawa Penitentiary No. 2', box.NULL, 'Prison', 100},
        {vshard_router = 'locations'})

    t.assert_equals(err, nil)
    t.assert_items_equals(result.rows, {{'Okinawa Penitentiary No. 2', 19088, 'Prison', 100}})

    local storage = g.cluster:server('locations-storage-1').net_box
    local storage_result = storage.space['locations']:get{'Okinawa Penitentiary No. 2'}
    t.assert_equals(storage_result, {'Okinawa Penitentiary No. 2', 19088, 'Prison', 100})
end

pgroup.test_call_replace_wrong_router = function(g)
    local result, err = g:call_router_opts3(
        'replace',
        'locations',
        {'Okinawa Penitentiary No. 2', box.NULL, 'Prison', 100},
        {vshard_router = 'customers'})

    t.assert_equals(result, nil)
    t.assert_str_contains(err.err, "Space \"locations\" doesn't exist")
end

pgroup.test_call_replace_no_router = function(g)
    local result, err = g:call_router_opts3(
        'replace',
        'locations',
        {'Okinawa Penitentiary No. 2', box.NULL, 'Prison', 100})

    t.assert_equals(result, nil)
    t.assert_str_contains(err.err,
                          "Default vshard group is not found and custom is not specified with opts.vshard_router")
end

pgroup.test_call_replace_wrong_option = function(g)
    local result, err = g:call_router_opts3(
        'replace',
        'locations',
        {'Okinawa Penitentiary No. 2', box.NULL, 'Prison', 100},
        {vshard_router = {group = 'locations'}})

    t.assert_equals(result, nil)
    t.assert_str_contains(err.err,
                          "Invalid opts.vshard_router table value, a vshard router instance has been expected")
end

pgroup.test_call_replace_object_with_default_sharding = function(g)
    local result, err = g:call_router_opts3(
        'replace_object',
        'customers_ddl',
        {id = 4, bucket_id = box.NULL, name = 'Taiga Saejima', age = 45},
        {vshard_router = 'customers'})

    t.assert_equals(err, nil)
    t.assert_items_equals(result.rows, {{4, 4344, 'Taiga Saejima', 45}})

    local storage = g.cluster:server('customers-storage-2').net_box
    local storage_result = storage.space['customers_ddl']:get{4, 'Taiga Saejima'}
    t.assert_equals(storage_result, {4, 4344, 'Taiga Saejima', 45})
end

pgroup.test_call_replace_object_with_ddl_sharding = function(g)
    local result, err = g:call_router_opts3(
        'replace_object',
        'locations',
        {name = 'Okinawa Penitentiary No. 2', bucket_id = box.NULL, type = 'Prison', workers = 100},
        {vshard_router = 'locations'})

    t.assert_equals(err, nil)
    t.assert_items_equals(result.rows, {{'Okinawa Penitentiary No. 2', 19088, 'Prison', 100}})

    local storage = g.cluster:server('locations-storage-1').net_box
    local storage_result = storage.space['locations']:get{'Okinawa Penitentiary No. 2'}
    t.assert_equals(storage_result, {'Okinawa Penitentiary No. 2', 19088, 'Prison', 100})
end

pgroup.test_call_replace_object_wrong_router = function(g)
    local result, err = g:call_router_opts3(
        'replace_object',
        'locations',
        {name = 'Okinawa Penitentiary No. 2', bucket_id = box.NULL, type = 'Prison', workers = 100},
        {vshard_router = 'customers'})

    t.assert_equals(result, nil)
    t.assert_str_contains(err.err, "Space \"locations\" doesn't exist")
end

pgroup.test_call_replace_object_no_router = function(g)
    local result, err = g:call_router_opts3(
        'replace_object',
        'locations',
        {name = 'Okinawa Penitentiary No. 2', bucket_id = box.NULL, type = 'Prison', workers = 100})

    t.assert_equals(result, nil)
    t.assert_str_contains(err.err,
                          "Default vshard group is not found and custom is not specified with opts.vshard_router")
end

pgroup.test_call_replace_object_wrong_option = function(g)
    local result, err = g:call_router_opts3(
        'replace_object',
        'locations',
        {name = 'Okinawa Penitentiary No. 2', bucket_id = box.NULL, type = 'Prison', workers = 100},
        {vshard_router = {group = 'locations'}})

    t.assert_equals(result, nil)
    t.assert_str_contains(err.err,
                          "Invalid opts.vshard_router table value, a vshard router instance has been expected")
end

pgroup.test_call_replace_many_with_default_sharding = function(g)
    local result, errs = g:call_router_opts3(
        'replace_many',
        'customers',
        {
            {3, box.NULL, 'Masayoshi Tanimura', 29},
            {4, box.NULL, 'Taiga Saejima', 45},
        },
        {vshard_router = 'customers'})

    t.assert_equals(errs, nil)
    t.assert_items_equals(result.rows,
                         {
                             {3, 11804, 'Masayoshi Tanimura', 29},
                             {4, 28161, 'Taiga Saejima', 45}
                         })

    local storage = g.cluster:server('customers-storage-2').net_box
    local storage_result = storage.space['customers']:get{3}
    t.assert_equals(storage_result, {3, 11804, 'Masayoshi Tanimura', 29})

    local storage = g.cluster:server('customers-storage-1').net_box
    local storage_result = storage.space['customers']:get{4}
    t.assert_equals(storage_result, {4, 28161, 'Taiga Saejima', 45})
end

pgroup.test_call_replace_many_with_ddl_sharding = function(g)
    local result, errs = g:call_router_opts3(
        'replace_many',
        'locations_ddl',
        {
            {'Tokyo Police Department', box.NULL, 'Police', 40000},
            {'Okinawa Penitentiary No. 2', box.NULL, 'Prison', 100}
        },
        {vshard_router = 'locations'})

    t.assert_equals(errs, nil)
    t.assert_items_equals(result.rows,
                    {
                        {'Tokyo Police Department', 28259, 'Police', 40000},
                        {'Okinawa Penitentiary No. 2', 6427, 'Prison', 100}
                    })

    local storage = g.cluster:server('locations-storage-1').net_box
    local storage_result = storage.space['locations_ddl']:get{'Tokyo Police Department', 'Police'}
    t.assert_equals(storage_result, {'Tokyo Police Department', 28259, 'Police', 40000})

    local storage = g.cluster:server('locations-storage-2').net_box
    local storage_result = storage.space['locations_ddl']:get{'Okinawa Penitentiary No. 2', 'Prison'}
    t.assert_equals(storage_result, {'Okinawa Penitentiary No. 2', 6427, 'Prison', 100})
end

pgroup.test_call_replace_many_wrong_router = function(g)
    local result, errs = g:call_router_opts3(
        'replace_many',
        'locations',
        {
            {'Tokyo Police Department', box.NULL, 'Police', 40000},
            {'Okinawa Penitentiary No. 2', box.NULL, 'Prison', 100},
        },
        {vshard_router = 'customers'})

    t.assert_equals(result, nil)
    t.assert_str_contains(errs[1].err, "Space \"locations\" doesn't exist")
end

pgroup.test_call_replace_many_no_router = function(g)
    local result, errs = g:call_router_opts3(
        'replace_many',
        'locations',
        {
            {'Tokyo Police Department', box.NULL, 'Police', 40000},
            {'Okinawa Penitentiary No. 2', box.NULL, 'Prison', 100}
        })

    t.assert_equals(result, nil)
    t.assert_str_contains(errs[1].err,
                          "Default vshard group is not found and custom is not specified with opts.vshard_router")
end

pgroup.test_call_replace_many_wrong_option = function(g)
    local result, errs = g:call_router_opts3(
        'replace_many',
        'locations',
        {
            {'Tokyo Police Department', box.NULL, 'Police', 40000},
            {'Okinawa Penitentiary No. 2', box.NULL, 'Prison', 100}
        },
        {vshard_router = {group = 'locations'}})

    t.assert_equals(result, nil)
    t.assert_str_contains(errs[1].err,
                          "Invalid opts.vshard_router table value, a vshard router instance has been expected")
end

pgroup.test_call_replace_object_many_with_default_sharding = function(g)
    local result, errs = g:call_router_opts3(
        'replace_object_many',
        'locations',
        {
            {name = 'Tokyo Police Department', bucket_id = box.NULL, type = 'Police', workers = 40000},
            {name = 'Okinawa Penitentiary No. 2', bucket_id = box.NULL, type = 'Prison', workers = 100}
        },
        {vshard_router = 'locations'}
    )

    t.assert_equals(errs, nil)
    t.assert_items_equals(result.rows,
                          {
                              {'Tokyo Police Department', 9017, 'Police', 40000},
                              {'Okinawa Penitentiary No. 2', 19088, 'Prison', 100},
                          })

    local storage = g.cluster:server('locations-storage-2').net_box
    local storage_result = storage.space['locations']:get{'Tokyo Police Department'}
    t.assert_equals(storage_result, {'Tokyo Police Department', 9017, 'Police', 40000})

    local storage = g.cluster:server('locations-storage-1').net_box
    local storage_result = storage.space['locations']:get{'Okinawa Penitentiary No. 2'}
    t.assert_equals(storage_result, {'Okinawa Penitentiary No. 2', 19088, 'Prison', 100})
end

pgroup.test_call_replace_object_many_with_ddl_sharding = function(g)
    local result, errs = g:call_router_opts3(
        'replace_object_many',
        'customers_ddl',
        {
            {id = 3, bucket_id = box.NULL, name = 'Masayoshi Tanimura', age = 29},
            {id = 4, bucket_id = box.NULL, name = 'Taiga Saejima', age = 45},
        },
        {vshard_router = 'customers'}
    )

    t.assert_equals(errs, nil)
    t.assert_items_equals(result.rows,
                          {
                              {3, 2900, 'Masayoshi Tanimura', 29},
                              {4, 4344, 'Taiga Saejima', 45}
                          })

    local storage = g.cluster:server('customers-storage-2').net_box
    local storage_result = storage.space['customers_ddl']:get{3, 'Masayoshi Tanimura'}
    t.assert_equals(storage_result, {3, 2900, 'Masayoshi Tanimura', 29})

    local storage = g.cluster:server('customers-storage-2').net_box
    local storage_result = storage.space['customers_ddl']:get{4, 'Taiga Saejima'}
    t.assert_equals(storage_result, {4, 4344, 'Taiga Saejima', 45})
end

pgroup.test_call_replace_object_many_wrong_router = function(g)
    local result, errs = g:call_router_opts3(
        'replace_object_many',
        'customers',
        {
            {id = 3, bucket_id = box.NULL, name = 'Masayoshi Tanimura', age = 29},
            {id = 4, bucket_id = box.NULL, name = 'Taiga Saejima', age = 45},
        },
        {vshard_router = 'locations'})

    t.assert_equals(result, nil)
    t.assert_str_contains(errs[1].err, "Space \"customers\" doesn't exist")
end

pgroup.test_call_replace_object_many_no_router = function(g)
    local result, errs = g:call_router_opts3(
        'replace_object_many',
        'customers',
        {
            {id = 3, bucket_id = box.NULL, name = 'Masayoshi Tanimura', age = 29},
            {id = 4, bucket_id = box.NULL, name = 'Taiga Saejima', age = 45},
        })

    t.assert_equals(result, nil)
    t.assert_str_contains(errs[1].err,
                          "Default vshard group is not found and custom is not specified with opts.vshard_router")
end

pgroup.test_call_replace_object_many_wrong_option = function(g)
    local result, errs = g:call_router_opts3(
        'replace_object_many',
        'customers',
        {
            {id = 3, bucket_id = box.NULL, name = 'Masayoshi Tanimura', age = 29},
            {id = 4, bucket_id = box.NULL, name = 'Taiga Saejima', age = 45},
        },
        {vshard_router = {group = 'customers'}})

    t.assert_equals(result, nil)
    t.assert_str_contains(errs[1].err,
                          "Invalid opts.vshard_router table value, a vshard router instance has been expected")
end

pgroup.test_call_select_with_default_sharding = function(g)
    local result, err = g:call_router_opts3('select',
        'locations',
        {{'=', 'name', 'Sky Finance'}},
        {vshard_router = 'locations', mode = 'write'}
    )

    t.assert_equals(err, nil)
    t.assert_items_equals(result.rows, {{'Sky Finance', 26826, 'Credit company', 2}})
end

pgroup.test_call_select_with_ddl_sharding = function(g)
    local result, err = g:call_router_opts3('select',
        'customers_ddl',
        {{'=', 'id', 2}, {'=', 'name', 'Kazuma Kiryu'}},
        {vshard_router = 'customers', mode = 'write'}
    )

    t.assert_equals(err, nil)
    t.assert_items_equals(result.rows, {{2, 8768, 'Kazuma Kiryu', 41}})
end

pgroup.test_call_select_wrong_router = function(g)
    local result, err = g:call_router_opts3('select',
        'locations',
        {{'=', 'name', 'Sky Finance'}},
        {vshard_router = 'customers', mode = 'write'}
    )

    t.assert_equals(result, nil)
    t.assert_str_contains(err.err, "Space \"locations\" doesn't exist")
end

pgroup.test_call_select_no_router = function(g)
    local result, err = g:call_router_opts3('select',
        'locations',
        {{'=', 'name', 'Sky Finance'}}
    )

    t.assert_equals(result, nil)
    t.assert_str_contains(err.err,
                          "Default vshard group is not found and custom is not specified with opts.vshard_router")
end

pgroup.test_call_select_wrong_option = function(g)
    local result, err = g:call_router_opts3(
        'select', 'locations', {{'=', 'name', 'Sky Finance'}}, {vshard_router = {group = 'locations'}})

    t.assert_equals(result, nil)
    t.assert_str_contains(err.err,
                          "Invalid opts.vshard_router table value, a vshard router instance has been expected")
end

pgroup.test_call_pairs_with_default_sharding = function(g)
    local result, err = g:call_router_pairs(
        'locations',
        {{'=', 'name', 'Sky Finance'}},
        {vshard_router = 'locations', mode = 'write'}
    )

    t.assert_equals(err, nil)
    t.assert_items_equals(result, {{'Sky Finance', 26826, 'Credit company', 2}})
end

pgroup.test_call_pairs_with_ddl_sharding = function(g)
    local result, err = g:call_router_pairs(
        'customers_ddl',
        {{'=', 'id', 2}, {'=', 'name', 'Kazuma Kiryu'}},
        {vshard_router = 'customers', mode = 'write'}
    )

    t.assert_equals(err, nil)
    t.assert_items_equals(result, {{2, 8768, 'Kazuma Kiryu', 41}})
end

pgroup.test_call_pairs_wrong_router = function(g)
    t.assert_error_msg_contains(
        "Space \"locations\" doesn't exist",
        g.call_router_pairs, g, 'locations', {{'=', 'name', 'Sky Finance'}}, {vshard_router = 'customers'})
end

pgroup.test_call_pairs_no_router = function(g)
    t.assert_error_msg_contains(
        "Default vshard group is not found and custom is not specified with opts.vshard_router",
        g.call_router_pairs, g, 'locations', {{'=', 'name', 'Sky Finance'}})
end

pgroup.test_call_pairs_wrong_option = function(g)
    t.assert_error_msg_contains(
        "Invalid opts.vshard_router table value, a vshard router instance has been expected",
        g.call_router_pairs, g, 'locations', {{'=', 'name', 'Sky Finance'}}, {vshard_router = {group = 'locations'}})
end

pgroup.before_test('test_call_truncate', function(g)
    local storage = g.cluster:server('locations-storage-1').net_box
    local storage_result = storage.space['locations']:get{'Sky Finance'}
    t.assert_equals(storage_result, {'Sky Finance', 26826, 'Credit company', 2})

    local storage = g.cluster:server('locations-storage-2').net_box
    local storage_result = storage.space['locations']:get{'Sunflower'}
    t.assert_equals(storage_result, {'Sunflower', 12261, 'Orphanage', 1})
end)

pgroup.test_call_truncate = function(g)
    local result, err = g:call_router_opts2(
        'truncate', 'locations', {vshard_router = 'locations'})

    t.assert_equals(err, nil)
    t.assert_equals(result, true)

    local storage = g.cluster:server('locations-storage-1').net_box
    local storage_result = storage.space['locations']:get{'Sky Finance'}
    t.assert_equals(storage_result, nil)

    local storage = g.cluster:server('locations-storage-2').net_box
    local storage_result = storage.space['locations']:get{'Sunflower'}
    t.assert_equals(storage_result, nil)
end

pgroup.test_call_truncate_wrong_router = function(g)
    local result, err = g:call_router_opts2(
        'truncate', 'customers', {vshard_router = 'locations'})

    t.assert_equals(result, nil)
    t.assert_str_contains(err.err, "Space \"customers\" doesn't exist")
end

pgroup.test_call_truncate_no_router = function(g)
    local result, err = g:call_router_opts2(
        'truncate', 'customers')

    t.assert_equals(result, nil)
    t.assert_str_contains(err.err,
                          "Default vshard group is not found and custom is not specified with opts.vshard_router")
end

pgroup.test_call_truncate_wrong_option = function(g)
    local result, err = g:call_router_opts2(
        'truncate', 'customers', {vshard_router = {group = 'customers'}})

    t.assert_equals(result, nil)
    t.assert_str_contains(err.err,
                          "Invalid opts.vshard_router table value, a vshard router instance has been expected")
end

pgroup.test_call_update_with_default_sharding = function(g)
    local result, err = g:call_router_opts4(
        'update',
        'locations',
        {'Sky Finance'},
        {{'-', 'workers', 1}},
        {vshard_router = 'locations'})

    t.assert_equals(err, nil)
    t.assert_items_equals(result.rows, {{'Sky Finance', 26826, 'Credit company', 1}})
end

pgroup.test_call_update_with_ddl_sharding = function(g)
    local result, err = g:call_router_opts4(
        'update',
        'customers_ddl',
        {2, 'Kazuma Kiryu'},
        {{'+', 'age', 1}},
        {vshard_router = 'customers'})

    t.assert_equals(err, nil)
    t.assert_items_equals(result.rows, {{2, 8768, 'Kazuma Kiryu', 42}})
end

pgroup.test_call_update_wrong_router = function(g)
    local result, err = g:call_router_opts4(
        'update',
        'locations',
        {'Sky Finance'},
        {{'-', 'workers', 1}},
        {vshard_router = 'customers'})

    t.assert_equals(result, nil)
    t.assert_str_contains(err.err, "Space \"locations\" doesn't exist")
end

pgroup.test_call_update_no_router = function(g)
    local result, err = g:call_router_opts4(
        'update',
        'locations',
        {'Sky Finance'},
        {{'-', 'workers', 1}})

    t.assert_equals(result, nil)
    t.assert_str_contains(err.err,
                          "Default vshard group is not found and custom is not specified with opts.vshard_router")
end

pgroup.test_call_update_wrong_option = function(g)
    local result, err = g:call_router_opts4(
        'update',
        'locations',
        {'Sky Finance'},
        {{'-', 'workers', 1}},
        {vshard_router = {group = 'locations'}})

    t.assert_equals(result, nil)
    t.assert_str_contains(err.err,
                          "Invalid opts.vshard_router table value, a vshard router instance has been expected")
end

pgroup.test_call_upsert_with_default_sharding = function(g)
    local result, err = g:call_router_opts4(
        'upsert',
        'customers_ddl',
        {4, box.NULL, 'Taiga Saejima', 45},
        {{'+', 'age', 1}},
        {vshard_router = 'customers'})

    t.assert_equals(err, nil)
    t.assert_items_equals(result.rows, {})

    local storage = g.cluster:server('customers-storage-2').net_box
    local storage_result = storage.space['customers_ddl']:get{4, 'Taiga Saejima'}
    t.assert_equals(storage_result, {4, 4344, 'Taiga Saejima', 45})
end

pgroup.test_call_upsert_with_ddl_sharding = function(g)
    local result, err = g:call_router_opts4(
        'upsert',
        'locations',
        {'Okinawa Penitentiary No. 2', box.NULL, 'Prison', 100},
        {{'-', 'workers', 1}},
        {vshard_router = 'locations'})

    t.assert_equals(err, nil)
    t.assert_items_equals(result.rows, {})

    local storage = g.cluster:server('locations-storage-1').net_box
    local storage_result = storage.space['locations']:get{'Okinawa Penitentiary No. 2'}
    t.assert_equals(storage_result, {'Okinawa Penitentiary No. 2', 19088, 'Prison', 100})
end

pgroup.test_call_upsert_wrong_router = function(g)
    local result, err = g:call_router_opts4(
        'upsert',
        'locations',
        {'Okinawa Penitentiary No. 2', box.NULL, 'Prison', 100},
        {{'-', 'workers', 1}},
        {vshard_router = 'customers'})

    t.assert_equals(result, nil)
    t.assert_str_contains(err.err, "Space \"locations\" doesn't exist")
end

pgroup.test_call_upsert_no_router = function(g)
    local result, err = g:call_router_opts4(
        'upsert',
        'locations',
        {'Okinawa Penitentiary No. 2', box.NULL, 'Prison', 100},
        {{'-', 'workers', 1}})

    t.assert_equals(result, nil)
    t.assert_str_contains(err.err,
                          "Default vshard group is not found and custom is not specified with opts.vshard_router")
end

pgroup.test_call_upsert_wrong_option = function(g)
    local result, err = g:call_router_opts4(
        'upsert',
        'locations',
        {'Okinawa Penitentiary No. 2', box.NULL, 'Prison', 100},
        {{'-', 'workers', 1}},
        {vshard_router = {group = 'locations'}})

    t.assert_equals(result, nil)
    t.assert_str_contains(err.err,
                          "Invalid opts.vshard_router table value, a vshard router instance has been expected")
end

pgroup.test_call_upsert_object_with_default_sharding = function(g)
    local result, err = g:call_router_opts4(
        'upsert_object',
        'customers_ddl',
        {id = 4, bucket_id = box.NULL, name = 'Taiga Saejima', age = 45},
        {{'+', 'age', 1}},
        {vshard_router = 'customers'})

    t.assert_equals(err, nil)
    t.assert_items_equals(result.rows, {})

    local storage = g.cluster:server('customers-storage-2').net_box
    local storage_result = storage.space['customers_ddl']:get{4, 'Taiga Saejima'}
    t.assert_equals(storage_result, {4, 4344, 'Taiga Saejima', 45})
end

pgroup.test_call_upsert_object_with_ddl_sharding = function(g)
    local result, err = g:call_router_opts4(
        'upsert_object',
        'locations',
        {name = 'Okinawa Penitentiary No. 2', bucket_id = box.NULL, type = 'Prison', workers = 100},
        {{'-', 'workers', 1}},
        {vshard_router = 'locations'})

    t.assert_equals(err, nil)
    t.assert_items_equals(result.rows, {})

    local storage = g.cluster:server('locations-storage-1').net_box
    local storage_result = storage.space['locations']:get{'Okinawa Penitentiary No. 2'}
    t.assert_equals(storage_result, {'Okinawa Penitentiary No. 2', 19088, 'Prison', 100})
end

pgroup.test_call_upsert_object_wrong_router = function(g)
    local result, err = g:call_router_opts4(
        'upsert_object',
        'locations',
        {name = 'Okinawa Penitentiary No. 2', bucket_id = box.NULL, type = 'Prison', workers = 100},
        {{'-', 'workers', 1}},
        {vshard_router = 'customers'})

    t.assert_equals(result, nil)
    t.assert_str_contains(err.err, "Space \"locations\" doesn't exist")
end

pgroup.test_call_upsert_object_no_router = function(g)
    local result, err = g:call_router_opts4(
        'upsert_object',
        'locations',
        {name = 'Okinawa Penitentiary No. 2', bucket_id = box.NULL, type = 'Prison', workers = 100},
        {{'-', 'workers', 1}})

    t.assert_equals(result, nil)
    t.assert_str_contains(err.err,
                          "Default vshard group is not found and custom is not specified with opts.vshard_router")
end

pgroup.test_call_upsert_object_wrong_option = function(g)
    local result, err = g:call_router_opts4(
        'upsert_object',
        'locations',
        {name = 'Okinawa Penitentiary No. 2', bucket_id = box.NULL, type = 'Prison', workers = 100},
        {{'-', 'workers', 1}},
        {vshard_router = {group = 'locations'}})

    t.assert_equals(result, nil)
    t.assert_str_contains(err.err,
                          "Invalid opts.vshard_router table value, a vshard router instance has been expected")
end

pgroup.test_call_upsert_many_with_default_sharding = function(g)
    local result, errs = g:call_router_opts3(
        'upsert_many',
        'customers',
        {
            {{3, box.NULL, 'Masayoshi Tanimura', 29}, {{'+', 'age', 1}}},
            {{4, box.NULL, 'Taiga Saejima', 45}, {{'+', 'age', 1}}}
        },
        {vshard_router = 'customers'})

    t.assert_equals(errs, nil)
    t.assert_equals(result.rows, nil)

    local storage = g.cluster:server('customers-storage-2').net_box
    local storage_result = storage.space['customers']:get{3}
    t.assert_equals(storage_result, {3, 11804, 'Masayoshi Tanimura', 29})

    local storage = g.cluster:server('customers-storage-1').net_box
    local storage_result = storage.space['customers']:get{4}
    t.assert_equals(storage_result, {4, 28161, 'Taiga Saejima', 45})
end

pgroup.test_call_upsert_many_with_ddl_sharding = function(g)
    local result, errs = g:call_router_opts3(
        'upsert_many',
        'locations_ddl',
        {
            {{'Tokyo Police Department', box.NULL, 'Police', 40000}, {{'-', 'workers', 1}}},
            {{'Okinawa Penitentiary No. 2', box.NULL, 'Prison', 100}, {{'-', 'workers', 1}}}
        },
        {vshard_router = 'locations'})

    t.assert_equals(errs, nil)
    t.assert_equals(result.rows, nil)

    local storage = g.cluster:server('locations-storage-1').net_box
    local storage_result = storage.space['locations_ddl']:get{'Tokyo Police Department', 'Police'}
    t.assert_equals(storage_result, {'Tokyo Police Department', 28259, 'Police', 40000})

    local storage = g.cluster:server('locations-storage-2').net_box
    local storage_result = storage.space['locations_ddl']:get{'Okinawa Penitentiary No. 2', 'Prison'}
    t.assert_equals(storage_result, {'Okinawa Penitentiary No. 2', 6427, 'Prison', 100})
end

pgroup.test_call_upsert_many_wrong_router = function(g)
    local result, errs = g:call_router_opts3(
        'upsert_many',
        'locations',
        {
            {{'Tokyo Police Department', box.NULL, 'Police', 40000}, {{'-', 'workers', 1}}},
            {{'Okinawa Penitentiary No. 2', box.NULL, 'Prison', 100}, {{'-', 'workers', 1}}}
        },
        {vshard_router = 'customers'})

    t.assert_equals(result, nil)
    t.assert_str_contains(errs[1].err, "Space \"locations\" doesn't exist")
end

pgroup.test_call_upsert_many_no_router = function(g)
    local result, errs = g:call_router_opts3(
        'upsert_many',
        'locations',
        {
            {{'Tokyo Police Department', box.NULL, 'Police', 40000}, {{'-', 'workers', 1}}},
            {{'Okinawa Penitentiary No. 2', box.NULL, 'Prison', 100}, {{'-', 'workers', 1}}}
        })

    t.assert_equals(result, nil)
    t.assert_str_contains(errs[1].err,
                          "Default vshard group is not found and custom is not specified with opts.vshard_router")
end

pgroup.test_call_upsert_many_wrong_option = function(g)
    local result, errs = g:call_router_opts3(
        'upsert_many',
        'locations',
        {
            {{'Tokyo Police Department', box.NULL, 'Police', 40000}, {{'-', 'workers', 1}}},
            {{'Okinawa Penitentiary No. 2', box.NULL, 'Prison', 100}, {{'-', 'workers', 1}}}
        },
        {vshard_router = {group = 'locations'}})

    t.assert_equals(result, nil)
    t.assert_str_contains(errs[1].err,
                          "Invalid opts.vshard_router table value, a vshard router instance has been expected")
end

pgroup.test_call_upsert_object_many_with_default_sharding = function(g)
    local result, errs = g:call_router_opts3(
        'upsert_object_many',
        'locations',
        {
            {
                {name = 'Tokyo Police Department', bucket_id = box.NULL, type = 'Police', workers = 40000},
                {{'-', 'workers', 1}}
            },
            {
                {name = 'Okinawa Penitentiary No. 2', bucket_id = box.NULL, type = 'Prison', workers = 100},
                {{'-', 'workers', 1}}
            }
        },
        {vshard_router = 'locations'}
    )

    t.assert_equals(errs, nil)
    t.assert_equals(result.rows, nil)

    local storage = g.cluster:server('locations-storage-2').net_box
    local storage_result = storage.space['locations']:get{'Tokyo Police Department'}
    t.assert_equals(storage_result, {'Tokyo Police Department', 9017, 'Police', 40000})

    local storage = g.cluster:server('locations-storage-1').net_box
    local storage_result = storage.space['locations']:get{'Okinawa Penitentiary No. 2'}
    t.assert_equals(storage_result, {'Okinawa Penitentiary No. 2', 19088, 'Prison', 100})
end

pgroup.test_call_upsert_object_many_with_ddl_sharding = function(g)
    local result, errs = g:call_router_opts3(
        'upsert_object_many',
        'customers_ddl',
        {
            {{id = 3, bucket_id = box.NULL, name = 'Masayoshi Tanimura', age = 29}, {{'+', 'age', 1}}},
            {{id = 4, bucket_id = box.NULL, name = 'Taiga Saejima', age = 45}, {{'+', 'age', 1}}},
        },
        {vshard_router = 'customers'}
    )

    t.assert_equals(errs, nil)
    t.assert_equals(result.rows, nil)

    local storage = g.cluster:server('customers-storage-2').net_box
    local storage_result = storage.space['customers_ddl']:get{3, 'Masayoshi Tanimura'}
    t.assert_equals(storage_result, {3, 2900, 'Masayoshi Tanimura', 29})

    local storage = g.cluster:server('customers-storage-2').net_box
    local storage_result = storage.space['customers_ddl']:get{4, 'Taiga Saejima'}
    t.assert_equals(storage_result, {4, 4344, 'Taiga Saejima', 45})
end

pgroup.test_call_upsert_object_many_wrong_router = function(g)
    local result, errs = g:call_router_opts3(
        'upsert_object_many',
        'customers',
        {
            {{id = 3, bucket_id = box.NULL, name = 'Masayoshi Tanimura', age = 29}, {{'+', 'age', 1}}},
            {{id = 4, bucket_id = box.NULL, name = 'Taiga Saejima', age = 45}, {{'+', 'age', 1}}},
        },
        {vshard_router = 'locations'})

    t.assert_equals(result, nil)
    t.assert_str_contains(errs[1].err, "Space \"customers\" doesn't exist")
end

pgroup.test_call_upsert_object_many_no_router = function(g)
    local result, errs = g:call_router_opts3(
        'upsert_object_many',
        'customers',
        {
            {{id = 3, bucket_id = box.NULL, name = 'Masayoshi Tanimura', age = 29}, {{'+', 'age', 1}}},
            {{id = 4, bucket_id = box.NULL, name = 'Taiga Saejima', age = 45}, {{'+', 'age', 1}}},
        })

    t.assert_equals(result, nil)
    t.assert_str_contains(errs[1].err,
                          "Default vshard group is not found and custom is not specified with opts.vshard_router")
end

pgroup.test_call_upsert_object_many_wrong_option = function(g)
    local result, errs = g:call_router_opts3(
        'upsert_object_many',
        'customers',
        {
            {{id = 3, bucket_id = box.NULL, name = 'Masayoshi Tanimura', age = 29}, {{'+', 'age', 1}}},
            {{id = 4, bucket_id = box.NULL, name = 'Taiga Saejima', age = 45}, {{'+', 'age', 1}}},
        },
        {vshard_router = {group = 'customers'}})

    t.assert_equals(result, nil)
    t.assert_str_contains(errs[1].err,
                          "Invalid opts.vshard_router table value, a vshard router instance has been expected")
end

pgroup.test_schema = function(g)
    local result, err = g.router:call('crud.schema', {'customers', {vshard_router = 'customers'}})

    t.assert_equals(err, nil)
    t.assert_equals(result, helpers.schema_compatibility({customers = {
        format = {
            {name = "id", type = "unsigned", is_nullable = false},
            {name = "bucket_id", type = "unsigned", is_nullable = true},
            {name = "name", type = "string", is_nullable = false},
            {name = "age", type = "number", is_nullable = false},
        },
        indexes = {
            [0] = {
                id = 0,
                name = "pk",
                parts = {{exclude_null = false, fieldno = 1, is_nullable = false, type = "unsigned"}},
                type = "TREE",
                unique = true,
            },
            [2] = {
                id = 2,
                name = "age",
                parts = {{exclude_null = false, fieldno = 4, is_nullable = false, type = "number"}},
                type = "TREE",
                unique = false,
            },
        },
    }})['customers'])
end

pgroup.test_schema_router_mismatch = function(g)
    local result, err = g.router:call('crud.schema', {'customers', {vshard_router = 'locations'}})

    t.assert_equals(result, nil, err)
    t.assert_str_contains(err.err, "Space \"customers\" doesn't exist")
end
