local t = require('luatest')

local helpers = require('test.helper')
local cluster_helpers = require('test.tarantool3_helpers.cluster')

local g = t.group()

g.before_all(function(cg)
    helpers.skip_if_tarantool3_crud_roles_unsupported()

    local config = {
        credentials = {
            users = {
                guest = {
                    roles = {'super'},
                },
                replicator = {
                    password = 'replicating',
                    roles = {'replication'},
                },
                storage = {
                    password = 'storing-buckets',
                    roles = {'sharding'},
                },
            },
        },
        iproto = {
            advertise = {
                peer = {
                    login = 'replicator',
                },
                sharding = {
                    login = 'storage',
                },
            },
        },
        sharding = {
            bucket_count = 3000,
        },
        groups = {
            routers = {
                sharding = {
                    roles = {'router'},
                },
                roles = {'roles.crud-router'},
                replicasets = {
                    ['router'] = {
                        leader = 'router',
                        instances = {
                            ['router'] = {
                                iproto = {
                                    listen = {{uri = 'localhost:3301'}},
                                },
                            },
                        },
                    },
                },
            },
            storages = {
                app = {
                    module = 'storage',
                },
                sharding = {
                    roles = {'storage'},
                },
                roles = {'roles.crud-storage'},
                replicasets = {
                    ['s-1'] = {
                        leader = 's1-master',
                        instances = {
                            ['s1-master'] = {
                                iproto = {
                                    listen = {{uri = 'localhost:3302'}},
                                },
                            },
                            ['s1-replica'] = {
                                iproto = {
                                    listen = {{uri = 'localhost:3303'}},
                                },
                            },
                        },
                    },
                    ['s-2'] = {
                        leader = 's2-master',
                        instances = {
                            ['s2-master'] = {
                                iproto = {
                                    listen = {{uri = 'localhost:3304'}},
                                },
                            },
                            ['s2-replica'] = {
                                iproto = {
                                    listen = {{uri = 'localhost:3305'}},
                                },
                            },
                        },
                    },
                },
            },
        },
        replication = {
            failover = 'manual',
        },
    }

    cg.cluster = cluster_helpers:new({
        config = config,
        modules = {
            storage = [[
                box.watch('box.status', function()
                    if box.info.ro then
                        return
                    end

                    local customers_space = box.schema.space.create('customers', {
                        format = {
                            {name = 'id', type = 'unsigned'},
                            {name = 'bucket_id', type = 'unsigned'},
                            {name = 'name', type = 'string'},
                            {name = 'age', type = 'number'},
                        },
                        if_not_exists = true,
                        engine = 'memtx',
                    })

                    customers_space:create_index('id', {
                        parts = {{field = 'id'}},
                        if_not_exists = true,
                    })

                    customers_space:create_index('bucket_id', {
                        parts = {{field = 'bucket_id'}},
                        unique = false,
                        if_not_exists = true,
                    })

                    box.schema.func.create('_is_schema_ready', {
                        language = 'LUA',
                        body = 'function() return true end',
                        if_not_exists = true,
                    })
                end)
            ]],
        },
        storage_wait_until_ready = [[
            local clock = require('clock')
            local fiber = require('fiber')

            local TIMEOUT = 60
            local start = clock.monotonic()

            while clock.monotonic() - start < TIMEOUT do
                local status, ready = pcall(box.schema.func.call, '_is_schema_ready')
                if status and ready then
                    return true
                end

                fiber.sleep(0.05)
            end

            error('timeout while waiting for storage bootstrap')
        ]],
    })

    cg.cluster:start()
end)

g.after_all(function(cg)
    cg.cluster:drop()
end)

g.test_router_provides_crud_version = function(cg)
    local result, err = cg.cluster:server('router'):exec(function()
        local crud = require('crud')
        return crud._VERSION
    end)
    t.assert_equals(err, nil)
    t.assert_equals(result, require('crud')._VERSION)
end

g.test_router_tags = function(cg)
    local router = cg.cluster:server('router')
    t.assert_equals(router:is_router(), true)
    t.assert_equals(router:is_storage(), false)
end

g.test_storage_tags = function(cg)
    local storage = cg.cluster:server('s1-master')
    t.assert_equals(storage:is_router(), false)
    t.assert_equals(storage:is_storage(), true)
end

g.test_cluster_supports_basic_insert_get_object = function(cg)
    cg.cluster:server('router'):exec(function()
        local crud = require('crud')

        local _, err = crud.insert_object('customers',
            {id = 1, name = 'Vincent Brooks', age = 32},
            {noreturn = true}
        )
        t.assert_equals(err, nil)

        local result, err = crud.get('customers', 1, {mode = 'write'})
        t.assert_equals(err, nil)
        t.assert_equals(#result.rows, 1, 'Tuple found')

        local objects, err = crud.unflatten_rows(result.rows, result.metadata)
        t.assert_equals(err, nil)
        t.assert_equals(objects[1].id, 1)
        t.assert_equals(objects[1].name, 'Vincent Brooks')
        t.assert_equals(objects[1].age, 32)
    end)
end
