#!/usr/bin/env tarantool

-- How to run:
--
-- $ ./doc/playground.lua
--
-- Or
--
-- $ KEEP_DATA=1 ./doc/playground.lua
--
-- What to do next:
--
-- Choose an example from README.md, doc/select.md or doc/pairs.md
-- and run. For example:
--
-- tarantool> crud.select('customers', {{'<=', 'age', 35}}, {first = 10})
-- tarantool> crud.select('developers', nil, {first = 6})

local fio = require('fio')
local console = require('console')
local vshard = require('vshard')
local crud = require('crud')

-- Trick to don't leave *.snap, *.xlog files. See
-- test/tuple_keydef.test.lua in the tuple-keydef module.
if os.getenv('KEEP_DATA') ~= nil then
    box.cfg()
else
    local tempdir = fio.tempdir()
    box.cfg({
        memtx_dir = tempdir,
        wal_mode = 'none',
    })
    fio.rmtree(tempdir)
end

local replicaset_uuid
if box.info().replicaset ~= nil then
    replicaset_uuid = box.info().replicaset.uuid
else
    replicaset_uuid = box.info().cluster.uuid
end

-- Setup vshard.
_G.vshard = vshard
local uri = 'guest@localhost:3301'
local cfg = {
    bucket_count = 3000,
    sharding = {
        [replicaset_uuid] = {
            replicas = {
                [box.info().uuid] = {
                    uri = uri,
                    name = 'storage',
                    master = true,
                },
            },
        },
    },
}
vshard.storage.cfg(cfg, box.info().uuid)
vshard.router.cfg(cfg)
vshard.router.bootstrap()

-- Create the 'customers' space.
box.once('customers', function()
    box.schema.create_space('customers', {
        format = {
            {name = 'id', type = 'unsigned'},
            {name = 'bucket_id', type = 'unsigned'},
            {name = 'name', type = 'string'},
            {name = 'age', type = 'number'},
        }
    })
    box.space.customers:create_index('primary_index', {
        parts = {
            {field = 1, type = 'unsigned'},
        },
    })
    box.space.customers:create_index('bucket_id', {
        parts = {
            {field = 2, type = 'unsigned'},
        },
        unique = false,
    })
    box.space.customers:create_index('age', {
        parts = {
            {field = 4, type = 'number'},
        },
        unique = false,
    })

    -- Fill the space.
    box.space.customers:insert({1, 477, 'Elizabeth', 12})
    box.space.customers:insert({2, 401, 'Mary', 46})
    box.space.customers:insert({3, 2804, 'David', 33})
    box.space.customers:insert({4, 1161, 'William', 81})
    box.space.customers:insert({5, 1172, 'Jack', 35})
    box.space.customers:insert({6, 1064, 'William', 25})
    box.space.customers:insert({7, 693, 'Elizabeth', 18})
end)

-- Create the developers space.
box.once('developers', function()
    box.schema.create_space('developers', {
        format = {
            {name = 'id', type = 'unsigned'},
            {name = 'bucket_id', type = 'unsigned'},
            {name = 'name', type = 'string'},
            {name = 'surname', type = 'string'},
            {name = 'age', type = 'number'},
        }
    })
    box.space.developers:create_index('primary_index', {
        parts = {
            {field = 1, type = 'unsigned'},
        },
    })
    box.space.developers:create_index('bucket_id', {
        parts = {
            {field = 2, type = 'unsigned'},
        },
        unique = false,
    })
    box.space.developers:create_index('age_index', {
        parts = {
            {field = 5, type = 'number'},
        },
        unique = false,
    })
    box.space.developers:create_index('full_name', {
        parts = {
            {field = 3, type = 'string'},
            {field = 4, type = 'string'},
        },
        unique = false,
    })

    -- Fill the space.
    box.space.developers:insert({1, 477, 'Alexey', 'Adams', 20})
    box.space.developers:insert({2, 401, 'Sergey', 'Allred', 21})
    box.space.developers:insert({3, 2804, 'Pavel', 'Adams', 27})
    box.space.developers:insert({4, 1161, 'Mikhail', 'Liston', 51})
    box.space.developers:insert({5, 1172, 'Dmitry', 'Jacobi', 16})
    box.space.developers:insert({6, 1064, 'Alexey', 'Sidorov', 31})
end)

-- Initialize crud.
crud.init_storage()
crud.init_router()

-- Start a console.
console.start()
os.exit()
