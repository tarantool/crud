require('strict').on()

local t = require('luatest')

local log = require('log')
local checks = require('checks')
local digest = require('digest')
local fio = require('fio')

local crud = require('crud')

if os.getenv('DEV') == nil then
    os.setenv('DEV', 'ON')
end

local helpers = {}

local pgroup = require('test.pgroup')
helpers.pgroup = pgroup

local ok, cartridge_helpers = pcall(require, 'cartridge.test-helpers')
if not ok then
    log.error('Please, install cartridge rock to run tests')
    os.exit(1)
end

for name, value in pairs(cartridge_helpers) do
    helpers[name] = value
end

helpers.project_root = fio.dirname(debug.sourcedir())

local __fio_tempdir = fio.tempdir
fio.tempdir = function(base)
    base = base or os.getenv('TMPDIR')
    if base == nil or base == '/tmp' then
        return __fio_tempdir()
    else
        local random = digest.urandom(9)
        local suffix = digest.base64_encode(random, {urlsafe = true})
        local path = fio.pathjoin(base, 'tmp.cartridge.' .. suffix)
        fio.mktree(path)
        return path
    end
end

function helpers.entrypoint(name)
    local path = fio.pathjoin(
        helpers.project_root,
        'test', 'entrypoint',
        string.format('%s.lua', name)
    )
    if not fio.path.exists(path) then
        error(path .. ': no such entrypoint', 2)
    end
    return path
end

function helpers.table_keys(t)
    checks('table')
    local keys = {}
    for key in pairs(t) do
        table.insert(keys, key)
    end
    return keys
end

function helpers.get_results_list(results_map)
    checks('table')
    local results_list = {}
    for _, res in pairs(results_map) do
        table.insert(results_list, res)
    end
    return results_list
end

function helpers.box_cfg()
    if type(box.cfg) ~= 'function' then
        return
    end

    local tempdir = fio.tempdir()
    box.cfg({
        memtx_dir = tempdir,
        wal_mode = 'none',
    })
    fio.rmtree(tempdir)
end

function helpers.insert_objects(g, space_name, objects)
    local inserted_objects = {}

    for _, obj in ipairs(objects) do
        local result, err = g.cluster.main_server.net_box:call('crud.insert_object', {space_name, obj})

        t.assert_equals(err, nil)

        local objects, err = crud.unflatten_rows(result.rows, result.metadata)
        t.assert_equals(err, nil)
        t.assert_equals(#objects, 1)
        table.insert(inserted_objects, objects[1])
    end

    return inserted_objects
end

function helpers.get_objects_by_idxs(objects, idxs)
    local results = {}
    for _, idx in ipairs(idxs) do
        table.insert(results, objects[idx])
    end
    return results
end

function helpers.stop_cluster(cluster)
    assert(cluster ~= nil)
    cluster:stop()
    fio.rmtree(cluster.datadir)
end

function helpers.drop_space_on_cluster(cluster, space_name)
    assert(cluster ~= nil)
    for _, server in ipairs(cluster.servers) do
        server.net_box:eval([[
            local space_name = ...
            local space = box.space[space_name]
            if space ~= nil and not box.cfg.read_only then
                space:drop()
            end
        ]], {space_name})
    end
end

function helpers.truncate_space_on_cluster(cluster, space_name)
    assert(cluster ~= nil)
    for _, server in ipairs(cluster.servers) do
        server.net_box:eval([[
            local space_name = ...
            local space = box.space[space_name]
            if space ~= nil and not box.cfg.read_only then
                space:truncate()
            end
        ]], {space_name})
    end
end

function helpers.get_test_replicasets()
    return {
        {
            uuid = helpers.uuid('a'),
            alias = 'router',
            roles = { 'crud-router' },
            servers = {
                { instance_uuid = helpers.uuid('a', 1), alias = 'router' },
            },
        },
        {
            uuid = helpers.uuid('b'),
            alias = 's-1',
            roles = { 'customers-storage', 'crud-storage' },
            servers = {
                { instance_uuid = helpers.uuid('b', 1), alias = 's1-master' },
                { instance_uuid = helpers.uuid('b', 2), alias = 's1-replica' },
            },
        },
        {
            uuid = helpers.uuid('c'),
            alias = 's-2',
            roles = { 'customers-storage', 'crud-storage' },
            servers = {
                { instance_uuid = helpers.uuid('c', 1), alias = 's2-master' },
                { instance_uuid = helpers.uuid('c', 2), alias = 's2-replica' },
            },
        }
    }
end

function helpers.call_on_servers(cluster, aliases, func)
    for _, alias in ipairs(aliases) do
        local server = cluster:server(alias)
        func(server)
    end
end

function helpers.assert_ge(actual, expected, message)
    if not (actual >= expected) then
        local err = string.format('expected: %s >= %s', actual, expected)
        if message ~= nil then
            err = message .. '\n' .. err
        end
        error(err, 2)
    end
end

function helpers.get_replicasets_with_equal_key(cluster, key)
    return cluster.main_server.net_box:eval([[
        local vshard = require('vshard')

        local key = ...
        local bucket_id = vshard.router.bucket_id_strcrc32(key)
        local bucket_ids = {bucket_id,}

        local replicasets = vshard.router.routeall()

        local other_replicaset_uuid
        for replicaset_uuid, replicaset in pairs(replicasets) do
            local stat, err = replicaset:callrw('vshard.storage.bucket_stat', {bucket_id})

            if err ~= nil and err.name == 'WRONG_BUCKET' then
                other_replicaset_uuid = replicaset_uuid
                break
            end

            if err ~= nil then
                return nil, string.format(
                    'vshard.storage.bucket_stat returned unexpected error: %s',
                    require('json').encode(err)
                )
            end
        end

        if other_replicaset_uuid == nil then
            return nil, 'Other replicaset is not found'
        end

        local other_replicaset = replicasets[other_replicaset_uuid]
        if other_replicaset == nil then
            return nil, string.format('Replicaset %s not found', other_replicaset_uuid)
        end

        local buckets_info = other_replicaset:callrw('vshard.storage.buckets_info')
        local res_bucket_id = next(buckets_info)

        table.insert(bucket_ids, res_bucket_id)

        return bucket_ids
    ]], {key})
end

return helpers
