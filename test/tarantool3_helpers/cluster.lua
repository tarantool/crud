local checks = require('checks')
local json = require('json')
local fun = require('fun')
local yaml = require('yaml')

local t = require('luatest')

local vtest = require('test.vshard_helpers.vtest')
local treegen = require('test.tarantool3_helpers.treegen')
local server_helper = require('test.tarantool3_helpers.server')
local utils = require('test.tarantool3_helpers.utils')

local Cluster = {}

-- Inspired by
-- https://github.com/tarantool/cartridge/blob/b9dc61e61bb85e75b7da7dc8f369867c0d3786c4/cartridge/test-helpers/cluster.lua
-- together with
-- https://github.com/tarantool/tarantool/blob/1a5e3bf3c3badd14ffc37b2b31e47554e4778cde/test/config-luatest/basic_test.lua#L39-L67

function Cluster:inherit(object)
    setmetatable(object, self)
    self.__index = self
    return object
end

function Cluster:new(object)
    checks('table', {
        config = 'table',
        modules = '?table',
        env = '?table',
        crud_init = '?boolean',
        router_wait_until_ready = '?string',
        storage_wait_until_ready = '?string',
    })

    self:inherit(object)
    object:initialize()
    return object
end

local function write_config(dir, config)
    return treegen.write_script(dir, 'config.yaml', yaml.encode(config))
end

function Cluster:initialize()
    self.servers = {}
    self.dirs = {}

    self.treegen = {}
    treegen.init(self.treegen)

    for _, group in pairs(self.config.groups) do
        for _, replicaset in pairs(group.replicasets) do
            local is_router = utils.is_replicaset_a_sharding_router(group, replicaset)
            local is_storage = utils.is_replicaset_a_sharding_storage(group, replicaset)

            for alias, _ in pairs(replicaset.instances) do
                local dir = treegen.prepare_directory(self.treegen, {}, {})

                local config_file = write_config(dir, self.config)

                for name, content in pairs(self.modules or {}) do
                    treegen.write_script(dir, name .. '.lua', content)
                end

                local opts = {config_file = config_file, chdir = dir}

                local server = server_helper:new(fun.chain(opts, {alias = alias}):tomap())

                for k, v in pairs(self.env or {}) do
                    server.env[k] = v
                end

                server:set_router_tag(is_router)
                server:set_storage_tag(is_storage)

                table.insert(self.servers, server)
                self.dirs[server] = dir
            end
        end
    end

    self.main_server = self.servers[1]

    return self
end

function Cluster:set_etalon_bucket_balance()
    local masters = {}
    local etalon_balance = {}
    local replicaset_count = 0

    for _, group in pairs(self.config.groups) do
        for rs_id, rs in pairs(group.replicasets) do
            if not utils.is_replicaset_a_sharding_storage(group, rs) then
                goto continue
            end

            local rs_uuid
            if (rs.database or {}).replicaset_uuid ~= nil then
                rs_uuid = rs.database.replicaset_uuid
            else
                rs_uuid = vtest.replicaset_name_to_uuid(rs_id)
            end

            local leader_id = rs.leader
            assert(leader_id ~= nil, "Only explicit leader is supported now.")

            masters[rs_uuid] = self:server(leader_id)

            local weight = 1 -- Only equal weight is supported now.

            etalon_balance[rs_uuid] = {
                weight = weight,
            }
            replicaset_count = replicaset_count + 1

            ::continue::
        end
    end
    t.assert_not_equals(masters, {}, 'have masters')

    local bucket_count = self.config.sharding.bucket_count
    vtest.distribute_etalon_buckets(etalon_balance, masters, replicaset_count, bucket_count)
end

function Cluster:method_on_replicaset(method, config_replicaset, func, args)
    for alias, _ in pairs(config_replicaset.instances) do
        local server = self:server(alias)
        server[method](server, func, args)
    end
end

function Cluster:exec_on_replicaset(config_replicaset, func, args)
    self:method_on_replicaset('exec', config_replicaset, func, args)
end

function Cluster:eval_on_replicaset(config_replicaset, func, args)
    self:method_on_replicaset('eval', config_replicaset, func, args)
end

local function bootstrap_vshard_router()
    local vshard = require('vshard')
    vshard.router.bootstrap()
end

function Cluster:bootstrap_vshard_routers()
    for _, group in pairs(self.config.groups) do
        for _, rs in pairs(group.replicasets) do
            if utils.is_replicaset_a_sharding_router(group, rs) then
                self:exec_on_replicaset(rs, bootstrap_vshard_router)
            end
        end
    end
end

local function bootstrap_crud_router()
    local crud = require('crud')
    crud.init_router()
end

local function bootstrap_crud_storage()
    local crud = require('crud')
    crud.init_storage{async = true}
end

function Cluster:bootstrap_crud()
    for _, group in pairs(self.config.groups) do
        for _, rs in pairs(group.replicasets) do
            if utils.is_replicaset_a_sharding_router(group, rs) then
                self:exec_on_replicaset(rs, bootstrap_crud_router)
            end

            if utils.is_replicaset_a_sharding_storage(group, rs) then
                self:exec_on_replicaset(rs, bootstrap_crud_storage)
            end
        end
    end
end

function Cluster:wait_for_leaders_rw()
    for _, group in pairs(self.config.groups) do
        for _, rs in pairs(group.replicasets) do
            local leader_id = rs.leader
            local leader = self:server(leader_id)

            leader:wait_for_rw()
        end
    end
end

function Cluster:start()
    for _, server in ipairs(self.servers) do
        server:start({wait_until_ready = false})
    end

    return self:wait_until_ready()
end

function Cluster:wait_until_ready()
    for _, server in ipairs(self.servers) do
        server:wait_until_ready()
    end

    for _, server in ipairs(self.servers) do
        t.assert_equals(server:eval('return box.info.name'), server.alias)
    end

    self:wait_for_leaders_rw()

    self:bootstrap()
    self:wait_until_bootstrap_finished()

    return self
end

function Cluster:bootstrap()
    self:set_etalon_bucket_balance()
    self:bootstrap_vshard_routers()

    if self.crud_init then
        self:bootstrap_crud()
    end

    return self
end

function Cluster:wait_until_bootstrap_finished()
    if self.crud_init then
        self:wait_crud_is_ready_on_cluster()
    end

    self:wait_modules_are_ready_on_cluster()

    return self
end

local function assert_expected_number_of_storages_is_running(router, expected_number)
    local res, err = router:call('crud.storage_info')
    assert(
        err == nil,
        ('crud is not bootstrapped: error on getting storage info: %s'):format(err)
    )

    local running_storages = 0
    for _, storage in pairs(res) do
        if storage.status == 'running' then
            running_storages = running_storages + 1
        end
    end

    assert(
        running_storages == expected_number,
        ('crud is not bootstrapped: expected %d running storages, got the following storage info: %s'):format(
            expected_number, json.encode(res))
    )

    return true
end

function Cluster:wait_crud_is_ready_on_cluster()
    local router = self:get_router()
    local storages_in_topology = self:count_storages()

    local WAIT_TIMEOUT = 5
    local DELAY = 0.1
    t.helpers.retrying(
        {timeout = WAIT_TIMEOUT, delay = DELAY},
        assert_expected_number_of_storages_is_running,
        router, storages_in_topology
    )

    return self
end

function Cluster:get_router()
    for _, server in pairs(self.servers) do
        if server:is_router() then
            return server
        end
    end

    return nil
end

function Cluster:count_storages()
    local storages_in_topology = 0
    for _, server in pairs(self.servers) do
        if server:is_storage() then
            storages_in_topology = storages_in_topology + 1
        end
    end

    return storages_in_topology
end

function Cluster:wait_modules_are_ready_on_cluster()
    for _, group in pairs(self.config.groups) do
        for _, rs in pairs(group.replicasets) do
            if self.router_wait_until_ready ~= nil
            and utils.is_replicaset_a_sharding_router(group, rs) then
                self:eval_on_replicaset(rs, self.router_wait_until_ready)
            end

            if self.storage_wait_until_ready ~= nil
            and utils.is_replicaset_a_sharding_storage(group, rs) then
                self:eval_on_replicaset(rs, self.storage_wait_until_ready)
            end
        end
    end

    return self
end

function Cluster:cfg(new_config)
    if new_config ~= nil then
        self:reload_config(new_config)
    end

    return table.deepcopy(self.config)
end

function Cluster:reload_config(new_config)
    t.assert_equals(new_config.groups, self.config.groups, 'groups reload is not supported yet')

    for _, server in ipairs(self.servers) do
        write_config(self.dirs[server], new_config)
    end

    for _, server in ipairs(self.servers) do
        server:exec(function()
            require('config'):reload()
        end)
    end

    self.config = new_config
end

function Cluster:stop()
    for _, server in ipairs(self.servers) do
        server:stop()
    end

    return self
end

function Cluster:drop()
    self:stop()
    treegen.clean(self.treegen)

    return self
end

function Cluster:server(alias)
    for _, server in ipairs(self.servers) do
        if server.alias == alias then
            return server
        end
    end
    error('Server ' .. alias .. ' not found', 2)
end

return Cluster
