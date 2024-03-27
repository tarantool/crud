local checks = require('checks')

local utils = require('test.tarantool3_helpers.utils')

local function set_listen_uris_to_instances(groups)
    local port = 3301

    for _, group in pairs(groups) do
        for _, replicaset in pairs(group.replicasets) do
            for _, instance in pairs(replicaset.instances) do
                instance.iproto = {listen = {{uri = ('localhost:%d'):format(port)}}}

                port = port + 1
            end
        end
    end

    return groups
end

local Script = {
    new = function(self)
        local object = {_contents = ''}

        setmetatable(object, self)
        self.__index = self
        return object
    end,
    append = function(self, code)
        self._contents = self._contents .. ("%s;\n"):format(code)
    end,
    append_require_and_run = function(self, to_require, to_run)
        self:append((
            "do\n" ..
            "    local to_require = %q\n" ..
            "    local to_run = %q\n" ..

            "    local module = require(to_require)\n" ..
            "    local method = to_run\n" ..
            "    if module[method] ~= nil then\n" ..
            "        module[method]()\n" ..
            "    end\n" ..
            "end"
        ):format(to_require, to_run))
    end,
    dump = function(self)
        return self._contents
    end,
}

local function generate_modules(storage_entrypoint, router_entrypoint, all_entrypoint)
    local router = Script:new()
    local storage = Script:new()
    local router_and_storage = Script:new()

    if storage_entrypoint ~= nil then
        storage:append_require_and_run(storage_entrypoint, 'init')
        router_and_storage:append_require_and_run(storage_entrypoint, 'init')
    end

    if router_entrypoint ~= nil then
        router:append_require_and_run(router_entrypoint, 'init')
        router_and_storage:append_require_and_run(router_entrypoint, 'init')
    end

    if all_entrypoint ~= nil then
        router:append_require_and_run(all_entrypoint, 'init')
        storage:append_require_and_run(all_entrypoint, 'init')
        router_and_storage:append_require_and_run(all_entrypoint, 'init')
    end

    return {
        ['router'] = router:dump(),
        ['storage'] = storage:dump(),
        ['router_and_storage'] = router_and_storage:dump(),
    }
end

local function generate_wait_until_ready_evals(storage_entrypoint, router_entrypoint, all_entrypoint)
    local router = Script:new()
    local storage = Script:new()

    if storage_entrypoint ~= nil then
        storage:append_require_and_run(storage_entrypoint, 'wait_until_ready')
    end

    if router_entrypoint ~= nil then
        router:append_require_and_run(router_entrypoint, 'wait_until_ready')
    end

    if all_entrypoint ~= nil then
        router:append_require_and_run(all_entrypoint, 'wait_until_ready')
        storage:append_require_and_run(all_entrypoint, 'wait_until_ready')
    end

    return {
        ['router'] = router:dump(),
        ['storage'] = storage:dump(),
    }
end

local function set_app_module(app, is_router, is_storage)
    if is_router and is_storage then
        app.module = 'router_and_storage'
    elseif is_router then
        app.module = 'router'
    elseif is_storage then
        app.module = 'storage'
    end

    return app
end

local function set_modules_to_groups(groups)
    for _, group in pairs(groups) do
        group.app = group.app or {}

        local is_router = utils.is_group_a_sharding_router(group)
        local is_storage = utils.is_group_a_sharding_storage(group)

        group.app = set_app_module(group.app, is_router, is_storage)
    end

    return groups
end

-- Do not deepcopy anything here.
local function new(cfg)
    checks({
        groups = 'table',
        bucket_count = 'number',
        storage_entrypoint = '?string',
        router_entrypoint = '?string',
        all_entrypoint = '?string',
        env = '?table',
    })

    local modules = generate_modules(
        cfg.storage_entrypoint,
        cfg.router_entrypoint,
        cfg.all_entrypoint
    )

    local credentials = {
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
    }

    local iproto = {
        advertise = {
            peer = {
                login = 'replicator',
            },
            sharding = {
                login = 'storage'
            },
        },
    }

    local sharding = {
        bucket_count = cfg.bucket_count,
    }

    local groups = cfg.groups
    groups = set_listen_uris_to_instances(groups)
    groups = set_modules_to_groups(groups)

    local replication = {
        failover = 'manual',
        -- https://github.com/tarantool/tarantool/blob/e01fe8f7144eebc64249ab60a83f656cb4a11dc0/test/config-luatest/cbuilder.lua#L72-L87
        timeout = 0.1,
    }

    local config = {
        credentials = credentials,
        iproto = iproto,
        sharding = sharding,
        groups = groups,
        replication = replication,
    }

    local wait_until_ready_evals = generate_wait_until_ready_evals(
        cfg.storage_entrypoint,
        cfg.router_entrypoint,
        cfg.all_entrypoint
    )

    return {
        config = config,
        modules = modules,
        env = cfg.env,
        router_wait_until_ready = wait_until_ready_evals.router,
        storage_wait_until_ready = wait_until_ready_evals.storage,
    }
end

return {
    new = new,
}
