-- Borrowed from https://github.com/tarantool/tarantool/blob/1a5e3bf3c3badd14ffc37b2b31e47554e4778cde/test/luatest_helpers/server.lua

local fun = require('fun')
local yaml = require('yaml')
local urilib = require('uri')
local fio = require('fio')
local luatest = require('luatest')

local path = require('test.path')
local vclock_utils = require('test.vshard_helpers.vclock')

local WAIT_TIMEOUT = 60
local WAIT_DELAY = 0.1

-- Join paths in an intuitive way.
--
-- If a component is nil, it is skipped.
--
-- If a component is an absolute path, it skips all the previous
-- components.
--
-- The wrapper is written for two components for simplicity.
local function pathjoin(a, b)
    -- No first path -- skip it.
    if a == nil then
        return b
    end
    -- No second path -- skip it.
    if b == nil then
        return a
    end
    -- The absolute path is checked explicitly due to gh-8816.
    if b:startswith('/') then
        return b
    end
    return fio.pathjoin(a, b)
end

local function find_instance(groups, instance_name)
    for _, group in pairs(groups or {}) do
        for _, replicaset in pairs(group.replicasets or {}) do
            local instance = (replicaset.instances or {})[instance_name]

            if instance ~= nil then
                return group, replicaset, instance
            end
        end
    end

    return nil, nil, nil
end

-- Determine advertise URI for given instance from a cluster
-- configuration.
local function find_advertise_uri(config, instance_name, dir)
    if config == nil or next(config) == nil then
        return nil
    end

    -- Determine listen and advertise options that are in effect
    -- for the given instance.
    local advertise = nil
    local listen = nil

    local group, replicaset, instance = find_instance(config.groups, instance_name)

    if instance ~= nil then
        if instance.iproto ~= nil then
            if instance.iproto.advertise ~= nil then
                advertise = advertise or instance.iproto.advertise.client
            end
            listen = listen or instance.iproto.listen
        end
        if replicaset.iproto ~= nil then
            if replicaset.iproto.advertise ~= nil then
                advertise = advertise or replicaset.iproto.advertise.client
            end
            listen = listen or replicaset.iproto.listen
        end
        if group.iproto ~= nil then
            if group.iproto.advertise ~= nil then
                advertise = advertise or group.iproto.advertise.client
            end
            listen = listen or group.iproto.listen
        end
    end

    if config.iproto ~= nil then
        if config.iproto.advertise ~= nil then
            advertise = advertise or config.iproto.advertise.client
        end
        listen = listen or config.iproto.listen
    end

    local uris
    if advertise ~= nil then
        uris = {{uri = advertise}}
    else
        uris = listen
    end

    for _, uri in ipairs(uris or {}) do
        uri = table.copy(uri)
        uri.uri = uri.uri:gsub('{{ *instance_name *}}', instance_name)
        uri.uri = uri.uri:gsub('unix/:%./', ('unix/:%s/'):format(dir))
        local u = urilib.parse(uri)
        if u.ipv4 ~= '0.0.0.0' and u.ipv6 ~= '::' and u.service ~= '0' then
            return uri
        end
    end
    error('No suitable URI to connect is found')
end

local Server = luatest.Server:inherit({})

-- Adds the following options:
--
-- * config_file (string)
--
--   An argument of the `--config <...>` CLI option.
--
--   Used to deduce advertise URI to connect net.box to the
--   instance.
--
--   The special value '' means running without `--config <...>`
--   CLI option (but still pass `--name <alias>`).
-- * remote_config (table)
--
--   If `config_file` is not passed, this config value is used to
--   deduce the advertise URI to connect net.box to the instance.
Server.constructor_checks = fun.chain(Server.constructor_checks, {
    config_file = 'string',
    remote_config = '?table',
}):tomap()

function Server:new(object, extra)
    extra = extra or {}
    extra._tags = {}

    return getmetatable(self).new(self, object, extra)
end

function Server:initialize()
    if self.config_file ~= nil then
        self.command = arg[-1]

        self.args = fun.chain(self.args or {}, {
            '--name', self.alias
        }):totable()

        if self.config_file ~= '' then
            table.insert(self.args, '--config')
            table.insert(self.args, self.config_file)

            -- Take into account self.chdir to calculate a config
            -- file path.
            local config_file_path = pathjoin(self.chdir, self.config_file)

            -- Read the provided config file.
            local fh, err = fio.open(config_file_path, {'O_RDONLY'})
            if fh == nil then
                error(('Unable to open file %q: %s'):format(config_file_path,
                    err))
            end
            self.config = yaml.decode(fh:read())
            fh:close()
        end

        if self.net_box_uri == nil then
            local config = self.config or self.remote_config

            -- NB: listen and advertise URIs are relative to
            -- process.work_dir, which, in turn, is relative to
            -- self.chdir.
            local work_dir
            if config.process ~= nil and config.process.work_dir ~= nil then
                work_dir = config.process.work_dir
            end
            local dir = pathjoin(self.chdir, work_dir)
            self.net_box_uri = find_advertise_uri(config, self.alias, dir)
        end
    end

    self.env = self.env or {}

    if self.env['LUA_PATH'] == nil then
        self.env['LUA_PATH'] = path.LUA_PATH
    end

    getmetatable(getmetatable(self)).initialize(self)
end

function Server:set_tag(name, value)
    self._tags[name] = value
end

function Server:get_tag(name)
    return self._tags[name]
end

function Server:set_storage_tag(value)
    self:set_tag('storage', value)
end

function Server:set_router_tag(value)
    self:set_tag('router', value)
end

function Server:is_storage()
    local is_storage = self:get_tag('storage')
    assert(is_storage ~= nil, 'please, prepare server before using this handle')
    return is_storage
end

function Server:is_router()
    local is_router = self:get_tag('router')
    assert(is_router ~= nil, 'please, prepare server before using this handle')
    return is_router
end

function Server:connect_net_box()
    getmetatable(getmetatable(self)).connect_net_box(self)

    if self.config_file == nil then
        return
    end

    if not self.net_box then
        return
    end

    -- Replace the ready condition.
    local saved_eval = self.net_box.eval
    self.net_box.eval = function(self, expr, args, opts)
        if expr == 'return _G.ready' then
            expr = "return require('config'):info().status == 'ready' or " ..
                          "require('config'):info().status == 'check_warnings'"
        end
        return saved_eval(self, expr, args, opts)
    end
end

function Server:wait_for_rw()
    luatest.helpers.retrying({timeout = WAIT_TIMEOUT, delay = WAIT_DELAY}, function()
        local ro, err = self:exec(function()
            return box.info.ro
        end)

        luatest.assert_equals(err, nil)
        luatest.assert_equals(ro, false)
    end)
end

-- Enable the startup waiting if the advertise URI of the instance
-- is determined.
function Server:start(opts)
    opts = opts or {}
    if self.config_file and opts.wait_until_ready == nil then
        opts.wait_until_ready = self.net_box_uri ~= nil
    end
    getmetatable(getmetatable(self)).start(self, opts)
end

vclock_utils.extend_with_vclock_methods(Server)

return Server
