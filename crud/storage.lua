local checks = require('checks')

local dev_checks = require('crud.common.dev_checks')
local stash = require('crud.common.stash')
local utils = require('crud.common.utils')

local sharding_metadata = require('crud.common.sharding.sharding_metadata')
local insert = require('crud.insert')
local insert_many = require('crud.insert_many')
local replace = require('crud.replace')
local replace_many = require('crud.replace_many')
local get = require('crud.get')
local update = require('crud.update')
local upsert = require('crud.upsert')
local upsert_many = require('crud.upsert_many')
local delete = require('crud.delete')
local select = require('crud.select')
local truncate = require('crud.truncate')
local len = require('crud.len')
local count = require('crud.count')
local borders = require('crud.borders')
local readview = require('crud.readview')
local storage_info = require('crud.storage_info')

local storage = {}

local internal_stash = stash.get(stash.name.storage_init)

local function init_local_part(_, name, func)
    rawset(_G[utils.STORAGE_NAMESPACE], name, func)
end

local function init_persistent_part(user, name, _)
    name = utils.get_storage_call(name)
    box.schema.func.create(name, {setuid = true, if_not_exists = true})
    box.schema.user.grant(user, 'execute', 'function', name, {if_not_exists = true})
end

--- Initializes a storage function by its name.
--
--  It adds the function into the global scope by its name and required
--  access to a vshard storage user.
--
--  @function init_storage_call
--
--  @param string name of a user.
--  @param string name a name of the function.
--  @param function func the function.
--
--  @return nil
local function init_storage_call(user, storage_api)
    dev_checks('?string', 'table')

    for name, func in pairs(storage_api) do
        init_local_part(user, name, func)

        if user ~= nil then
            init_persistent_part(user, name, func)
        end
    end
end

local modules_with_storage_api = {
    sharding_metadata,
    insert,
    insert_many,
    get,
    replace,
    replace_many,
    update,
    upsert,
    upsert_many,
    delete,
    select,
    truncate,
    len,
    count,
    borders,
    readview,
    -- Must be initialized last: properly working storage info is the flag
    -- of initialization success.
    storage_info,
}

local DEFAULT_ASYNC
if utils.is_tarantool_3() then
    DEFAULT_ASYNC = true
else
    DEFAULT_ASYNC = false
end

local function init_impl()
    rawset(_G, utils.STORAGE_NAMESPACE, {})

    -- User is required only for persistent part of the init.
    -- vshard may not yet be properly set up in cartridge on replicas,
    -- see [1] CI fails
    -- https://github.com/tarantool/crud/actions/runs/8432361330/job/23091298092?pr=417
    local user = nil
    if not box.info.ro then
        user = utils.get_this_replica_user() or 'guest'
    end

    for _, module in ipairs(modules_with_storage_api) do
        init_storage_call(user, module.storage_api)
    end
end

function storage.init(opts)
    checks({async = '?boolean'})

    opts = opts or {}

    if opts.async == nil then
        opts.async = DEFAULT_ASYNC
    end

    if type(box.cfg) ~= 'table' then
        error('box.cfg() must be called first')
    end

    if internal_stash.watcher ~= nil then
        internal_stash.watcher:unregister()
        internal_stash.watcher = nil
    end

    if opts.async then
        assert(utils.tarantool_supports_box_watch(),
               'async start is supported only for Tarantool versions with box.watch support')

        internal_stash.watcher = box.watch('box.status', init_impl)
    else
        init_impl()
    end
end

function storage.stop()
    if internal_stash.watcher ~= nil then
        internal_stash.watcher:unregister()
        internal_stash.watcher = nil
    end

    rawset(_G, utils.STORAGE_NAMESPACE, nil)
end

return storage
