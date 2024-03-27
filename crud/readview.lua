local fiber = require('fiber')
local checks = require('checks')
local errors = require('errors')

local const = require('crud.common.const')
local stash = require('crud.common.stash')
local utils = require('crud.common.utils')
local sharding = require('crud.common.sharding')
local select_executor = require('crud.select.executor')
local select_filters = require('crud.compare.filters')
local dev_checks = require('crud.common.dev_checks')
local schema = require('crud.common.schema')
local stats = require('crud.stats')

local ReadviewError = errors.new_class('ReadviewError', {capture_stack = false})

local has_merger = (utils.tarantool_supports_external_merger() and
    package.search('tuple.merger')) or utils.tarantool_has_builtin_merger()

local OPEN_FUNC_NAME = 'readview_open_on_storage'
local CRUD_OPEN_FUNC_NAME = utils.get_storage_call(OPEN_FUNC_NAME)
local SELECT_FUNC_NAME = 'select_readview_on_storage'
local CLOSE_FUNC_NAME = 'readview_close_on_storage'
local CRUD_CLOSE_FUNC_NAME = utils.get_storage_call(CLOSE_FUNC_NAME)

if (not utils.tarantool_version_at_least(2, 11, 0))
or (not utils.is_enterprise_package()) or (not has_merger) then
    return {
        new = function()
            return nil, ReadviewError:new("Tarantool does not support readview")
        end,
        storage_api = {},
    }
end
local select = require('crud.select.compat.select')

local readview = {}


local function readview_open_on_storage(readview_name)
    if not utils.tarantool_version_at_least(2, 11, 0) or
    not utils.is_enterprise_package() then
        ReadviewError:assert(false, ("Tarantool does not support readview"))
    end
    -- We store readview in stash because otherwise gc will delete it.
    -- e.g master switch.
    local read_view = box.read_view.open({name = readview_name})
    local stash_readview = stash.get(stash.name.storage_readview)
    stash_readview[read_view.id] = read_view

    if read_view == nil then
        ReadviewError:assert(false, ("Error creating readview"))
    end

    return {
        id = read_view.id,

        uuid = box.info().uuid, -- Backward compatibility.
        replica_id = utils.get_self_vshard_replica_id(), -- Replacement for uuid.
    }
end

local function readview_close_on_storage(info)
    dev_checks('table')

    local replica_id = utils.get_self_vshard_replica_id()

    local readview_id
    for _, replica_info in pairs(info) do
        local found = false

        if replica_info.replica_id == replica_id then
            found = true
        elseif replica_info.uuid == box.info().uuid then -- Backward compatibility.
            found = true
        end

        if found then
            readview_id = replica_info.id
        end
    end

    local list = box.read_view.list()
    for k,v in pairs(list) do
        if v.id == readview_id then
            list[k]:close()
            local stash_readview = stash.get(stash.name.storage_readview)
            stash_readview[readview_id] = nil
            return true
        end
    end

    return false
end

local function select_readview_on_storage(space_name, index_id, conditions, opts)
    dev_checks('string', 'number', '?table', {
        scan_value = 'table|cdata',
        after_tuple = '?table|cdata',
        tarantool_iter = 'number',
        limit = 'number',
        scan_condition_num = '?number',
        field_names = '?table',
        sharding_key_hash = '?number',
        sharding_func_hash = '?number',
        skip_sharding_hash_check = '?boolean',
        yield_every = '?number',
        fetch_latest_metadata = '?boolean',
        readview_id = 'number',
    })

    local cursor = {}
    if opts.fetch_latest_metadata then
        local replica_schema_version
        if box.info.schema_version ~= nil then
            replica_schema_version = box.info.schema_version
        else
            replica_schema_version = box.internal.schema_version()
        end
        cursor.storage_info = {
            replica_uuid = box.info().uuid, -- Backward compatibility.
            replica_id = utils.get_self_vshard_replica_id(), -- Replacement for replica_uuid.
            replica_schema_version = replica_schema_version,
        }
    end

    local list = box.read_view.list()
    local space_readview

    for k,v in pairs(list) do
        if v.id == opts.readview_id then
            space_readview = list[k].space[space_name]
        end
    end

    if space_readview == nil then
        return ReadviewError:assert(false, "Space %q doesn't exist", space_name)
    end

    local space = box.space[space_name]
    if space == nil then
        return ReadviewError:assert(false, "Space %q doesn't exist", space_name)
    end
    space_readview.format = space:format()

    local index_readview = space_readview.index[index_id]
    if index_readview == nil then
        return ReadviewError:assert(false, "Index with ID %s doesn't exist", index_id)
    end
    local index = space.index[index_id]
    if index == nil then
        return ReadviewError:assert(false, "Index with ID %s doesn't exist", index_id)
    end

    local _, err = sharding.check_sharding_hash(space_name,
                                                opts.sharding_func_hash,
                                                opts.sharding_key_hash,
                                                opts.skip_sharding_hash_check)

    if err ~= nil then
        return nil, err
    end

    local filter_func, err = select_filters.gen_func(space, index, conditions, {
        tarantool_iter = opts.tarantool_iter,
        scan_condition_num = opts.scan_condition_num,
    })
    if err ~= nil then
        return cursor, ReadviewError:new("Failed to generate tuples filter: %s", err)
    end

    -- execute select
    local resp, err = select_executor.execute(space, index, filter_func, {
        scan_value = opts.scan_value,
        after_tuple = opts.after_tuple,
        tarantool_iter = opts.tarantool_iter,
        limit = opts.limit,
        yield_every = opts.yield_every,
        readview = true,
        readview_index = index_readview,
    })
    if err ~= nil then
        return ReadviewError:assert(false, "Failed to execute select: %s", err)
    end

    if resp.tuples_fetched < opts.limit or opts.limit == 0 then
        cursor.is_end = true
    else
        local last_tuple = resp.tuples[#resp.tuples]
        cursor.after_tuple = last_tuple:totable()
    end

    cursor.stats = {
        tuples_lookup = resp.tuples_lookup,
        tuples_fetched = resp.tuples_fetched,
    }

    -- getting tuples with user defined fields (if `fields` option is specified)
    -- and fields that are needed for comparison on router (primary key + scan key)
    local filtered_tuples = schema.filter_tuples_fields(resp.tuples, opts.field_names)

    local result = {cursor, filtered_tuples}

    local select_module_compat_info = stash.get(stash.name.select_module_compat_info)
    if not select_module_compat_info.has_merger then
        if opts.fetch_latest_metadata then
            result[3] = cursor.storage_info.replica_schema_version
        end
    end

    return unpack(result)
end

local Readview_obj = {}
Readview_obj.__index = Readview_obj

local select_call = stats.wrap(select.call, stats.op.SELECT)

function Readview_obj:select(space_name, user_conditions, opts)
    opts = opts or {}
    opts.readview = true
    opts.readview_info = self._info

    if self.opened == false then
        return nil, ReadviewError:new("Read view is closed")
    end

    return select_call(space_name, user_conditions, opts)
end

local pairs_call = stats.wrap(select.pairs, stats.op.SELECT, {pairs = true})

function Readview_obj:pairs(space_name, user_conditions, opts)
    opts = opts or {}
    opts.readview = true
    opts.readview_info = self._info

    if self.opened == false then
        return nil, ReadviewError:new("Read view is closed")
    end

    return pairs_call(space_name, user_conditions, opts)
end

readview.storage_api = {
    [OPEN_FUNC_NAME] = readview_open_on_storage,
    [CLOSE_FUNC_NAME] = readview_close_on_storage,
    [SELECT_FUNC_NAME] = select_readview_on_storage,
}

function Readview_obj:close(opts)
    checks('table', {
        timeout = '?number',
    })
    opts = opts or {}
    if self.opened == false then
        return
    end

    local vshard_router, err = utils.get_vshard_router_instance(nil)
    if err ~= nil then
        return ReadviewError:new(err)
    end

    local replicasets, err = vshard_router:routeall()
    if err ~= nil then
        return ReadviewError:new(err)
    end

    if opts.timeout == nil then
        opts.timeout = const.DEFAULT_VSHARD_CALL_TIMEOUT
    end

    local errors = {}
    for replicaset_id, replicaset in pairs(replicasets) do
        local replicaset_info = self._info[replicaset_id]

        if replicaset_info == nil then
            goto next_replicaset
        end

        for replica_id, replica in pairs(replicaset.replicas) do
            local found = false

            if replicaset_info.replica_id == replica_id then
                found = true
            elseif replicaset_info.uuid == replica.uuid then -- Backward compatibility.
                found = true
            end

            if not found then
                goto next_replica
            end

            local replica_result, replica_err = replica.conn:call(CRUD_CLOSE_FUNC_NAME,
                {self._info}, {timeout = opts.timeout})
            if replica_err ~= nil then
                table.insert(errors, ReadviewError:new("Failed to close Readview on storage: %s", replica_err))
            end
            if replica_err == nil and (not replica_result) then
                table.insert(errors, ReadviewError:new("Readview was not found on storage: %s", replica_id))
            end

            ::next_replica::
        end

        ::next_replicaset::
    end

    if next(errors) ~= nil then
        return errors
    end

    self.opened = false
    return nil

end

function Readview_obj:__gc()
    fiber.new(self.close, self)
end

function Readview_obj.create(vshard_router, opts)
    local readview = {}

    -- For tarantool lua (and lua 5.1) __gc metamethod only works for cdata types.
    -- So in order to create a proper GC hook, we need to create cdata with
    -- __gc call.
    -- __gc call for this cdata will be a __gc call for our readview.
    -- https://github.com/tarantool/tarantool/issues/5770
    local proxy = newproxy(true)
    getmetatable(proxy).__gc = function(_) Readview_obj.__gc(readview) end
    readview[proxy] = true
    setmetatable(readview, Readview_obj)

    readview._name = opts.name
    local results, err, err_id = vshard_router:map_callrw(CRUD_OPEN_FUNC_NAME,
        {readview._name}, {timeout = opts.timeout})
    if err ~= nil then
        return nil, ReadviewError:new(
            "Failed to call readview_open_on_storage on storage-side: storage id: %s err: %s",
            err_id, err
        )
    end

    -- map_callrw response format:
    -- {replicaset_id1 = {res1}, replicaset_id2 = {res2}, ...}
    local info = {}
    for replicaset_id, replicaset_results in pairs(results) do
        local _, replica_info = next(replicaset_results)
        info[replicaset_id] = replica_info
    end

    readview._info = info
    readview.opened = true

    return readview, nil
 end

function readview.new(opts)
    checks({
        name = '?string',
        timeout = '?number',
    })
    opts = opts or {}
    local vshard_router, err = utils.get_vshard_router_instance(nil)
    if err ~= nil then
        return nil, ReadviewError:new(err)
    end

    return Readview_obj.create(vshard_router, opts)
end


return readview
