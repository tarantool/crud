local fiber = require('fiber')
local checks = require('checks')
local errors = require('errors')
local tarantool = require('tarantool')

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

if (not utils.tarantool_version_at_least(2, 11, 0))
or (tarantool.package ~= 'Tarantool Enterprise') or (not has_merger) then
    return {
        new = function() return nil,
        ReadviewError:new("Tarantool does not support readview") end,
        init = function() return nil end}
end
local select = require('crud.select.compat.select')

local readview = {}


local function readview_open_on_storage(readview_name)
    if not utils.tarantool_version_at_least(2, 11, 0) or
    tarantool.package ~= 'Tarantool Enterprise' then
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

    local replica_info = {}
    replica_info.uuid = box.info().uuid
    replica_info.id = read_view.id

    return replica_info, nil
end

local function readview_close_on_storage(readview_uuid)
    dev_checks('table')

    local list = box.read_view.list()
    local readview_id
    for _, replica_info in pairs(readview_uuid) do
        if replica_info.uuid == box.info().uuid then
            readview_id = replica_info.id
        end
    end

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
        scan_value = 'table',
        after_tuple = '?table',
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
            replica_uuid = box.info().uuid,
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
        return cursor, ReadviewError:new("Space %q doesn't exist", space_name)
    end

    local space = box.space[space_name]
    if space == nil then
        return cursor, ReadviewError:new("Space %q doesn't exist", space_name)
    end
    space_readview.format = space:format()

    local index_readview = space_readview.index[index_id]
    if index_readview == nil then
        return cursor, ReadviewError:new("Index with ID %s doesn't exist", index_id)
    end
    local index = space.index[index_id]
    if index == nil then
        return cursor, ReadviewError:new("Index with ID %s doesn't exist", index_id)
    end

    local _, err = sharding.check_sharding_hash(space_name,
                                                opts.sharding_func_hash,
                                                opts.sharding_key_hash,
                                                opts.skip_sharding_hash_check)

    if err ~= nil then
        return nil, err
    end

    local filter_func, err = select_filters.gen_func(space, conditions, {
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
        return cursor, ReadviewError:new("Failed to execute select: %s", err)
    end

    if resp.tuples_fetched < opts.limit or opts.limit == 0 then
        cursor.is_end = true
    else
        local last_tuple = resp.tuples[#resp.tuples]
        cursor.after_tuple = last_tuple
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
    opts.readview_uuid = self._uuid

    if self.opened == false then
        return nil, ReadviewError:new("Read view is closed")
    end

    return select_call(space_name, user_conditions, opts)
end

local pairs_call = stats.wrap(select.pairs, stats.op.SELECT, {pairs = true})

function Readview_obj:pairs(space_name, user_conditions, opts)
    opts = opts or {}
    opts.readview = true
    opts.readview_uuid = self._uuid

    if self.opened == false then
        return nil, ReadviewError:new("Read view is closed")
    end

    return pairs_call(space_name, user_conditions, opts)
end

function readview.init()
    _G._crud.readview_open_on_storage = readview_open_on_storage
    _G._crud.readview_close_on_storage = readview_close_on_storage
    _G._crud.select_readview_on_storage = select_readview_on_storage
 end

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
    for _, replicaset in pairs(replicasets) do
        for replica_uuid, replica in pairs(replicaset.replicas) do
            for _, value in pairs(self._uuid) do
                if replica_uuid == value.uuid then
                    local replica_result, replica_err = replica.conn:call('_crud.readview_close_on_storage',
                    {self._uuid}, {timeout = opts.timeout})
                    if replica_err ~= nil then
                        table.insert(errors, ReadviewError:new("Failed to close Readview on storage: %s", replica_err))
                    end
                    if replica_err == nil and (not replica_result) then
                        table.insert(errors, ReadviewError:new("Readview was not found on storage: %s", replica_uuid))
                    end
                end
            end
        end
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
    setmetatable(readview, Readview_obj)
    readview._name = opts.name
    local results, err, err_uuid = vshard_router:map_callrw('_crud.readview_open_on_storage',
        {readview._name}, {timeout = opts.timeout})
    if err ~= nil then
        return nil,
        ReadviewError:new("Failed to call readview_open_on_storage on storage-side: storage uuid: %s err: %s",
        err_uuid, err)
    end

    local uuid = {}
    local errors = {}
    for _, replicaset_results in pairs(results) do
        for _, replica_result in pairs(replicaset_results) do
            table.insert(uuid, replica_result)
        end
    end

    readview._uuid = uuid
    readview.opened = true

    if next(errors) ~= nil then
        return nil, errors
    end
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
