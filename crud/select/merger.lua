local buffer = require('buffer')
local errors = require('errors')
local msgpack = require('msgpack')
local ffi = require('ffi')
local call = require('crud.common.call')
local fiber = require('fiber')
local sharding = require('crud.common.sharding')
local sharding_metadata_module = require('crud.common.sharding.sharding_metadata')

local compat = require('crud.common.compat')
local merger_lib = compat.require('tuple.merger', 'merger')

local Keydef = require('crud.compare.keydef')
local stats = require('crud.stats')
local utils = require("crud.common.utils")

local function bswap_u16(num)
    return bit.rshift(bit.bswap(tonumber(num)), 16)
end

-- See
-- https://github.com/tarantool/tarantool/blob/0ab21ac9eeaaae2aa0aef5e598d374669f96df9e/src/lua/msgpackffi.lua
-- to understand following hell
-- This code works for ALL Tarantool versions
local strict_alignment = (jit.arch == 'arm')
local uint16_ptr_t = ffi.typeof('uint16_t *')
local uint32_ptr_t = ffi.typeof('uint32_t *')
local char_ptr = ffi.typeof('char *')

local decode_u16
local decode_u32
if strict_alignment then
    local tmpint = ffi.new('union tmpint[1]')
    decode_u16 = function(data)
        ffi.copy(tmpint, data[0], 2)
        data[0] = data[0] + 2
        return tonumber(bswap_u16(tmpint[0].u16))
    end
    decode_u32 = function(data)
        ffi.copy(tmpint, data[0], 4)
        data[0] = data[0] + 4
        return tonumber(
            ffi.cast('uint32_t', bit.bswap(tonumber(tmpint[0].u32))))
    end
else
    decode_u16 = function(data)
        local num = bswap_u16(ffi.cast(uint16_ptr_t, data[0])[0])
        data[0] = data[0] + 2
        return tonumber(num)
    end
    decode_u32 = function(data)
        local num = ffi.cast('uint32_t',
            bit.bswap(tonumber(ffi.cast(uint32_ptr_t, data[0])[0])))
        data[0] = data[0] + 4
        return tonumber(num)
    end
end

local data = ffi.new('const unsigned char *[1]')

local function decode_response_array_header()
    local c = data[0][0]
    data[0] = data[0] + 1
    if c == 0xdc then
        decode_u16(data)
    elseif c == 0xdd then
        decode_u32(data)
    end

    return ffi.cast(char_ptr, data[0])
end

local function decode_response_headers(buf)
    data[0] = buf.rpos

    if not utils.tarantool_supports_netbox_skip_header_option() then
        -- {48: [cursor, [tuple_1, tuple_2, ...]]} (exactly 1 pair of key-value)

        -- 48 (key)
        data[0] = data[0] + 1

        -- [cursor, [tuple_1, tuple_2, ...]] (value)
        data[0] = data[0] + 1
    end

    return decode_response_array_header()
end

local function decode_metainfo(buf)
    -- Skip an array around a call return values.
    buf.rpos = decode_response_headers(buf)

    -- Decode a first return value (metainfo).
    local res, err
    res, buf.rpos = msgpack.decode(buf.rpos, buf:size())

    -- If res is nil, decode second return value (error).
    if res == nil then
        err, buf.rpos = msgpack.decode(buf.rpos, buf:size())
    end
    return res, err
end

--- Wait for a data chunk and request for the next data chunk.
local function fetch_chunk(context, state)
    local net_box_opts = context.net_box_opts
    local buf = context.buffer
    local func_name = context.func_name
    local func_args = context.func_args
    local replicaset = context.replicaset
    local vshard_call_name = context.vshard_call_name
    local timeout = context.timeout or call.DEFAULT_VSHARD_CALL_TIMEOUT
    local space_name = context.space_name
    local vshard_router = context.vshard_router
    local future = state.future

    -- The source was entirely drained.
    if future == nil then
        return nil
    end

    -- Wait for requested data.
    local res, err = future:wait_result(timeout)
    if res == nil then
        local wrapped_err = errors.wrap(utils.update_storage_call_error_description(err, func_name, replicaset.uuid))
        error(wrapped_err)
    end

    -- Decode metainfo, leave data to be processed by the merger.
    local cursor, err = decode_metainfo(buf)
    if cursor == nil then
        -- Wrap net.box errors error to restore metatable.
        local wrapped_err = errors.wrap(err)

        if sharding.result_needs_sharding_reload(err) then
            sharding_metadata_module.reload_sharding_cache(vshard_router, space_name)
        end

        error(wrapped_err)
    end

    if context.fetch_latest_metadata then
        if fiber.self().storage.storages_info_on_select == nil then
            fiber.self().storage.storages_info_on_select = {}
        end
        local replica_uuid = cursor.storage_info.replica_uuid
        local replica_schema_version = cursor.storage_info.replica_schema_version
        local fiber_storage = fiber.self().storage.storages_info_on_select
        fiber_storage[replica_uuid] = {}
        fiber_storage[replica_uuid].replica_schema_version = replica_schema_version
    end

    -- Extract stats info.
    -- Stats extracted with callback here and not passed
    -- outside to wrapper because fetch for pairs can be
    -- called even after pairs() return from generators.
    if cursor.stats ~= nil then
        stats.update_fetch_stats(cursor.stats, space_name)
    end

    local next_state = {}

    -- Check whether we need the next call.
    if cursor.is_end then
        return next_state, buf
    end

    -- Request the next data while we processing the current ones.
    -- Note: We reuse the same buffer for all request to a replicaset.
    local next_func_args = func_args

    -- change context.func_args too, but it does not matter
    next_func_args[4].after_tuple = cursor.after_tuple

    if context.readview then
        next_state = {future = context.future_replica.conn:call(func_name, next_func_args, net_box_opts)}
    else
        local next_future = replicaset[vshard_call_name](replicaset, func_name, next_func_args, net_box_opts)
        next_state = {future = next_future}
    end
    return next_state, buf
end

local reverse_tarantool_iters = {
    [box.index.LE] = true,
    [box.index.LT] = true,
    [box.index.REQ] = true,
}

local function new(vshard_router, replicasets, space, index_id, func_name, func_args, opts)
    opts = opts or {}
    local call_opts = opts.call_opts
    local mode = call_opts.mode or 'read'
    local vshard_call_name = call.get_vshard_call_name(mode, call_opts.prefer_replica, call_opts.balance)

    -- Request a first data chunk and create merger sources.
    local merger_sources = {}
    for _, replicaset in pairs(replicasets) do
        -- Perform a request.
        local buf = buffer.ibuf()
        local net_box_opts = {is_async = true, buffer = buf,
                              skip_header = utils.tarantool_supports_netbox_skip_header_option() or nil}
        local future = replicaset[vshard_call_name](replicaset, func_name, func_args,
                net_box_opts)

        -- Create a source.
        local context = {
            net_box_opts = net_box_opts,
            buffer = buf,
            func_name = func_name,
            func_args = func_args,
            replicaset = replicaset,
            vshard_call_name = vshard_call_name,
            timeout = call_opts.timeout,
            fetch_latest_metadata = call_opts.fetch_latest_metadata,
            space_name = space.name,
            vshard_router = vshard_router,
            readview = false,
        }

        local state = {future = future}
        local source = merger_lib.new_buffer_source(fetch_chunk, context, state)
        table.insert(merger_sources, source)
    end

    -- Trick for performance.
    --
    -- No need to create merger, key_def and pass tuples over the
    -- merger, when we have only one tuple source.
    if #merger_sources == 1 then
        return merger_sources[1]
    end

    local keydef = Keydef.new(space, opts.field_names, index_id)
    -- When built-in merger is used with external keydef, `merger_lib.new(keydef)`
    -- fails. It's simply fixed by casting `keydef` to 'struct key_def&'.
    keydef = ffi.cast('struct key_def&', keydef)

    local merger = merger_lib.new(keydef, merger_sources, {
        reverse = reverse_tarantool_iters[opts.tarantool_iter],
    })

    return merger
end


local function new_readview(vshard_router, replicasets, readview_info, space, index_id, func_name, func_args, opts)
    opts = opts or {}
    local call_opts = opts.call_opts

    -- Request a first data chunk and create merger sources.
    local merger_sources = {}
    for replicaset_id, replicaset in pairs(replicasets) do
        local replicaset_info = readview_info[replicaset_id]

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

            -- Perform a request.
            local buf = buffer.ibuf()
            local net_box_opts = {is_async = true, buffer = buf,
                                  skip_header = utils.tarantool_supports_netbox_skip_header_option() or nil}
            func_args[4].readview_id = replicaset_info.id
            local future = replica.conn:call(func_name, func_args, net_box_opts)

            -- Create a source.
            local context = {
                net_box_opts = net_box_opts,
                buffer = buf,
                func_name = func_name,
                func_args = func_args,
                replicaset = replicaset,
                vshard_call_name = nil,
                timeout = call_opts.timeout,
                fetch_latest_metadata = call_opts.fetch_latest_metadata,
                space_name = space.name,
                vshard_router = vshard_router,
                readview = true,
                future_replica = replica
            }
            local state = {future = future}
            local source = merger_lib.new_buffer_source(fetch_chunk, context, state)
            table.insert(merger_sources, source)

            ::next_replica::
        end

        ::next_replicaset::
    end

    -- Trick for performance.
    --
    -- No need to create merger, key_def and pass tuples over the
    -- merger, when we have only one tuple source.
    if #merger_sources == 1 then
        return merger_sources[1]
    end

    local keydef = Keydef.new(space, opts.field_names, index_id)
    -- When built-in merger is used with external keydef, `merger_lib.new(keydef)`
    -- fails. It's simply fixed by casting `keydef` to 'struct key_def&'.
    keydef = ffi.cast('struct key_def&', keydef)

    local merger = merger_lib.new(keydef, merger_sources, {
        reverse = reverse_tarantool_iters[opts.tarantool_iter],
    })

    return merger
end

return {
    new = new,
    new_readview = new_readview,
}
