local buffer = require('buffer')
local msgpack = require('msgpack')
local log = require('log')
local ffi = require('ffi')
local collations = require('crud.common.collations')

local key_def_lib
local merger_lib

if pcall(require, 'tuple.merger') then
    merger_lib = require('tuple.merger')
    key_def_lib = require('tuple.keydef')
elseif pcall(require, 'merger') then
    log.info('Impossible to load "tuple-merger" module. Built-in "merger" is used')
    merger_lib = require('merger')
    key_def_lib = require('key_def')
else
    error('Seems your Tarantool version (' .. _TARANTOOL ..
            ') does not support "tuple-merger" or "merger" modules')
end

local key_def_cache = {}
setmetatable(key_def_cache, {__mode = 'k'})

-- As "tuple.key_def" doesn't support collation_id
-- we manually change it to collation
local function normalize_parts(index_parts)
    local result = {}

    for _, part in ipairs(index_parts) do
        if part.collation_id == nil then
            table.insert(result, part)
        else
            local part_copy = table.copy(part)
            part_copy.collation = collations.get(part)
            part_copy.collation_id = nil
            table.insert(result, part_copy)
        end
    end

    return result
end

local function get_key_def(replicasets, space_name, index_name)
    -- Get requested and primary index metainfo.
    local conn = select(2, next(replicasets)).master.conn
    local index = conn.space[space_name].index[index_name]

    if key_def_cache[index] ~= nil then
        return key_def_cache[index]
    end

    -- Create a key def
    local primary_index = conn.space[space_name].index[0]
    local key_def = key_def_lib.new(normalize_parts(index.parts))
    if not index.unique then
        key_def = key_def:merge(key_def_lib.new(normalize_parts(primary_index.parts)))
    end

    key_def_cache[index] = key_def

    return key_def
end

local function bswap_u16(num)
    return bit.rshift(bit.bswap(tonumber(num)), 16)
end

-- See
-- https://github.com/tarantool/tarantool/blob/0ab21ac9eeaaae2aa0aef5e598d374669f96df9e/src/lua/msgpackffi.lua
-- to understand following hell
-- This code will work for ALL Tarantool versions
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

local function decode_response_headers(buf)
    -- {48: [cursor, [tuple_1, tuple_2, ...]]} (exactly 1 pair of key-value)
    data[0] = buf.rpos

    -- 48 (key)
    data[0] = data[0] + 1

    -- [cursor, [tuple_1, tuple_2, ...]] (value)
    data[0] = data[0] + 1

    -- Decode array header
    local c = data[0][0]
    data[0] = data[0] + 1
    if c == 0xdc then
        decode_u16(data)
    elseif c == 0xdd then
        decode_u32(data)
    end

    return ffi.cast(char_ptr, data[0])
end

local function decode_metainfo(buf)
    -- Skip an array around a call return values.
    buf.rpos = decode_response_headers(buf)

    -- Decode a first return value (metainfo).
    local res
    res, buf.rpos = msgpack.decode(buf.rpos, buf:size())
    return res
end

--- Wait for a data chunk and request for the next data chunk.
local function fetch_chunk(context, state)
    local net_box_opts = context.net_box_opts
    local buf = context.buffer
    local func_name = context.func_name
    local func_args = context.func_args
    local replicaset = context.replicaset
    local future = state.future

    -- The source was entirely drained.
    if future == nil then
        return nil
    end

    -- Wait for requested data.
    local res, err = future:wait_result()
    if res == nil then
        error(err)
    end

    -- Decode metainfo, leave data to be processed by the merger.
    local cursor = decode_metainfo(buf)

    -- Check whether we need the next call.
    if cursor.is_end then
        local next_state = {}
        return next_state, buf
    end

    -- Request the next data while we processing the current ones.
    -- Note: We reuse the same buffer for all request to a replicaset.
    local next_func_args = func_args

    -- change context.func_args too, but it does not matter
    next_func_args[4].after_tuple = cursor.after_tuple
    local next_future = replicaset:callro(func_name, next_func_args, net_box_opts)

    local next_state = {future = next_future}
    return next_state, buf
end

local reverse_tarantool_iters = {
    [box.index.LE] = true,
    [box.index.LT] = true,
    [box.index.REQ] = true,
}

local function new(replicasets, space_name, index_id, func_name, func_args, opts)
    opts = opts or {}

    local key_def = get_key_def(replicasets, space_name, index_id)

    -- Request a first data chunk and create merger sources.
    local merger_sources = {}
    for _, replicaset in pairs(replicasets) do
        -- Perform a request.
        local buf = buffer.ibuf()
        local net_box_opts = {is_async = true, buffer = buf, skip_header = false}
        local future = replicaset:callro(func_name, func_args,
                net_box_opts)

        -- Create a source.
        local context = {
            net_box_opts = net_box_opts,
            buffer = buf,
            func_name = func_name,
            func_args = func_args,
            replicaset = replicaset,
        }
        local state = {future = future}
        local source = merger_lib.new_buffer_source(fetch_chunk, context, state)
        table.insert(merger_sources, source)
    end

    local merger = merger_lib.new(key_def, merger_sources, {
        reverse = reverse_tarantool_iters[opts.tarantool_iter],
    })

    return merger
end

return {
    new = new,
}
