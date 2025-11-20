local fiber = require('fiber')
local log = require('log')
local vshard_consts = require('vshard.consts')
local utils = require('crud.common.utils')

local SETTINGS_SPACE_NAME = '_crud_settings'
local SAFE_MOD_ENABLE_EVENT = '_crud.safe_mode_enable'


local M = {
    safe_mode = false,
    safe_mode_enable_hooks = {},
    safe_mode_disable_hooks = {},
    _router_cache_last_clear_ts = fiber.time()
}

local function create_space()
    local settings_space = box.schema.space.create(SETTINGS_SPACE_NAME, {
        engine = 'memtx',
        format = {
            { name = 'key', type = 'string' },
            { name = 'value', type = 'any' },
        },
        if_not_exists = true,
    })
    settings_space:create_index('primary', { parts = { 'key' }, if_not_exists = true })
end

local function safe_mode_trigger(_, new, space, op)
    if space ~= '_bucket' then
        return
    end
    if (op == 'INSERT' and new.status == vshard_consts.BUCKET.RECEIVING) or
            (op == 'REPLACE' and new.status == vshard_consts.BUCKET.SENDING) then
        box.broadcast(SAFE_MOD_ENABLE_EVENT, true)
    end
end

local function register_enable_hook(func)
    M.safe_mode_enable_hooks[func] = true
end

local function remove_enable_hook(func)
    M.safe_mode_enable_hooks[func] = nil
end

local function register_disable_hook(func)
    M.safe_mode_disable_hooks[func] = true
end

local function remove_disable_hook(func)
    M.safe_mode_disable_hooks[func] = nil
end

local function safe_mode_status()
    return M.safe_mode
end

local function safe_mode_enable()
    if not box.info.ro then
        box.space[SETTINGS_SPACE_NAME]:replace{ 'safe_mode', true }
        for _, trig in pairs(box.space._bucket:on_replace()) do
            if trig == safe_mode_trigger then
                box.space._bucket:on_replace(nil, safe_mode_trigger)
            end
        end
    end
    M.safe_mode = true

    for hook, _ in pairs(M.safe_mode_enable_hooks) do
        hook()
    end
end

local function safe_mode_disable()
    if not box.info.ro then
        box.space[SETTINGS_SPACE_NAME]:replace{ 'safe_mode', false }
        box.space._bucket:on_replace(safe_mode_trigger)
    end
    M.safe_mode = false

    for hook, _ in pairs(M.safe_mode_disable_hooks) do
        hook()
    end
end

local function rebalance_init()
    -- box.watch was introduced in tarantool 2.10.0
    if not utils.tarantool_supports_box_watch() then
        log.warn('This version of tarantool does not support autoswitch to safe mode during rebalance. '
            .. 'Update to newer version or use `_crud.rebalance_safe_mode_enable()` to enable safe mode manually.')
        return
    end

    box.watch('box.status', function()
        if box.info.ro then
            return
        end

        local stored_safe_mode
        if box.space[SETTINGS_SPACE_NAME] == nil then
            create_space()
            box.space[SETTINGS_SPACE_NAME]:insert{ 'safe_mode', false }
        else
            stored_safe_mode = box.space[SETTINGS_SPACE_NAME]:get{ 'safe_mode' }
        end
        M.safe_mode = stored_safe_mode and stored_safe_mode.value or false

        if M.safe_mode then
            for hook, _ in pairs(M.safe_mode_enable_hooks) do
                hook()
            end
        else
            box.space._bucket:on_replace(safe_mode_trigger)
            for hook, _ in pairs(M.safe_mode_disable_hooks) do
                hook()
            end
        end
    end)

    box.watch(SAFE_MOD_ENABLE_EVENT, function(_, do_enable)
        if box.info.ro or not do_enable then
            return
        end
        safe_mode_enable()
    end)
end

local function rebalance_stop()
    M.safe_mode_disable()
end

local function router_cache_clear()
    local r = utils.get_vshard_router_instance()
    M._router_cache_last_clear_ts = fiber.time()
    return r:_route_map_clear()
end

local function router_cache_length()
    local r = utils.get_vshard_router_instance()
    return r.known_bucket_count
end

local function router_cache_last_clear_ts()
    return M._router_cache_last_clear_ts
end

M.init = rebalance_init
M.stop = rebalance_stop
M.safe_mode_status = safe_mode_status
M.safe_mode_enable = safe_mode_enable
M.safe_mode_disable = safe_mode_disable
M.register_safe_mode_enable_hook = register_enable_hook
M.remove_safe_mode_enable_hook = remove_enable_hook
M.register_safe_mode_disable_hook = register_disable_hook
M.remove_safe_mode_disable_hook = remove_disable_hook

M.router = {
    cache_clear = router_cache_clear,
    cache_length = router_cache_length,
    cache_last_clear_ts = router_cache_last_clear_ts,
}

M.storage_api = {
    rebalance_safe_mode_status = safe_mode_status,
    rebalance_safe_mode_enable = safe_mode_enable,
    rebalance_safe_mode_disable = safe_mode_disable,
}

return M
