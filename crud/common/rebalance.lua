local log = require('log')
local trigger = require('internal.trigger')
local vshard_consts = require('vshard.consts')
local schema = require('crud.schema')
local utils = require('crud.common.utils')
local has_metrics_module, metrics = pcall(require, 'metrics')

local SAFE_MODE_STATUS = 'safe_mode_status'

local rebalance = {
    safe_mode = false,
    -- Trigger is run with one argument: true if safe mode is enabled and false if disabled.
    on_safe_mode_toggle = trigger.new('_crud.safe_mode_toggle'),
}

local function safe_mode_bucket_trigger(_, new, space, op)
    if space ~= '_bucket' then
        return
    end
    -- We are interested only in two operations that indicate the beginning of bucket migration:
    --  * We are receiving a bucket (new bucket with status RECEIVING)
    --  * We are sending a bucket to another node (existing bucket status changes to SENDING)
    if (op == 'INSERT' and new.status == vshard_consts.BUCKET.RECEIVING) or
       (op == 'REPLACE' and new.status == vshard_consts.BUCKET.SENDING) then
        local stored_safe_mode = schema.settings_space:get{ SAFE_MODE_STATUS }
        if not stored_safe_mode or not stored_safe_mode.value then
            schema.settings_space:replace{ SAFE_MODE_STATUS, true }
        end
    end
end

local function _safe_mode_enable()
    -- The trigger is needed to detect the beginning of rebalance process to enable safe mode.
    -- If safe mode is enabled we don't need the trigger anymore.
    for _, trig in pairs(box.space._bucket:on_replace()) do
        if trig == safe_mode_bucket_trigger then
            box.space._bucket:on_replace(nil, trig)
        end
    end
    rebalance.safe_mode = true

    -- This function is running inside on_commit trigger, need pcall to protect from errors in external code.
    pcall(rebalance.on_safe_mode_toggle.run, rebalance.on_safe_mode_toggle, true)

    log.info('Rebalance safe mode enabled')
end

local function _safe_mode_disable()
    -- We have disabled safe mode so we need to add the trigger to detect the beginning
    -- of rebalance process to enable safe mode again.
    box.space._bucket:on_replace(safe_mode_bucket_trigger)
    rebalance.safe_mode = false

    -- This function is running inside on_commit trigger, need pcall to protect from errors in external code.
    pcall(rebalance.on_safe_mode_toggle.run, rebalance.on_safe_mode_toggle, false)

    log.info('Rebalance safe mode disabled')
end

local function create_settings_trigger()
    schema.settings_space:on_replace(function()
        box.on_commit(function(rows_iter)
            -- There may be multiple operations on safe mode status tuple in one transaction.
            -- We will take only the last action.
            -- 0 = do nothing, 1 = enable safe mode, -1 = disable safe mode
            local safe_mode_action = 0
            for _, old, new, sp in rows_iter() do
                -- These checks must be changed to skip unknown keys when there will be more than one setting
                -- in _crud_settings_local space.
                -- But for now it is better to raise an error than to silently ignore them.
                if sp ~= schema.settings_space.id then
                    goto continue
                end
                assert((old == nil or old.key == SAFE_MODE_STATUS) and (new.key == SAFE_MODE_STATUS))

                if (not old or not old.value) and new.value then
                    safe_mode_action = 1
                elseif old and old.value and not new.value then
                    safe_mode_action = -1
                end

                ::continue::
            end

            if safe_mode_action == 1 then
                _safe_mode_enable()
            elseif safe_mode_action == -1 then
                _safe_mode_disable()
            end
        end)
    end)
end

function rebalance.init()
    create_settings_trigger()

    local stored_safe_mode = schema.settings_space:get{ SAFE_MODE_STATUS }
    if stored_safe_mode == nil then
        stored_safe_mode = schema.settings_space:replace{ SAFE_MODE_STATUS, false }
    end

    if stored_safe_mode.value then
        _safe_mode_enable()
    else
        _safe_mode_disable()
    end
end

function rebalance.safe_mode_status()
    return rebalance.safe_mode
end

function rebalance.safe_mode_enable()
    schema.settings_space:replace{ SAFE_MODE_STATUS, true }
end

function rebalance.safe_mode_disable()
    schema.settings_space:replace{ SAFE_MODE_STATUS, false }
end

--- Rebalance storage API
rebalance.storage_api = {
    rebalance_safe_mode_status = rebalance.safe_mode_status,
    rebalance_safe_mode_enable = rebalance.safe_mode_enable,
    rebalance_safe_mode_disable = rebalance.safe_mode_disable,
}

--- Rebalance router API
rebalance.router_api = {}

function rebalance.router_api.cache_clear()
    local router = utils.get_vshard_router_instance()
    if router == nil then
        log.warn("Router is not initialized yet")
        return
    end
    return router:_route_map_clear()
end

--- Rebalance related metrics
if has_metrics_module then
    local safe_mode_enabled_gauge = metrics.gauge(
            'tnt_crud_storage_safe_mode_enabled',
            "is safe mode enabled on this storage instance"
    )

    metrics.register_callback(function()
        safe_mode_enabled_gauge:set(rebalance.safe_mode_status() and 1 or 0)
    end)
end

return rebalance
