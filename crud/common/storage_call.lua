local fiber = require('fiber')
local log = require('log')

local utils = require('crud.common.utils')

local CALL_ON_STORAGE_FUNC_NAME = 'call_on_storage'
local CRUD_CALL_ON_STORAGE_FUNC_NAME = utils.get_storage_call(CALL_ON_STORAGE_FUNC_NAME)
local LEGACY_RECHECK_INTERVAL = 60

local storage_call = {}
local legacy_call_on_storage_cache = {}

local function cache_key(replicaset_id)
    if replicaset_id == nil then
        return nil
    end

    return tostring(replicaset_id)
end

local function cache_entry(replicaset_id)
    local key = cache_key(replicaset_id)
    if key == nil then
        return nil
    end

    local entry = legacy_call_on_storage_cache[key]
    if entry == nil then
        return nil
    end

    if fiber.clock() >= entry.recheck_at then
        legacy_call_on_storage_cache[key] = nil
        return nil
    end

    return entry
end

local function is_legacy_call_on_storage(replicaset_id)
    return cache_entry(replicaset_id) ~= nil
end

local function mark_legacy_call_on_storage(replicaset_id)
    local key = cache_key(replicaset_id)
    if key == nil then
        return
    end

    local now = fiber.clock()
    local entry = legacy_call_on_storage_cache[key]
    if entry == nil or now >= entry.recheck_at then
        log.warn(
            "CRUD storage replicaset %q does not support %q; " ..
            "falling back to direct storage calls until storage is upgraded",
            key, CRUD_CALL_ON_STORAGE_FUNC_NAME
        )
    end

    legacy_call_on_storage_cache[key] = {
        recheck_at = now + LEGACY_RECHECK_INTERVAL,
    }
end

local function is_call_on_storage_unsupported_error(err)
    if err == nil or type(err.message) ~= 'string' then
        return false
    end

    local not_defined = ("Procedure '%s' is not defined"):format(CRUD_CALL_ON_STORAGE_FUNC_NAME)
    local access_denied = ("Execute access to function '%s' is denied"):format(CRUD_CALL_ON_STORAGE_FUNC_NAME)

    return err.message == not_defined or err.message:startswith(access_denied)
end

--- Storage API function name used by new routers to call a function on storage
--- under the original effective user.
storage_call.CALL_ON_STORAGE_FUNC_NAME = CALL_ON_STORAGE_FUNC_NAME

--- Clear the legacy marker for the replicaset.
-- Called after _crud.call_on_storage succeeds, so the router keeps using
-- the new storage wrapper for subsequent calls.
-- @param ?string|number replicaset_id Vshard replicaset identifier.
function storage_call.mark_call_on_storage_supported(replicaset_id)
    local key = cache_key(replicaset_id)
    if key ~= nil then
        legacy_call_on_storage_cache[key] = nil
    end
end

--- Decide whether the call should be retried using the legacy direct path.
-- If fallback is required, the replicaset is marked as legacy for a short
-- interval to avoid probing _crud.call_on_storage on every request.
-- @param ?string|number replicaset_id Vshard replicaset identifier.
-- @param ?table err Error returned by vshard/net.box.
-- @param boolean used_legacy_call True when the failed call already used
--        the direct legacy path.
-- @return boolean
function storage_call.should_fallback_to_legacy(replicaset_id, err, used_legacy_call)
    if used_legacy_call then
        return false
    end

    if not is_call_on_storage_unsupported_error(err) then
        return false
    end

    mark_legacy_call_on_storage(replicaset_id)
    return true
end

--- Extract a storage-side error from an async vshard result.
-- Async calls return transport errors separately, but a storage function may
-- still return nil, err as the function result. This helper returns that err.
-- @param ?table result Result returned by future:wait_result().
-- @return ?table
function storage_call.result_error(result)
    if type(result) ~= 'table' then
        return nil
    end

    if result[1] ~= nil then
        return nil
    end

    return result[2]
end

--- Prepare function name and arguments for a storage call.
-- By default the call is routed through _crud.call_on_storage, which restores
-- the original effective user on storage. If a legacy marker or force_legacy
-- is set, the function is called directly for compatibility with pre-1.6
-- storages.
-- @param ?string|number replicaset_id Vshard replicaset identifier.
-- @param string func_name Storage function name to call.
-- @param ?table func_args Storage function arguments.
-- @param ?boolean force_legacy Force direct legacy call.
-- @return string Function name to pass to vshard.
-- @return ?table Arguments to pass to vshard.
-- @return boolean True when the direct legacy path is used.
function storage_call.prepare(replicaset_id, func_name, func_args, force_legacy)
    if force_legacy or is_legacy_call_on_storage(replicaset_id) then
        return func_name, func_args, true
    end

    local func_args_ext = utils.append_array({box.session.effective_user(), func_name}, func_args)
    return CRUD_CALL_ON_STORAGE_FUNC_NAME, func_args_ext, false
end

return storage_call
