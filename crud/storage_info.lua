local checks = require('checks')
local errors = require('errors')
local fiber = require('fiber')
local log = require('log')

local const = require('crud.common.const')
local utils = require('crud.common.utils')

local StorageInfoError = errors.new_class('StorageInfoError')

local storage_info = {}

local STORAGE_INFO_FUNC_NAME = 'storage_info_on_storage'
local CRUD_STORAGE_INFO_FUNC_NAME = utils.get_storage_call(STORAGE_INFO_FUNC_NAME)

--- Storage status information.
--
-- @function storage_info_on_storage
--
-- @return a table with storage status.
local function storage_info_on_storage()
    return {status = "running"}
end

storage_info.storage_api = {[STORAGE_INFO_FUNC_NAME] = storage_info_on_storage}

--- Polls replicas for storage state
--
-- @function call
--
-- @tparam ?number opts.timeout
--  Function call timeout
--
-- @tparam ?string|table opts.vshard_router
--  Cartridge vshard group name or vshard router instance.
--
-- @return a table of storage states by replica id.
function storage_info.call(opts)
    checks({
        timeout = '?number',
        vshard_router = '?string|table',
    })

    opts = opts or {}

    local vshard_router, err = utils.get_vshard_router_instance(opts.vshard_router)
    if err ~= nil then
        return nil, StorageInfoError:new(err)
    end

    local replicasets, err = vshard_router:routeall()
    if err ~= nil then
        return nil, StorageInfoError:new("Failed to get router replicasets: %s", err.err)
    end

    local futures_by_replicas = {}
    local replica_state_by_id = {}
    local async_opts = {is_async = true}
    local timeout = opts.timeout or const.DEFAULT_VSHARD_CALL_TIMEOUT

    for _, replicaset in pairs(replicasets) do
        local master = utils.get_replicaset_master(replicaset, {cached = false})

        for replica_id, replica in pairs(replicaset.replicas) do

            replica_state_by_id[replica_id] = {
                status = "error",
                is_master = master == replica
            }

            if replica.backoff_err ~= nil then
                replica_state_by_id[replica_id].message = tostring(replica.backoff_err)
            else
                local ok, res = pcall(replica.conn.call, replica.conn, CRUD_STORAGE_INFO_FUNC_NAME,
                                      {}, async_opts)
                if ok then
                    futures_by_replicas[replica_id] = res
                else
                    local err_msg = string.format("Error getting storage info for %s", replica_id)
                    if res ~= nil then
                        log.error("%s: %s", err_msg, res)
                        replica_state_by_id[replica_id].message = tostring(res)
                    else
                        log.error(err_msg)
                        replica_state_by_id[replica_id].message = err_msg
                    end
                end
            end
        end
    end

    local deadline = fiber.clock() + timeout
    for replica_id, future in pairs(futures_by_replicas) do
        local wait_timeout = deadline - fiber.clock()
        if wait_timeout < 0 then
            wait_timeout = 0
        end

        local result, err = future:wait_result(wait_timeout)
        if result == nil then
            future:discard()
            local err_msg = string.format("Error getting storage info for %s", replica_id)
            if err ~= nil then
                if err.type == 'ClientError' and err.code == box.error.NO_SUCH_PROC then
                    replica_state_by_id[replica_id].status = "uninitialized"
                else
                    log.error("%s: %s", err_msg, err)
                    replica_state_by_id[replica_id].message = tostring(err)
                end
            else
                log.error(err_msg)
                replica_state_by_id[replica_id].message = err_msg
            end
        else
            replica_state_by_id[replica_id].status = result[1].status or "uninitialized"
        end
    end

    return replica_state_by_id
end

return storage_info
