--- module to call vshard.storage.bucket_ref / vshard.storage.bucket_unref
--- on write requests
--- there are two modes: safe and fast. on safe mode module
--- calls vshard.storage.bucket_ref / vshard.storage.bucket_unref
--- on fast mode it does nothing.
--- default is fast mode.

--- bucket_refw and bucket_unrefrw must be called in one transaction in order to prevent
--- safe_mode change during execution.

local vshard = require('vshard')
local errors = require('errors')
local rebalance = require('crud.common.rebalance')

local safe_methods
local fast_methods

local M = {
    BucketRefError = errors.new_class('bucket_ref_error', {capture_stack = false})
}

local function make_bucket_ref_err(bucket_id, vshard_ref_err)
    local err = M.BucketRefError:new(M.BucketRefError:new(
        "failed bucket_ref: %s, bucket_id: %s",
        vshard_ref_err.name,
        bucket_id
    ))
    err.bucket_ref_errs = {
        {
            bucket_id = bucket_id,
            vshard_err = vshard_ref_err,
        }
    }
    return err
end

--- on module initialization safe_mode_status func must be set
--- it's rebalance.safe_mode_status
function M.safe_mode_status()
    error('safe_mode_status not set')
end

--- Slow bucket_refrw implementation that calls vshard.storage.bucket_refrw.
--- must be called with bucket_unrefrw in transaction
function M._bucket_refrw(bucket_id)
    local ref_ok, vshard_ref_err = vshard.storage.bucket_refrw(bucket_id)
    if not ref_ok then
        return false, make_bucket_ref_err(bucket_id, vshard_ref_err)
    end

    return true
end

--- Slow bucket_unrefrw implementation that calls vshard.storage.bucket_unrefrw.
--- must be called with bucket_refrw in transaction
function M._bucket_unrefrw(bucket_id)
    return vshard.storage.bucket_unrefrw(bucket_id)
end

--- Slow bucket_refro implementation that calls vshard.storage.bucket_refro.
function M._bucket_refro(bucket_id)
    local ref_ok, vshard_ref_err = vshard.storage.bucket_refro(bucket_id)
    if not ref_ok then
        return false, make_bucket_ref_err(bucket_id, vshard_ref_err)
    end

    return true
end

--- Slow bucket_unrefro implementation that calls vshard.storage.bucket_unrefro.
--- must be called in one transaction with bucket_refrw_many
function M._bucket_unrefro(bucket_id)
    return vshard.storage.bucket_unrefro(bucket_id)
end

--- Slow bucket_refrw_many that calls bucket_refrw for every bucket and aggregates errors
--- @param bucket_ids table<number, boolean>
function M._bucket_refrw_many(bucket_ids)
    local bucket_ref_errs = {}
    local reffed_bucket_ids = {}
    for bucket_id in pairs(bucket_ids) do
        local ref_ok, bucket_refrw_err = safe_methods.bucket_refrw(bucket_id)
        if not ref_ok then

            table.insert(bucket_ref_errs, bucket_refrw_err.bucket_ref_errs[1])
            goto continue
        end

        reffed_bucket_ids[bucket_id] = true
        ::continue::
    end

    if next(bucket_ref_errs) ~= nil then
        local err = M.BucketRefError:new(M.BucketRefError:new("failed bucket_ref"))
        err.bucket_ref_errs = bucket_ref_errs
        safe_methods.bucket_unrefrw_many(reffed_bucket_ids)
        return nil, err
    end

    return true
end

--- Slow bucket_unrefrw_many that calls vshard.storage.bucket_unrefrw for every bucket.
--- must be called in one transaction with bucket_refrw_many
--- will never happen in called in one transaction with bucket_refrw_many
--- @param bucket_ids table<number, true>
function M._bucket_unrefrw_many(bucket_ids)
    local unref_all_ok = true
    local unref_last_err
    for reffed_bucket_id in pairs(bucket_ids) do
        local unref_ok, unref_err = safe_methods.bucket_unrefrw(reffed_bucket_id)
        if not unref_ok then
            unref_all_ok = nil
            unref_last_err = unref_err
        end
    end

    if not unref_all_ok then
        return nil, unref_last_err
    end
    return true
end

--- _fast implements module logic for fast mode
function M._fast()
    return true
end

safe_methods = {
    bucket_refrw = M._bucket_refrw,
    bucket_unrefrw = M._bucket_unrefrw,
    bucket_refro = M._bucket_refro,
    bucket_unrefro = M._bucket_unrefro,
    bucket_refrw_many = M._bucket_refrw_many,
    bucket_unrefrw_many = M._bucket_unrefrw_many,
}

fast_methods = {
    bucket_refrw = M._fast,
    bucket_unrefrw = M._fast,
    bucket_refro = M._fast,
    bucket_unrefro = M._fast,
    bucket_refrw_many = M._fast,
    bucket_unrefrw_many = M._fast,
}

local function set_methods(methods)
    for method_name, func in pairs(methods) do
        M[method_name] = func
    end
end

local function set_safe_mode()
    set_methods(safe_methods)
end

local function set_fast_mode()
    set_methods(fast_methods)
end

local hooks_registered = false

--- set safe mode func
--- from rebalance.safe_mode_status
function M.set_safe_mode_status(safe_mode_status)
    M.safe_mode_status = safe_mode_status

    if safe_mode_status() then
        set_safe_mode()
    else
        set_fast_mode()
    end

    if not hooks_registered then
        rebalance.register_safe_mode_enable_hook(set_safe_mode)
        rebalance.register_safe_mode_disable_hook(set_fast_mode)
        hooks_registered = true
    end
end

set_fast_mode()

return M
