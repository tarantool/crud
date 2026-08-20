local utils = require('crud.common.utils')
local executor = require('crud.storage_call.executor')
local storage_call_errors = require('crud.storage_call.errors')

local storage = {}

local STORAGE_FUNC_NAME = 'storage_call_on_storage'

local function assert_service_user()
    local service_user = utils.get_this_replica_user() or 'guest'
    local caller = box.session.user()

    storage_call_errors.class:assert(
        caller == service_user,
        'Access to the internal storage_call dispatcher is denied for user %q',
        caller
    )
end

local function storage_call_on_storage(run_as_user, call_data)
    assert_service_user()
    return executor.execute(run_as_user, call_data)
end

storage.func_name = STORAGE_FUNC_NAME
storage.storage_api = {
    [STORAGE_FUNC_NAME] = storage_call_on_storage,
}

return storage
