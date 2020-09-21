--- Tarantool module for performing operations across the cluster
--
-- @module crud

local registry = require('crud.common.registry')
local call = require('crud.common.call')
local insert = require('crud.insert')
local get = require('crud.get')
local update = require('crud.update')
local delete = require('crud.delete')
local select = require('crud.select')

local crud = {}

--- Functions registry
-- @section registry

-- @refer registry.add
-- @function register
crud.register = registry.add

--- CRUD operations.
-- @section crud

-- @refer insert.call
-- @function insert
crud.insert = insert.call

-- @refer get.call
-- @function get
crud.get = get.call

-- @refer update.call
-- @function update
crud.update = update.call

-- @refer delete.call
-- @function delete
crud.delete = delete.call

-- @refer select.call
-- @function select
crud.select = select.call

--- Initializes crud on node
--
-- Exports all functions that are used for calls
-- and CRUD operations.
--
-- @function init
--
function crud.init()
    call.init()
    insert.init()
    get.init()
    update.init()
    delete.init()
    select.init()
end

return crud
