--- Tarantool module for performing operations across the cluster
--
-- @module crud

local registry = require('crud.common.registry')
local call = require('crud.common.call')
local insert = require('crud.insert')
local replace = require('crud.replace')
local get = require('crud.get')
local update = require('crud.update')
local upsert = require('crud.upsert')
local delete = require('crud.delete')
local select = require('crud.select')
local utils = require('crud.common.utils')

local crud = {}

--- Functions registry
-- @section registry

-- @refer registry.add
-- @function register
crud.register = registry.add

--- CRUD operations.
-- @section crud

-- @refer insert.object
-- @function insert
crud.insert = insert.object

-- @refer get.call
-- @function get
crud.get = get.call

-- @refer replace.object
-- @function replace
crud.replace = replace.object

-- @refer update.call
-- @function update
crud.update = update.call

-- @refer upsert.object
-- @function upsert
crud.upsert = upsert.object

-- @refer delete.call
-- @function delete
crud.delete = delete.call

-- @refer select.call
-- @function select
crud.select = select.call

-- @refer select.pairs
-- @function pairs
crud.pairs = select.pairs

-- @refer utils.unflatten_rows
-- @function unflatten_rows
crud.unflatten_rows = utils.unflatten_rows

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
    replace.init()
    update.init()
    upsert.init()
    delete.init()
    select.init()
end

return crud
