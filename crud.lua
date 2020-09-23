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

-- @refer replace.call
-- @function replace
crud.replace = replace.call

-- @refer update.call
-- @function update
crud.update = update.call

-- @refer upsert.call
-- @function upsert
crud.upsert = upsert.call

-- @refer delete.call
-- @function delete
crud.delete = delete.call

-- @refer select.call
-- @function select
crud.select = select.call

-- @refer select.pairs
-- @function pairs
crud.pairs = select.pairs

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
    replace.init()
    get.init()
    update.init()
    delete.init()
    select.init()
    upsert.init()
end

return crud
