--- Tarantool module for performing operations across the cluster
--
-- @module crud

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

--- CRUD operations.
-- @section crud

-- @refer insert.tuple
-- @function insert
crud.insert = insert.tuple

-- @refer insert.object
-- @function insert_object
crud.insert_object = insert.object

-- @refer get.call
-- @function get
crud.get = get.call

-- @refer replace.tuple
-- @function replace
crud.replace = replace.tuple

-- @refer replace.object
-- @function replace_object
crud.replace_object = replace.object

-- @refer update.call
-- @function update
crud.update = update.call

-- @refer upsert.tuple
-- @function upsert
crud.upsert = upsert.tuple

-- @refer upsert.object
-- @function upsert
crud.upsert_object = upsert.object

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
function crud.init_storage()
   if rawget(_G, '_crud') == nil then
      rawset(_G, '_crud', {})
   end

    insert.init()
    get.init()
    replace.init()
    update.init()
    upsert.init()
    delete.init()
    select.init()
end

function crud.init_router()
   rawset(_G, 'crud', crud)
end

function crud.init()
   crud.init_storage()
   crud.init_router()
end

return crud
