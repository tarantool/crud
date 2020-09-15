--- Tarantool module for performing operations across the cluster
--
-- @module elect

local registry = require('elect.common.registry')
local call = require('elect.common.call')
local insert = require('elect.insert')
local get = require('elect.get')
local update = require('elect.update')
local delete = require('elect.delete')
local select = require('elect.select')
local select_conditions = require('elect.select.conditions')

local elect = {}

--- Functions registry
-- @section registry

-- @refer registry.add
-- @function register
elect.register = registry.add

--- CRUD operations.
-- @section crud

-- @refer insert.call
-- @function insert
elect.insert = insert.call

-- @refer get.call
-- @function get
elect.get = get.call

-- @refer update.call
-- @function update
elect.update = update.call

-- @refer delete.call
-- @function delete
elect.delete = delete.call

-- @refer select.call
-- @function select
elect.select = select.call

-- Select operator functions
for func_name, func in pairs(select_conditions.funcs) do
    elect[func_name] = func
end

--- Initializes elect on node
--
-- Exports all functions that are used for calls
-- and CRUD operations.
--
-- @function init
--
function elect.init()
    call.init()
    insert.init()
    get.init()
    update.init()
    delete.init()
    select.init()
end

return elect
