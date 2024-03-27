--- Tarantool module for performing operations across the cluster
--
-- @module crud

local cfg = require('crud.cfg')
local insert = require('crud.insert')
local insert_many = require('crud.insert_many')
local replace = require('crud.replace')
local replace_many = require('crud.replace_many')
local get = require('crud.get')
local update = require('crud.update')
local upsert = require('crud.upsert')
local upsert_many = require('crud.upsert_many')
local delete = require('crud.delete')
local select = require('crud.select')
local truncate = require('crud.truncate')
local len = require('crud.len')
local count = require('crud.count')
local borders = require('crud.borders')
local utils = require('crud.common.utils')
local stats = require('crud.stats')
local readview = require('crud.readview')
local schema = require('crud.schema')
local storage_info = require('crud.storage_info')
local storage = require('crud.storage')

local crud = {}

-- @refer crud.version
-- @tfield string _VERSION
--  Module version.
crud._VERSION = require('crud.version')

--- CRUD operations.
-- @section crud

-- @refer insert.tuple
-- @function insert
crud.insert = stats.wrap(insert.tuple, stats.op.INSERT)

-- @refer insert.object
-- @function insert_object
crud.insert_object = stats.wrap(insert.object, stats.op.INSERT)

-- @refer insert_many.tuples
-- @function insert_many
crud.insert_many = stats.wrap(insert_many.tuples, stats.op.INSERT_MANY)

-- @refer insert_many.objects
-- @function insert_object_many
crud.insert_object_many = stats.wrap(insert_many.objects, stats.op.INSERT_MANY)

-- @refer get.call
-- @function get
crud.get = stats.wrap(get.call, stats.op.GET)

-- @refer replace.tuple
-- @function replace
crud.replace = stats.wrap(replace.tuple, stats.op.REPLACE)

-- @refer replace.object
-- @function replace_object
crud.replace_object = stats.wrap(replace.object, stats.op.REPLACE)

-- @refer replace_many.tuples
-- @function replace_many
crud.replace_many = stats.wrap(replace_many.tuples, stats.op.REPLACE_MANY)

-- @refer replace_many.objects
-- @function replace_object_many
crud.replace_object_many = stats.wrap(replace_many.objects, stats.op.REPLACE_MANY)

-- @refer update.call
-- @function update
crud.update = stats.wrap(update.call, stats.op.UPDATE)

-- @refer upsert.tuple
-- @function upsert
crud.upsert = stats.wrap(upsert.tuple, stats.op.UPSERT)

-- @refer upsert_many.tuples
-- @function upsert_many
crud.upsert_many = stats.wrap(upsert_many.tuples, stats.op.UPSERT_MANY)

-- @refer upsert_many.objects
-- @function upsert_object_many
crud.upsert_object_many = stats.wrap(upsert_many.objects, stats.op.UPSERT_MANY)

-- @refer upsert.object
-- @function upsert
crud.upsert_object = stats.wrap(upsert.object, stats.op.UPSERT)

-- @refer delete.call
-- @function delete
crud.delete = stats.wrap(delete.call, stats.op.DELETE)

-- @refer select.call
-- @function select
crud.select = stats.wrap(select.call, stats.op.SELECT)

-- @refer select.pairs
-- @function pairs
crud.pairs = stats.wrap(select.pairs, stats.op.SELECT, { pairs = true })

-- @refer utils.unflatten_rows
-- @function unflatten_rows
crud.unflatten_rows = utils.unflatten_rows

-- @refer truncate.call
-- @function truncate
crud.truncate = stats.wrap(truncate.call, stats.op.TRUNCATE)

-- @refer len.call
-- @function len
crud.len = stats.wrap(len.call, stats.op.LEN)

-- @refer count.call
-- @function count
crud.count = stats.wrap(count.call, stats.op.COUNT)

-- @refer borders.min
-- @function min
crud.min = stats.wrap(borders.min, stats.op.BORDERS)

-- @refer borders.max
-- @function max
crud.max = stats.wrap(borders.max, stats.op.BORDERS)

-- @refer utils.cut_rows
-- @function cut_rows
crud.cut_rows = utils.cut_rows

-- @refer utils.cut_objects
-- @function cut_objects
crud.cut_objects = utils.cut_objects

-- @refer cfg.cfg
-- @function cfg
crud.cfg = cfg.cfg

-- @refer stats.get
-- @function stats
crud.stats = stats.get

-- @refer stats.reset
-- @function reset_stats
crud.reset_stats = stats.reset

-- @refer storage_info.call
-- @function storage_info
crud.storage_info = storage_info.call

-- @refer readview.new
-- @function readview
crud.readview = readview.new

-- @refer schema.call
-- @function schema
crud.schema = schema.call

function crud.init_router()
   rawset(_G, 'crud', crud)
end

function crud.stop_router()
    rawset(_G, 'crud', nil)
end

--- Initializes crud on node
--
-- Exports all functions that are used for calls
-- and CRUD operations.
--
-- @function init
--
crud.init_storage = storage.init

crud.stop_storage = storage.stop

return crud
