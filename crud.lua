--- Tarantool module for performing operations across the cluster
--
-- @module crud

local cfg = require('crud.cfg')
local insert = require('crud.insert')
local insert_many = require('crud.insert_many')
local replace = require('crud.replace')
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
local sharding_metadata = require('crud.common.sharding.sharding_metadata')
local utils = require('crud.common.utils')
local stats = require('crud.stats')

local crud = {}

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
crud.insert_many = insert_many.tuples

-- @refer insert_many.objects
-- @function insert_object_many
crud.insert_object_many = insert_many.objects

-- @refer get.call
-- @function get
crud.get = stats.wrap(get.call, stats.op.GET)

-- @refer replace.tuple
-- @function replace
crud.replace = stats.wrap(replace.tuple, stats.op.REPLACE)

-- @refer replace.object
-- @function replace_object
crud.replace_object = stats.wrap(replace.object, stats.op.REPLACE)

-- @refer update.call
-- @function update
crud.update = stats.wrap(update.call, stats.op.UPDATE)

-- @refer upsert.tuple
-- @function upsert
crud.upsert = stats.wrap(upsert.tuple, stats.op.UPSERT)

-- @refer upsert_many.tuples
-- @function upsert_many
crud.upsert_many = upsert_many.tuples

-- @refer upsert_many.objects
-- @function upsert_object_many
crud.upsert_object_many = upsert_many.objects

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
    insert_many.init()
    get.init()
    replace.init()
    update.init()
    upsert.init()
    upsert_many.init()
    delete.init()
    select.init()
    truncate.init()
    len.init()
    count.init()
    borders.init()
    sharding_metadata.init()
end

function crud.init_router()
   rawset(_G, 'crud', crud)
end

function crud.stop_router()
    rawset(_G, 'crud', nil)
end

function crud.stop_storage()
    rawset(_G, '_crud', nil)
end

return crud
