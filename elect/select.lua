local checks = require('checks')
local errors = require('errors')
local key_def = require('key_def')
local log = require('log')
local vshard = require('vshard')

require('elect.checkers')

local call = require('elect.call')
local state = require('elect.state')
local registry = require('elect.registry')

local Iterator = require('elect.iterator')

local SelectError = errors.new_class('Select',  {capture_stack = false})

local DEFAULT_BATCH_SIZE = 10

local select = {}

local SELECT_FUNC_NAME = '__select'
local SELECT_CLEANUP_FUNC_NAME = '__select_cleanup'

local function call_select_on_storage(select_id, space_name, opts)
    checks('string', 'string',  {
        batch_size = '?number',
    })

    local space = box.space[space_name]
    if space == nil then
        return nil, SelectError:new("Space %s doesn't exists", space_name)
    end

    local select_state = state.get(select_id)

    local tuples = space:select(select_state.cursor, {
        limit = opts.batch_size or DEFAULT_BATCH_SIZE,
        iterator = box.index.GT,
    })

    local last_batch = false
    if opts.batch_size ~= nil and #tuples < opts.batch_size then
        last_batch = true
    end

    if #tuples == 0 then
        state.clear(select_state.id)
        return {objects = {}, last_batch = true}
    end

    if select_state.key_def == nil then
        local primary_index = space.index[0]
        select_state.key_def = key_def.new(primary_index.parts)
    end

    select_state.cursor = select_state.key_def:extract_key(tuples[#tuples])

    local objects = {}
    for _, tuple in ipairs(tuples) do
        table.insert(objects, tuple:tomap({names_only = true}))
    end

    return {
        objects = objects,
        last_batch = last_batch
    }
end

local function select_cleanup(id)
    checks('string')
    return state.clear(id)
end

function select.init()
    registry.add({
        [SELECT_FUNC_NAME] = call_select_on_storage,
        [SELECT_CLEANUP_FUNC_NAME] = select_cleanup,
    })
end

local function select_iteration(select_id, state, opts)
    checks('string', 'table', {
        timeout = '?number',
        batch_size = '?number',
        replicasets = 'table',
    })

    local results_map, err = call.ro({
        func_name = SELECT_FUNC_NAME,
        func_args = {select_id, state.space_name, {
            batch_size = opts.batch_size,
        }},
        replicasets = opts.replicasets,
        timeout = opts.timeout,
    })

    if results_map == nil then
        local ok, cleanup_err = call.ro({
            func_name = SELECT_CLEANUP_FUNC_NAME,
            func_args = {select_id},
            timeout = opts.timeout,
        })

        if not ok then
            log.warn("Failed to cleanup select state: %s", cleanup_err)
        end

        return nil, SelectError:new("Failed to select: %s", err)
    end

    return results_map
end

function select.call(space_name, key_parts, opts)
    checks('string', 'strings_array', {
        timeout = '?number',
        limit = '?number',      -- the whole select limit
        batch_size = '?number', -- tuples per one call
    })

    opts = opts or {}

    local replicasets, err = vshard.router.routeall()
    if err ~= nil then
        return nil, SelectError:new("Failed to get all replicasets: %s", err.err)
    end

    local initial_state = {
        space_name = space_name,
    }

    local iter = Iterator.new(initial_state, {
        limit = opts.limit,
        batch_size = opts.batch_size,
        replicasets = replicasets,
        iteration_func = select_iteration,
        key_parts = key_parts,
    })

    return iter
end

return select
