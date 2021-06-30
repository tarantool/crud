local errors = require('errors')

local select_executor = require('crud.select.executor')
local select_filters = require('crud.select.filters')
local dev_checks = require('crud.common.dev_checks')
local schema = require('crud.common.schema')

local SelectError = errors.new_class('SelectError')

local select_module

if '2' <= _TARANTOOL and _TARANTOOL <= '2.3.3' then
    -- "merger" segfaults here
    -- See https://github.com/tarantool/tarantool/issues/4954
    select_module = require('crud.select.compat.select_old')
elseif not package.search('tuple.merger') and package.loaded['merger'] == nil then
    -- we don't use pcall(require, modile_name) here because it
    -- leads to ignoring errors other than 'No LuaRocks module found'

    -- "merger" isn't supported here
    select_module = require('crud.select.compat.select_old')
else
    select_module = require('crud.select.compat.select')
end

local function make_cursor(data)
    local last_tuple = data[#data]

    return {
        after_tuple = last_tuple,
    }
end

function checkers.vshard_call_mode(p)
    return p == 'write' or p == 'read'
end

local function select_on_storage(space_name, index_id, conditions, opts)
    dev_checks('string', 'number', '?table', {
        scan_value = 'table',
        after_tuple = '?table',
        tarantool_iter = 'number',
        limit = 'number',
        scan_condition_num = '?number',
        field_names = '?table',
    })

    local space = box.space[space_name]
    if space == nil then
        return nil, SelectError:new("Space %q doesn't exist", space_name)
    end

    local index = space.index[index_id]
    if index == nil then
        return nil, SelectError:new("Index with ID %s doesn't exist", index_id)
    end

    local filter_func, err = select_filters.gen_func(space, conditions, {
        tarantool_iter = opts.tarantool_iter,
        scan_condition_num = opts.scan_condition_num,
    })
    if err ~= nil then
        return nil, SelectError:new("Failed to generate tuples filter: %s", err)
    end

    -- execute select
    local tuples, err = select_executor.execute(space, index, filter_func, {
        scan_value = opts.scan_value,
        after_tuple = opts.after_tuple,
        tarantool_iter = opts.tarantool_iter,
        limit = opts.limit,
    })
    if err ~= nil then
        return nil, SelectError:new("Failed to execute select: %s", err)
    end

    local cursor
    if #tuples < opts.limit or opts.limit == 0 then
        cursor = {is_end = true}
    else
        cursor = make_cursor(tuples)
    end

    -- getting tuples with user defined fields (if `fields` option is specified)
    -- and fields that are needed for comparison on router (primary key + scan key)
    return cursor, schema.filter_tuples_fields(tuples, opts.field_names)
end

function select_module.init()
   _G._crud.select_on_storage = select_on_storage
end

return select_module
