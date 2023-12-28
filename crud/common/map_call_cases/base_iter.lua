local errors = require('errors')

local dev_checks = require('crud.common.dev_checks')
local GetReplicasetsError = errors.new_class('GetReplicasetsError')

local BaseIterator = {}

--- Create new base iterator for map call
--
-- @function new
--
-- @tparam[opt] table opts
-- Options of BaseIterator:new
-- @tparam[opt] table opts.func_args
-- Function arguments to call
-- @tparam[opt] table opts.replicasets
-- Replicasets to call
--
-- @return[1] table iterator
-- @treturn[2] nil
-- @treturn[2] table of tables Error description
function BaseIterator:new(opts)
    dev_checks('table', {
        func_args = '?table',
        replicasets = '?table',
        vshard_router = 'table',
    })

    local replicasets, err
    if opts.replicasets ~= nil then
        replicasets = opts.replicasets
    else
        replicasets, err = opts.vshard_router:routeall()
        if err ~= nil then
            return nil, GetReplicasetsError:new("Failed to get all replicasets: %s", err.err)
        end
    end

    local next_index, next_replicaset = next(replicasets)

    local iter = {
        func_args = opts.func_args,
        replicasets = replicasets,
        next_replicaset = next_replicaset,
        next_index = next_index
    }

    setmetatable(iter, self)
    self.__index = self

    return iter
end

--- Check there is next replicaset to call
--
-- @function has_next
--
-- @return[1] boolean
function BaseIterator:has_next()
    return self.next_index ~= nil
end

--- Get function arguments and next replicaset
--
-- @function get
--
-- @return[1] table func_args
-- @return[2] table replicaset
-- @return[3] string replicaset_id
function BaseIterator:get()
    local replicaset_id = self.next_index
    local replicaset = self.next_replicaset
    self.next_index, self.next_replicaset = next(self.replicasets, self.next_index)

    return self.func_args, replicaset, replicaset_id
end

return BaseIterator
