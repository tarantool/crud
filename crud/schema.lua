local checks = require('checks')
local errors = require('errors')

local SchemaError = errors.new_class('SchemaError', {capture_stack = false})

local schema_module = require('crud.common.schema')
local utils = require('crud.common.utils')

local schema = {}

schema.system_spaces = {
    -- https://github.com/tarantool/tarantool/blob/3240201a2f5bac3bddf8a74015db9b351954e0b5/src/box/schema_def.h#L77-L127
    ['_vinyl_deferred_delete'] = true,
    ['_schema'] = true,
    ['_collation'] = true,
    ['_vcollation'] = true,
    ['_space'] = true,
    ['_vspace'] = true,
    ['_sequence'] = true,
    ['_sequence_data'] = true,
    ['_vsequence'] = true,
    ['_index'] = true,
    ['_vindex'] = true,
    ['_func'] = true,
    ['_vfunc'] = true,
    ['_user'] = true,
    ['_vuser'] = true,
    ['_priv'] = true,
    ['_vpriv'] = true,
    ['_cluster'] = true,
    ['_trigger'] = true,
    ['_truncate'] = true,
    ['_space_sequence'] = true,
    ['_vspace_sequence'] = true,
    ['_fk_constraint'] = true,
    ['_ck_constraint'] = true,
    ['_func_index'] = true,
    ['_session_settings'] = true,
    -- https://github.com/tarantool/vshard/blob/b3c27b32637863e9a03503e641bb7c8c69779a00/vshard/storage/init.lua#L752
    ['_bucket'] = true,
    -- https://github.com/tarantool/ddl/blob/b55d0ff7409f32e4d527e2d25444d883bce4163b/test/set_sharding_metadata_test.lua#L92-L98
    ['_ddl_sharding_key'] = true,
    ['_ddl_sharding_func'] = true,
}

local function get_crud_schema(space)
    local sch = schema_module.get_normalized_space_schema(space)

    -- bucket_id is not nullable for a storage, yet
    -- it is optional for a crud user.
    for _, v in ipairs(sch.format) do
        if v.name == 'bucket_id' then
            v.is_nullable = true
        end
    end

    for id, v in pairs(sch.indexes) do
        -- There is no reason for a user to know about
        -- bucket_id index.
        if v.name == 'bucket_id' then
            sch.indexes[id] = nil
        end
    end

    return sch
end

schema.call = function(space_name, opts)
    checks('?string', {
        vshard_router = '?string|table',
        timeout = '?number',
        cached = '?boolean',
    })

    opts = opts or {}

    local vshard_router, err = utils.get_vshard_router_instance(opts.vshard_router)
    if err ~= nil then
        return nil, SchemaError:new(err)
    end

    if opts.cached ~= true then
        local _, err = schema_module.reload_schema(vshard_router)
        if err ~= nil then
            return nil, SchemaError:new(err)
        end
    end

    local spaces, err = utils.get_spaces(vshard_router, opts.timeout)
    if err ~= nil then
        return nil, SchemaError:new(err)
    end

    if space_name ~= nil then
        local space = spaces[space_name]
        if space == nil then
            return nil, SchemaError:new("Space %q doesn't exist", space_name)
        end
        return get_crud_schema(space)
    else
        local resp = {}

        for name, space in pairs(spaces) do
            -- Can be indexed by space id and space name,
            -- so we need to be careful with duplicates.
            if type(name) == 'string' and schema.system_spaces[name] == nil then
                resp[name] = get_crud_schema(space)
            end
        end

        return resp
    end
end

return schema
