local fiber = require('fiber')

local spaces = {}

spaces.SPACES = {
    memtx = '_crud_idempotency',
    vinyl = '_crud_idempotency_vinyl',
}

local MARKER_FORMAT = {
    {name = 'key', type = 'string', is_nullable = false},
    {name = 'bucket_id', type = 'unsigned', is_nullable = false},
    {name = 'applied_max', type = 'unsigned', is_nullable = true},
    {name = 'expires_at', type = 'number', is_nullable = false},
}

local function ensure_space(name, engine)
    box.schema.space.create(name, {
        engine = engine,
        format = MARKER_FORMAT,
        if_not_exists = true,
    })
    box.space[name]:create_index('primary', {
        type = 'TREE',
        unique = true,
        parts = {{'key', 'string'}, {'bucket_id', 'unsigned'}},
        if_not_exists = true,
    })
    -- The vshard rebalancer moves markers together with the data bucket.
    box.space[name]:create_index('bucket_id', {
        type = 'TREE',
        unique = false,
        parts = {{'bucket_id', 'unsigned'}},
        if_not_exists = true,
    })
    -- Ordered scan of the expired markers.
    box.space[name]:create_index('expires_at', {
        type = 'TREE',
        unique = false,
        parts = {{'expires_at', 'number'}},
        if_not_exists = true,
    })
end

-- Create the marker spaces on a master, wait for them on a replica.
-- Mirrors crud/schema.lua schema.init().
function spaces.ensure()
    ::init_start::
    if not box.info.ro then
        for engine, name in pairs(spaces.SPACES) do
            ensure_space(name, engine)
        end
    else
        while box.space[spaces.SPACES.memtx] == nil
              or box.space[spaces.SPACES.vinyl] == nil do
            fiber.sleep(0.05)
            if not box.info.ro then
                goto init_start
            end
        end
    end
end

function spaces.get(engine)
    return box.space[spaces.SPACES[engine]]
end

return spaces
