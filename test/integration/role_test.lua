local t = require('luatest')

local helpers = require('test.helper')

local g = t.group()

g.before_all(function(cg)
    helpers.skip_if_tarantool3_crud_roles_unsupported()

    cg.template_cfg = helpers.build_default_tarantool3_cluster_cfg('srv_select')
end)

g.before_each(function(cg)
    -- Tests are rather dangerous and may break the cluster,
    -- so it's safer to restart for each case.
    helpers.start_tarantool3_cluster(cg, cg.template_cfg)
    cg.router = cg.cluster:server('router')

    helpers.wait_crud_is_ready_on_cluster(cg, {backend = helpers.backend.CONFIG})
end)

g.after_each(function(cg)
    cg.cluster:drop()
end)

-- Use autoincrement id so one test would be able to call the helper multiple times.
local last_id = 0
local function basic_insert_get_object(cluster)
    last_id = last_id + 1

    cluster:server('router'):exec(function(id)
        local crud = require('crud')

        local _, err = crud.insert_object('customers',
            {
                id = id,
                name = 'Vincent',
                last_name = 'Brooks',
                age = 32,
                city = 'Babel',
            },
            {noreturn = true}
        )
        t.assert_equals(err, nil)

        local result, err = crud.get('customers', id, {mode = 'write'})
        t.assert_equals(err, nil)
        t.assert_equals(#result.rows, 1, 'Tuple found')

        local objects, err = crud.unflatten_rows(result.rows, result.metadata)
        t.assert_equals(err, nil)
        t.assert_equals(objects[1].id, id)
        t.assert_equals(objects[1].name, 'Vincent')
        t.assert_equals(objects[1].last_name, 'Brooks')
        t.assert_equals(objects[1].age, 32)
        t.assert_equals(objects[1].city, 'Babel')
    end, {last_id})
end

g.test_cluster_works_if_roles_enabled = function(cg)
    basic_insert_get_object(cg.cluster)
end

g.test_cluster_works_after_vshard_user_password_alter = function(cg)
    -- Alter the cluster.
    local cfg = cg.cluster:cfg()

    local old_password = cfg.credentials.users['storage'].password
    cfg.credentials.users['storage'].password = old_password .. '_new_suffix'

    cg.cluster:reload_config(cfg)

    -- Wait until ready.
    helpers.wait_crud_is_ready_on_cluster(cg, {backend = helpers.backend.CONFIG})

    -- Check everything is fine.
    basic_insert_get_object(cg.cluster)
end

g.test_cluster_works_after_vshard_user_alter = function(cg)
    -- Alter the cluster.
    local cfg = cg.cluster:cfg()

    cfg.credentials.users['storage'] = nil
    cfg.credentials.users['new_storage'] = {
        password = 'storing-buckets-instead-of-storage',
        roles = {'sharding'},
    }

    cfg.iproto.advertise.sharding.login = 'new_storage'

    cg.cluster:reload_config(cfg)

    -- Wait until ready.
    helpers.wait_crud_is_ready_on_cluster(cg, {backend = helpers.backend.CONFIG})

    -- Check everything is fine.
    basic_insert_get_object(cg.cluster)
end
