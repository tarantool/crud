local t = require('luatest')

local helpers = require('test.helper')
local net_box = require('net.box')
local group_of_tests = t.group(nil, {
    {
        backend = helpers.backend.VSHARD,
        backend_cfg = nil,
        space_access_granted = true,
    },
    {
        backend = helpers.backend.VSHARD,
        backend_cfg = nil,
        space_access_granted = false,
    },
})

local ORIGINAL_ROWS = {
    { id = 1, name = "Elizabeth", last_name = "Jackson", age = 12, city = "New York", },
    { id = 2, name = "Mary", last_name = "Brown", age = 46, city = "Los Angeles", },
    { id = 3, name = "David", last_name = "Smith", age = 33, city = "Los Angeles", },
    { id = 4, name = "William", last_name = "White", age = 81, city = "Chicago", },
    { id = 5, name = "James", last_name = "Johnson", age = 29, city = "Houston", },
    { id = 6, name = "Patricia", last_name = "Miller", age = 54, city = "Phoenix", },
    { id = 7, name = "Robert", last_name = "Davis", age = 40, city = "Philadelphia", },
    { id = 8, name = "Jennifer", last_name = "Garcia", age = 25, city = "San Antonio", },
    { id = 9, name = "Michael", last_name = "Martinez", age = 37, city = "San Diego", },
    { id = 10, name = "Linda", last_name = "Hernandez", age = 62, city = "Dallas", },
    { id = 11, name = "Charles", last_name = "Lopez", age = 50, city = "San Jose", },
    { id = 12, name = "Barbara", last_name = "Gonzalez", age = 45, city = "Austin", },
    { id = 13, name = "Joseph", last_name = "Wilson", age = 34, city = "Jacksonville", },
    { id = 14, name = "Susan", last_name = "Anderson", age = 28, city = "Fort Worth", },
    { id = 15, name = "Thomas", last_name = "Thomas", age = 70, city = "Columbus", },
    { id = 16, name = "Jessica", last_name = "Taylor", age = 31, city = "Charlotte", },
}

group_of_tests.before_all(function(g)
    if (not helpers.tarantool_version_at_least(2, 11, 0))
    or (not require('luatest.tarantool').is_enterprise_package()) then
        t.skip('Readview is supported only for Tarantool Enterprise starting from v2.11.0')
    end
    helpers.start_default_cluster(g, 'srv_select')

    g.space_format = g.cluster:server('s1-master').net_box.space.customers:format()
end)

group_of_tests.after_all(function(g)
    helpers.stop_cluster(g.cluster, g.params.backend)
end)

group_of_tests.before_each(function(g)
    helpers.truncate_space_on_cluster(g.cluster, 'customers')
end)

local function privilegies_test_base_init(g, access_operation_type)
    helpers.insert_objects(g, 'customers', ORIGINAL_ROWS)
    helpers.exec_on_cluster(g.cluster, function(access_operation_type)
        if not box.cfg.read_only then
            local user = box.session.effective_user()
            box.session.su('admin')

            box.schema.func.create('read_view_select', {
                language = 'LUA',
                if_not_exists = true,
                body = [[
                    function()
                        local rv = crud.readview()
                        local result, err = rv:select("customers")
                        rv:close()

                        return result, err
                    end
                ]]
            })

            box.schema.func.create('read_view_pairs', {
                language = 'LUA',
                if_not_exists = true,
                body = [[
                    function()
                        local rv = crud.readview()
                        local rows = {}
                        for _, row in rv:pairs('customers', {{'<=', 'age', 35}}, {use_tomap = true}) do
                            table.insert(rows, row)
                        end
                        rv:close()

                        return rows
                    end
                ]]
            })

            if box.schema.user.exists('testuser1') and box.space.customers then
                box.schema.user.revoke('testuser1', 'read,write', 'space', 'customers', {if_exists = true})
            end
            box.schema.user.drop('testuser1', {if_exists = true})

            box.schema.user.create('testuser1', { password = 'secret' })
            if _TARANTOOL >= '3.0.0' then
                box.schema.user.grant('testuser1', 'execute', 'lua_call', 'box.session.user')
                box.schema.user.grant('testuser1', 'execute', 'lua_call', 'crud.select')
                box.schema.user.grant('testuser1', 'execute', 'lua_call', 'crud.get')
                box.schema.user.grant('testuser1', 'execute', 'lua_call', 'crud.insert_object')
                box.schema.user.grant('testuser1', 'execute', 'lua_call', 'crud.insert_object_many')
                box.schema.user.grant('testuser1', 'execute', 'lua_call', 'crud.replace_object')
                box.schema.user.grant('testuser1', 'execute', 'lua_call', 'crud.replace_object_many')
                box.schema.user.grant('testuser1', 'execute', 'lua_call', 'crud.update')
                box.schema.user.grant('testuser1', 'execute', 'lua_call', 'crud.upsert_object')
                box.schema.user.grant('testuser1', 'execute', 'lua_call', 'crud.upsert_object_many')
                box.schema.user.grant('testuser1', 'execute', 'lua_call', 'crud.delete')
                box.schema.user.grant('testuser1', 'execute', 'lua_call', 'crud.truncate')
                box.schema.user.grant('testuser1', 'execute', 'lua_call', 'crud.len')
                box.schema.user.grant('testuser1', 'execute', 'lua_call', 'crud.count')
                box.schema.user.grant('testuser1', 'execute', 'lua_call', 'crud.min')
                box.schema.user.grant('testuser1', 'execute', 'lua_call', 'crud.max')

                box.schema.user.grant('testuser1', 'execute', 'function', 'read_view_select')
                box.schema.user.grant('testuser1', 'execute', 'function', 'read_view_pairs')
            else
                box.schema.user.grant('testuser1', 'execute', 'universe')
            end

            if access_operation_type and box.space.customers then
                box.schema.user.grant('testuser1', access_operation_type, 'space', 'customers')
            end
            if box.space._bucket then
                box.schema.user.grant('testuser1', 'read', 'space', '_bucket')
            end

            box.session.su(user)
        end
    end, {g.params.space_access_granted and access_operation_type})

    local conn = net_box.connect(
        g.router.net_box_uri,
        {
            user = "testuser1",
            password = "secret"
        }
    )
    t.assert_not_equals(conn, nil)
    t.assert_equals(conn:is_connected(), true, conn.error)

    return conn
end

local function tomap(tuple)
    return {
        id = tuple[1],
        bucket_id = nil,
        name = tuple[3],
        last_name = tuple[4],
        age = tuple[5],
        city = tuple[6],
    }
end

group_of_tests.test_read_view_select = function(g)
    local conn = privilegies_test_base_init(g, "read")

    local ok, res, err = pcall(conn.call, conn, "read_view_select")
    t.assert_equals(ok, true, tostring(res))

    if g.params.space_access_granted then
        t.assert_equals(err, nil, err)
        t.assert_equals(#res.rows, #ORIGINAL_ROWS)
    else
        t.assert_not_equals(err, nil)
        t.assert_equals(type(err), "table")
        t.assert_str_contains(err.str, "ReadviewError: Space \"customers\" doesn't exist")

        t.assert_equals(res, nil)
    end
end

group_of_tests.test_read_view_pairs = function(g)
    local conn = privilegies_test_base_init(g, "read")

    local ok, res, err = pcall(conn.call, conn, "read_view_pairs")
    if g.params.space_access_granted then
        t.assert_equals(ok, true, tostring(res))
        t.assert_equals(err, nil, err)
        t.assert_equals(#res, 7)
    else
        t.assert_equals(ok, false)
        t.assert_str_contains(tostring(res), "ReadviewError: Space \"customers\" doesn't exist")
    end
end

group_of_tests.test_select = function(g)
    local conn = privilegies_test_base_init(g, "read")

    local ok, res, err = pcall(conn.call, conn, "crud.select", {"customers"})
    t.assert_equals(ok, true, tostring(res))

    if g.params.space_access_granted then
        t.assert_equals(err, nil, err)
        t.assert_equals(#res.rows, #ORIGINAL_ROWS)
    else
        t.assert_not_equals(err, nil)
        t.assert_equals(type(err), "table")
        t.assert_str_contains(err.str, "Space '#514' does not exist")
        --TODO: После исправления TNTP-2295 использовать это проверку вместо предыдущей
        --t.assert_str_contains(err.str, "Read access to space 'customers' is denied for user 'testuser1'")

        t.assert_equals(res, nil)
    end
end

group_of_tests.test_insert = function(g)
    local reference_record = {
        id = 17,
        name = "Ivan",
        last_name = "Ivanovitch",
        age = 42,
        city = "Barnaul",
    }

    local conn = privilegies_test_base_init(g, "write")

    local ok, res, err = pcall(conn.call, conn, "crud.insert_object", {"customers", reference_record})
    t.assert_equals(ok, true, tostring(res))

    local actual_rows, err2 = g.router:call("crud.get", {"customers", 17})
    t.assert_equals(err2, nil, err2)

    if g.params.space_access_granted then
        t.assert_equals(err, nil, err)
        t.assert_equals(#actual_rows.rows, 1)

        local actual_row = tomap(actual_rows.rows[1])
        t.assert_equals(actual_row, reference_record)
    else
        t.assert_not_equals(err, nil)
        t.assert_equals(type(err), "table")
        t.assert_str_contains(err.str, "Write access to space 'customers' is denied for user 'testuser1'")

        t.assert_equals(#actual_rows.rows, 0)
    end
end

group_of_tests.test_insert_many = function(g)
    local reference_record_list = {
        { id = 17, name = "Анна", last_name = "Иванова", age = 25, city = "Москва", },
        { id = 18, name = "محمد", last_name = "الزهراني", age = 40, city = "الرياض", },
        { id = 19, name = "Sophie", last_name = "Lefevre", age = 33, city = "Paris", },
        { id = 20, name = "Luca", last_name = "Rossi", age = 29, city = "Roma", },
        { id = 21, name = "Ming", last_name = "Wang", age = 45, city = "北京", },
        { id = 22, name = "Hiroshi", last_name = "Tanaka", age = 50, city = "東京", },
        { id = 23, name = "Carlos", last_name = "Fernández", age = 38, city = "Madrid", },
        { id = 24, name = "Fatima", last_name = "El Amrani", age = 27, city = "Casablanca", },
        { id = 25, name = "Johannes", last_name = "Schmidt", age = 60, city = "Berlin", },
        { id = 26, name = "Aarav", last_name = "Patel", age = 35, city = "Mumbai", },
        { id = 27, name = "Emily", last_name = "Smith", age = 22, city = "London", },
        { id = 28, name = "Mateo", last_name = "Gómez", age = 41, city = "Buenos Aires", },
        { id = 29, name = "Olga", last_name = "Petrova", age = 55, city = "Санкт-Петербург", },
        { id = 30, name = "Johan", last_name = "Andersson", age = 48, city = "Stockholm", },
        { id = 31, name = "Isabella", last_name = "Silva", age = 30, city = "São Paulo", },
        { id = 32, name = "Noah", last_name = "Dubois", age = 26, city = "Montréal", },
    }

    local conn = privilegies_test_base_init(g, "write")

    local ok, res, err = pcall(conn.call, conn, "crud.insert_object_many", {"customers", reference_record_list})
    t.assert_equals(ok, true, tostring(res))

    local actual_rows_qty, err2 = g.router:call("crud.len", {"customers"})
    t.assert_equals(err2, nil, err2)

    if g.params.space_access_granted then
        t.assert_equals(err, nil, err)
        t.assert_equals(actual_rows_qty, 32)
    else
        t.assert_not_equals(err, nil)
        t.assert_equals(type(err), "table")
        for i = 1, #reference_record_list do
            t.assert_str_contains(err[i].err, "Write access to space 'customers' is denied for user 'testuser1'")
        end

        t.assert_equals(actual_rows_qty, 16)
    end
end

group_of_tests.test_replace = function(g)
    local reference_record = {
        id = 1,
        name = "Ivan",
        last_name = "Ivanovitch",
        age = 42,
        city = "Barnaul",
    }

    local conn = privilegies_test_base_init(g, "write")

    local ok, res, err = pcall(conn.call, conn, "crud.replace_object", {"customers", reference_record})
    t.assert_equals(ok, true, tostring(res))

    local actual_rows, err2 = g.router:call("crud.get", {"customers", 1})
    t.assert_equals(err2, nil, err2)
    t.assert_equals(#actual_rows.rows, 1)

    local actual_row = tomap(actual_rows.rows[1])

    if g.params.space_access_granted then
        t.assert_equals(err, nil, err)
        t.assert_equals(actual_row, reference_record)
    else
        t.assert_not_equals(err, nil)
        t.assert_equals(type(err), "table")
        t.assert_str_contains(err.str, "Write access to space 'customers' is denied for user 'testuser1'")

        t.assert_equals(actual_row, ORIGINAL_ROWS[1])
    end
end

group_of_tests.test_replace_many = function(g)
    local reference_record_list = {
        { id = 1, name = "Анна", last_name = "Иванова", age = 25, city = "Москва", },
        { id = 2, name = "محمد", last_name = "الزهراني", age = 40, city = "الرياض", },
        { id = 3, name = "Sophie", last_name = "Lefevre", age = 33, city = "Paris", },
        { id = 4, name = "Luca", last_name = "Rossi", age = 29, city = "Roma", },
        { id = 5, name = "Ming", last_name = "Wang", age = 45, city = "北京", },
        { id = 6, name = "Hiroshi", last_name = "Tanaka", age = 50, city = "東京", },
        { id = 7, name = "Carlos", last_name = "Fernández", age = 38, city = "Madrid", },
        { id = 8, name = "Fatima", last_name = "El Amrani", age = 27, city = "Casablanca", },
        { id = 9, name = "Johannes", last_name = "Schmidt", age = 60, city = "Berlin", },
        { id = 10, name = "Aarav", last_name = "Patel", age = 35, city = "Mumbai", },
        { id = 11, name = "Emily", last_name = "Smith", age = 22, city = "London", },
        { id = 12, name = "Mateo", last_name = "Gómez", age = 41, city = "Buenos Aires", },
        { id = 13, name = "Olga", last_name = "Petrova", age = 55, city = "Санкт-Петербург", },
        { id = 14, name = "Johan", last_name = "Andersson", age = 48, city = "Stockholm", },
        { id = 15, name = "Isabella", last_name = "Silva", age = 30, city = "São Paulo", },
        { id = 16, name = "Noah", last_name = "Dubois", age = 26, city = "Montréal", },
    }

    local conn = privilegies_test_base_init(g, "write")

    local ok, res, err = pcall(conn.call, conn, "crud.replace_object_many", {"customers", reference_record_list})
    t.assert_equals(ok, true, tostring(res))

    local actual_tuples, err2 = g.router:call("crud.select", {"customers"})
    t.assert_equals(err2, nil, err2)
    t.assert_equals(#actual_tuples.rows, 16)

    local actual_rows = {}
    for _, tuple in ipairs(actual_tuples.rows) do
        table.insert(actual_rows, tomap(tuple))
    end

    table.sort(actual_rows, function(a, b) return a.id < b.id  end)

    if g.params.space_access_granted then
        t.assert_equals(err, nil, err)
        t.assert_equals(actual_rows, reference_record_list)
    else
        t.assert_not_equals(err, nil)
        t.assert_equals(type(err), "table")
        for i = 1, #reference_record_list do
            t.assert_str_contains(err[i].err, "Write access to space 'customers' is denied for user 'testuser1'")
        end

        t.assert_equals(actual_rows, ORIGINAL_ROWS)
    end
end

group_of_tests.test_update = function(g)
    local reference_record = {
        id = 1, name = "Elizabeth", last_name = "Jackson",
        age = 13, city = "New York",
    }

    local conn = privilegies_test_base_init(g, "write")

    local ok, res, err = pcall(conn.call, conn, "crud.update", {"customers", 1, {{'+', 'age', 1}}})
    t.assert_equals(ok, true, tostring(res))

    local actual_rows, err2 = g.router:call("crud.get", {"customers", 1})
    t.assert_equals(err2, nil, err2)
    t.assert_equals(#actual_rows.rows, 1)

    local actual_row = tomap(actual_rows.rows[1])

    if g.params.space_access_granted then
        t.assert_equals(err, nil, err)
        t.assert_equals(actual_row, reference_record)
    else
        t.assert_not_equals(err, nil)
        t.assert_equals(type(err), "table")
        t.assert_str_contains(err.str, "Write access to space 'customers' is denied for user 'testuser1'")

        t.assert_equals(actual_row, ORIGINAL_ROWS[1])
    end
end

group_of_tests.test_upsert = function(g)
    local reference_record = {
        id = 1, name = "Elizabeth", last_name = "Jackson",
        age = 13, city = "New York",
    }

    local conn = privilegies_test_base_init(g, "write")

    local ok, res, err = pcall(conn.call, conn, "crud.upsert_object", {
        "customers",
        reference_record,
        {{'+', 'age', 1}},
    })
    t.assert_equals(ok, true, tostring(res))

    local actual_rows, err2 = g.router:call("crud.get", {"customers", 1})
    t.assert_equals(err2, nil, err2)
    t.assert_equals(#actual_rows.rows, 1)

    local actual_row = tomap(actual_rows.rows[1])

    if g.params.space_access_granted then
        t.assert_equals(err, nil, err)
        t.assert_equals(actual_row, reference_record)
    else
        t.assert_not_equals(err, nil)
        t.assert_equals(type(err), "table")
        t.assert_str_contains(err.str, "Write access to space 'customers' is denied for user 'testuser1'")

        t.assert_equals(actual_row, ORIGINAL_ROWS[1])
    end
end

group_of_tests.test_delete = function(g)
    local conn = privilegies_test_base_init(g, "write")

    local ok, res, err = pcall(conn.call, conn, "crud.delete", {"customers", 1})
    t.assert_equals(ok, true, tostring(res))

    local actual_rows, err2 = g.router:call("crud.get", {"customers", 1})
    t.assert_equals(err2, nil, err2)

    if g.params.space_access_granted then
        t.assert_equals(err, nil, err)

        res, err = g.router:call("crud.get", {"customers", 1})
        t.assert_equals(err, nil, err)
        t.assert_equals(#res.rows, 0)
    else
        t.assert_not_equals(err, nil)
        t.assert_equals(type(err), "table")
        t.assert_str_contains(err.str, "Write access to space 'customers' is denied for user 'testuser1'")

        t.assert_equals(#actual_rows.rows, 1)
        local actual_row = tomap(actual_rows.rows[1])
        t.assert_equals(actual_row, ORIGINAL_ROWS[1])
    end
end

group_of_tests.test_get = function(g)
    local conn = privilegies_test_base_init(g, "read")

    local ok, res, err = pcall(conn.call, conn, "crud.get", {"customers", 1})
    t.assert_equals(ok, true, tostring(res))

    if g.params.space_access_granted then
        t.assert_equals(err, nil, err)
        t.assert_equals(#res.rows, 1)

        local actual_row1 = tomap(res.rows[1])
        t.assert_equals(actual_row1, ORIGINAL_ROWS[1])
    else
        t.assert_not_equals(err, nil)
        t.assert_equals(type(err), "table")
        t.assert_str_contains(err.str, "Read access to space 'customers' is denied for user 'testuser1'")
    end
end

group_of_tests.test_truncate = function(g)
    local conn = privilegies_test_base_init(g, "write")

    local ok, res, err = pcall(conn.call, conn, "crud.truncate", {"customers"})
    t.assert_equals(ok, true, tostring(res))

    local actual_rows, err2 = g.router:call("crud.select", {"customers"})
    t.assert_equals(err2, nil, err2)

    if g.params.space_access_granted then
        t.assert_equals(err, nil, err)
        t.assert_equals(#actual_rows.rows, 0)
    else
        t.assert_not_equals(err, nil)
        t.assert_equals(type(err), "table")
        t.assert_str_contains(err.str, "Write access to space 'customers' is denied for user 'testuser1'")

        t.assert_equals(#actual_rows.rows, #ORIGINAL_ROWS)
    end
end

group_of_tests.test_upsert_many = function(g)
    local reference_rows = {
        { id = 1, name = "Elizabeth", last_name = "Jackson", age = 13, city = "New York", },
        { id = 2, name = "Mary", last_name = "Brown", age = 47, city = "Los Angeles", },
    }

    local conn = privilegies_test_base_init(g, "write")

    local ok, res, err = pcall(conn.call, conn, "crud.upsert_object_many", {
        "customers", {
            {reference_rows[1], {{'+', 'age', 1}}},
            {reference_rows[2], {{'+', 'age', 1}}},
    }})
    t.assert_equals(ok, true, tostring(res))

    local actual_rows1, err2 = g.router:call("crud.get", {"customers", 1})
    t.assert_equals(err2, nil, err2)
    t.assert_equals(#actual_rows1.rows, 1)

    local actual_rows2, err3 = g.router:call("crud.get", {"customers", 2})
    t.assert_equals(err3, nil, err3)
    t.assert_equals(#actual_rows2.rows, 1)

    local actual_rows = {
        tomap(actual_rows1.rows[1]),
        tomap(actual_rows2.rows[1])
    }

    if g.params.space_access_granted then
        t.assert_equals(err, nil, err)
        t.assert_equals(actual_rows, reference_rows)
    else
        t.assert_not_equals(err, nil)
        t.assert_equals(type(err), "table")
        t.assert_str_contains(err[1].str, "Write access to space 'customers' is denied for user 'testuser1'")
        t.assert_str_contains(err[2].str, "Write access to space 'customers' is denied for user 'testuser1'")

        t.assert_equals(actual_rows, { ORIGINAL_ROWS[1], ORIGINAL_ROWS[2] })
    end
end

group_of_tests.test_len = function(g)
    local conn = privilegies_test_base_init(g, "read")

    local ok, res, err = pcall(conn.call, conn, "crud.len", {"customers"})
    t.assert_equals(ok, true, tostring(res))

    if g.params.space_access_granted then
        t.assert_equals(err, nil, err)
        t.assert_equals(res, #ORIGINAL_ROWS)
    else
        t.assert_not_equals(err, nil)
        t.assert_equals(type(err), "table")
        t.assert_str_contains(err.str, "Read access to space 'customers' is denied for user 'testuser1'")

        t.assert_equals(res, nil)
    end
end

group_of_tests.test_count = function(g)
    local conn = privilegies_test_base_init(g, "read")

    local ok, res, err = pcall(conn.call, conn, "crud.count", {"customers"})
    t.assert_equals(ok, true, tostring(res))

    if g.params.space_access_granted then
        t.assert_equals(err, nil, err)
        t.assert_equals(res, #ORIGINAL_ROWS)
    else
        t.assert_not_equals(err, nil)
        t.assert_equals(type(err), "table")
        t.assert_str_contains(err.str, "Read access to space 'customers' is denied for user 'testuser1'")

        t.assert_equals(res, nil)
    end
end

group_of_tests.test_min = function(g)
    local conn = privilegies_test_base_init(g, "read")

    local ok, res, err = pcall(conn.call, conn, "crud.min", {"customers"})
    t.assert_equals(ok, true, tostring(res))

    if g.params.space_access_granted then
        t.assert_equals(err, nil, err)

        local actual_row = tomap(res.rows[1])
        t.assert_equals(actual_row, ORIGINAL_ROWS[1])
    else
        t.assert_not_equals(err, nil)
        t.assert_equals(type(err), "table")
        t.assert_str_contains(err.str, "Read access to space 'customers' is denied for user 'testuser1'")

        t.assert_equals(res, nil)
    end
end

group_of_tests.test_max = function(g)
    local conn = privilegies_test_base_init(g, "read")

    local ok, res, err = pcall(conn.call, conn, "crud.max", {"customers"})
    t.assert_equals(ok, true, tostring(res))

    if g.params.space_access_granted then
        t.assert_equals(err, nil, err)

        local actual_row = tomap(res.rows[1])
        t.assert_equals(actual_row, ORIGINAL_ROWS[16])
    else
        t.assert_not_equals(err, nil)
        t.assert_equals(type(err), "table")
        t.assert_str_contains(err.str, "Read access to space 'customers' is denied for user 'testuser1'")

        t.assert_equals(res, nil)
    end
end
