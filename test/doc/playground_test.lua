local yaml = require('yaml')
local t = require('luatest')
local g = t.group()

local popen_ok, popen = pcall(require, 'popen')

g.before_all(function()
    t.skip_if(not popen_ok, 'no built-in popen module')
    t.skip_if(jit.os == 'OSX', 'popen is broken on Mac OS: ' ..
        'https://github.com/tarantool/tarantool/issues/6674')
end)

-- Run ./doc/playground.lua, execute a request and compare the
-- output with reference return values.
--
-- The first arguments is the request string. All the following
-- arguments are expected return values (as Lua values).
--
-- The function ignores trailing `null` values in the YAML
-- output.
local function check_request(request, ...)
    local ph, err = popen.new({'./doc/playground.lua'}, {
        stdin = popen.opts.PIPE,
        stdout = popen.opts.PIPE,
        stderr = popen.opts.DEVNULL,
    })
    if ph == nil then
        error('popen.new: ' .. tostring(err))
    end

    local ok, err = ph:write(request, {timeout = 1})
    if not ok then
        ph:close()
        error('ph:write: ' .. tostring(err))
    end
    ph:shutdown({stdin = true})

    -- Read everything until EOF.
    local chunks = {}
    while true do
        local chunk, err = ph:read()
        if chunk == nil then
            ph:close()
            error('ph:read: ' .. tostring(err))
        end
        if chunk == '' then break end -- EOF
        table.insert(chunks, chunk)
    end

    local status = ph:wait()
    assert(status.state == popen.state.EXITED)

    -- Glue all chunks, parse response.
    local stdout = table.concat(chunks)
    local response_yaml = string.match(stdout, '%-%-%-.-%.%.%.')
    local response = yaml.decode(response_yaml)

    -- NB: This call does NOT differentiate `nil` and `box.NULL`.
    t.assert_equals(response, {...})
end

local cases = {
    test_select_customers = {
        request = "crud.select('customers', {{'<=', 'age', 35}}, {first = 10})",
        retval_1 = {
            metadata = {
                {name = 'id', type = 'unsigned'},
                {name = 'bucket_id', type = 'unsigned'},
                {name = 'name', type = 'string'},
                {name = 'age', type = 'number'},
            },
            rows = {
                {5, 1172, 'Jack', 35},
                {3, 2804, 'David', 33},
                {6, 1064, 'William', 25},
                {7, 693, 'Elizabeth', 18},
                {1, 477, 'Elizabeth', 12},
            },
        }
    },
    test_select_developers = {
        request = "crud.select('developers', nil, {first = 6})",
        retval_1 = {
            metadata = {
                {name = 'id', type = 'unsigned'},
                {name = 'bucket_id', type = 'unsigned'},
                {name = 'name', type = 'string'},
                {name = 'surname', type = 'string'},
                {name = 'age', type = 'number'},
            },
            rows = {
                {1, 477, 'Alexey', 'Adams', 20},
                {2, 401, 'Sergey', 'Allred', 21},
                {3, 2804, 'Pavel', 'Adams', 27},
                {4, 1161, 'Mikhail', 'Liston', 51},
                {5, 1172, 'Dmitry', 'Jacobi', 16},
                {6, 1064, 'Alexey', 'Sidorov', 31},
            },
        },
    },
    test_insert = {
        request = ("crud.insert('developers', %s)"):format(
            "{100, nil, 'Alfred', 'Hitchcock', 123}"),
        retval_1 = {
            metadata = {
                {name = 'id', type = 'unsigned'},
                {name = 'bucket_id', type = 'unsigned'},
                {name = 'name', type = 'string'},
                {name = 'surname', type = 'string'},
                {name = 'age', type = 'number'},
            },
            rows = {
                {100, 2976, 'Alfred', 'Hitchcock', 123},
            },
        }
    },
    test_error = {
        request = [[
            do
                local res, err = crud.select('non_existent', nil, {first = 10})
                return res, err and err.err or nil
            end
        ]],
        retval_1 = box.NULL,
        retval_2 = 'Space "non_existent" doesn\'t exist',
    },
}

for case_name, case in pairs(cases) do
    g[case_name] = function()
        check_request(case.request, case.retval_1, case.retval_2)
    end
end
