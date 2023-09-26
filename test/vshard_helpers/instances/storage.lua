local fio = require('fio')

local appdir = fio.abspath(debug.sourcedir() .. '/../../../')
if package.setsearchroot ~= nil then
    package.setsearchroot(appdir)
else
    package.path = package.path .. appdir .. '/?.lua;'
    package.path = package.path .. appdir .. '/?/init.lua;'
    package.path = package.path .. appdir .. '/.rocks/share/tarantool/?.lua;'
    package.path = package.path .. appdir .. '/.rocks/share/tarantool/?/init.lua;'
    package.cpath = package.cpath .. appdir .. '/?.so;'
    package.cpath = package.cpath .. appdir .. '/?.dylib;'
    package.cpath = package.cpath .. appdir .. '/.rocks/lib/tarantool/?.so;'
    package.cpath = package.cpath .. appdir .. '/.rocks/lib/tarantool/?.dylib;'
end

local utils = require('test.vshard_helpers.instances.utils')

-- It is not necessary in fact, but simplify `callrw` calls in tests.
_G.vshard = {
    storage = require('vshard.storage'),
}

-- Somewhy shutdown hangs on new Tarantools even though the nodes do not seem to
-- have any long requests running.
if box.ctl.set_on_shutdown_timeout then
    box.ctl.set_on_shutdown_timeout(0.001)
end

box.cfg(utils.box_cfg())
box.schema.user.grant('guest', 'super', nil, nil, {if_not_exists = true})

_G.ready = true
