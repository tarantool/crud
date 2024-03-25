local fio = require('fio')

local ROOT = fio.dirname(fio.dirname(fio.abspath(package.search('test.path'))))

local LUA_PATH = ROOT .. '/?.lua;' ..
    ROOT .. '/?/init.lua;' ..
    ROOT .. '/.rocks/share/tarantool/?.lua;' ..
    ROOT .. '/.rocks/share/tarantool/?/init.lua'

return {
    ROOT = ROOT,
    LUA_PATH = LUA_PATH,
}
