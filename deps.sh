#!/bin/sh
# Call this script to install test dependencies

set -e

# Test dependencies:
tarantoolctl rocks install luatest 0.5.4
tarantoolctl rocks install luacheck 0.25.0
tarantoolctl rocks install luacov 0.13.0

# cluacov, luacov-coveralls and dependencies
tarantoolctl rocks install https://raw.githubusercontent.com/mpeterv/cluacov/master/cluacov-scm-1.rockspec
tarantoolctl rocks install https://raw.githubusercontent.com/LuaDist/dkjson/master/dkjson-2.5-2.rockspec
tarantoolctl rocks install https://raw.githubusercontent.com/keplerproject/luafilesystem/master/luafilesystem-scm-1.rockspec
tarantoolctl rocks install https://raw.githubusercontent.com/moteus/lua-path/master/rockspecs/lua-path-scm-0.rockspec
tarantoolctl rocks install https://raw.githubusercontent.com/moteus/luacov-coveralls/master/rockspecs/luacov-coveralls-scm-0.rockspec

tarantoolctl rocks install cartridge 2.7.1
tarantoolctl rocks make
