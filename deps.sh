#!/usr/bin/env bash
# Call this script to install test dependencies
# Usage examples:
#   CARTRIDGE_VERSION=2.16.3 ./deps.sh
#   VSHARD_VERSION=0.1.36 ./deps.sh

set -e

TTCTL=tt
if ! [ -x "$(command -v tt)" ];
then
    echo "tt not found"
    exit 1
fi

# Test dependencies:
${TTCTL} rocks install luatest 1.0.1
${TTCTL} rocks install luacheck 0.26.0

if [[ -n "${CARTRIDGE_VERSION}" ]]
then
    ${TTCTL} rocks install https://raw.githubusercontent.com/luarocks/cluacov/master/cluacov-dev-1.rockspec
    ${TTCTL} rocks install https://luarocks.org/manifests/dhkolf/dkjson-2.8-1.rockspec
    ${TTCTL} rocks install https://raw.githubusercontent.com/keplerproject/luafilesystem/master/luafilesystem-scm-1.rockspec
    ${TTCTL} rocks install https://raw.githubusercontent.com/moteus/lua-path/master/rockspecs/lua-path-scm-0.rockspec
    ${TTCTL} rocks install luacov-coveralls --only-server=https://luarocks.org/

    ${TTCTL} rocks install cartridge "${CARTRIDGE_VERSION}"
    ${TTCTL} rocks install migrations 1.1.0
else
    ${TTCTL} rocks install vshard "${VSHARD_VERSION:-0.1.36}"
fi

${TTCTL} rocks install ddl 1.7.1

${TTCTL} rocks make