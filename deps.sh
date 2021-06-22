#!/bin/sh
# Call this script to install test dependencies

set -e

# Test dependencies:
tarantoolctl rocks install luatest 0.5.3
tarantoolctl rocks install luacheck 0.25.0

tarantoolctl rocks install cartridge 2.5.1
tarantoolctl rocks make
