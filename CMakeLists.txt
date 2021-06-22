cmake_minimum_required(VERSION 2.8 FATAL_ERROR)

project(crud NONE)

set(CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake" ${CMAKE_MODULE_PATH})
set(CMAKE_SKIP_INSTALL_ALL_DEPENDENCY TRUE)

file(GLOB_RECURSE LUA_FILES
  "${CMAKE_CURRENT_SOURCE_DIR}/crud.lua"
  "${CMAKE_CURRENT_SOURCE_DIR}/crud/*.lua"
  "${CMAKE_CURRENT_SOURCE_DIR}/cartridge/roles/*.lua"
)

## Install ####################################################################
###############################################################################

install(
  DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/${PROJECT_NAME}
  DESTINATION ${TARANTOOL_INSTALL_LUADIR}
)

install(
  FILES ${CMAKE_CURRENT_SOURCE_DIR}/crud.lua
  DESTINATION ${TARANTOOL_INSTALL_LUADIR}
)

install(
  DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/cartridge
  DESTINATION ${TARANTOOL_INSTALL_LUADIR}
)

# Don't include to rockspec as some Tarantool versions (e.g. 2.2 and 2.3)
# don't have symbols required by "tuple-merger" and "tuple-keydef" modules.
execute_process(
    COMMAND bash "-c" "tarantoolctl rocks install tuple-keydef 0.0.2"
)

execute_process(
    COMMAND bash "-c" "tarantoolctl rocks install tuple-merger 0.0.2"
)
