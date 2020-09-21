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
