cmake_minimum_required(VERSION 2.8 FATAL_ERROR)

project(crud NONE)

set(CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake" ${CMAKE_MODULE_PATH})
set(CMAKE_SKIP_INSTALL_ALL_DEPENDENCY TRUE)

file(GLOB_RECURSE LUA_FILES
  "${CMAKE_CURRENT_SOURCE_DIR}/crud.lua"
  "${CMAKE_CURRENT_SOURCE_DIR}/crud/*.lua"
  "${CMAKE_CURRENT_SOURCE_DIR}/cartridge/roles/*.lua"
)

## Testing ####################################################################
###############################################################################

enable_testing()

find_package(LuaCheck)
add_custom_target(luacheck
  COMMAND ${LUACHECK} ${PROJECT_SOURCE_DIR}
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
)

find_package(LuaTest)
find_package(LuaCov)
find_package(LuaCovCoveralls)

set(CODE_COVERAGE_REPORT "${PROJECT_SOURCE_DIR}/luacov.report.out")
set(CODE_COVERAGE_STATS "${PROJECT_SOURCE_DIR}/luacov.stats.out")

add_custom_target(luatest
  COMMAND ${LUATEST} -v --coverage
  BYPRODUCTS ${CODE_COVERAGE_STATS}
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
  COMMENT "Run regression tests"
)

set(PERFORMANCE_TESTS_SUBDIR "test/performance")

add_custom_target(performance
  COMMAND PERF_MODE_ON=true ${LUATEST} -v -c ${PERFORMANCE_TESTS_SUBDIR}
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
  COMMENT "Run performance tests"
)

add_custom_target(coverage
  COMMAND ${LUACOV} ${PROJECT_SOURCE_DIR} && grep -A999 '^Summary' ${CODE_COVERAGE_REPORT}
  DEPENDS ${CODE_COVERAGE_STATS}
  BYPRODUCTS ${CODE_COVERAGE_REPORT}
  WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}
  COMMENT "Generate code coverage stats"
)

if(DEFINED ENV{GITHUB_TOKEN})
  set(COVERALLS_COMMAND ${LUACOVCOVERALLS} -v -r ${PROJECT_SOURCE_DIR} --repo-token $ENV{GITHUB_TOKEN})
else()
  set(COVERALLS_COMMAND ${CMAKE_COMMAND} -E echo "Skipped uploading to coveralls.io: no token.")
endif()

add_custom_target(coveralls
  # Replace absolute paths with relative ones.
  # In command line: sed -i -e 's@'"$(realpath .)"'/@@'.
  COMMAND sed -i -e "\"s@\"'${PROJECT_SOURCE_DIR}'\"/@@\"" ${CODE_COVERAGE_STATS}
  COMMAND ${COVERALLS_COMMAND}
  DEPENDS ${CODE_COVERAGE_STATS}
  WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}
  COMMENT "Send code coverage data to the coveralls.io service"
)

## Install ####################################################################
###############################################################################

if(NOT DEFINED TARANTOOL_INSTALL_LUADIR)
    set(TARANTOOL_INSTALL_LUADIR "${PROJECT_SOURCE_DIR}/.rocks/share/tarantool")
endif()

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
