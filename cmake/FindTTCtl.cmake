find_program(TTCTL
    NAMES tt tarantoolctl
    DOC "Utility for managing Tarantool packages"
)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(TTCtl
    REQUIRED_VARS TTCTL
)

mark_as_advanced(TTCTL)
