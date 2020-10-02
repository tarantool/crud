package = 'crud'
version = 'scm-1'
source  = {
    url = 'git+https://github.com/tarantool/crud.git',
    branch = 'master',
}

dependencies = {
    'tarantool',
    'lua >= 5.1',
    'checks == 3.1.0-1',
    'errors == 2.1.3-1',
    'vshard == 0.1.16-1',
}

build = {
    type = 'cmake',
    variables = {
        version = 'scm-1',
        TARANTOOL_INSTALL_LUADIR = '$(LUADIR)',
    },
}
