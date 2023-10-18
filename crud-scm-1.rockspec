package = 'crud'
version = 'scm-1'
source  = {
    url = 'git+https://github.com/tarantool/crud.git',
    branch = 'master',
}

dependencies = {
    'lua ~> 5.1',
    'checks >= 3.1.0-1',
    'errors >= 2.2.1-1',
    'vshard >= 0.1.18-1',
    'tuple-merger >= 0.0.2',
    'tuple-keydef >= 0.0.2',
}

build = {
    type = 'cmake',
    variables = {
        version = 'scm-1',
        TARANTOOL_INSTALL_LUADIR = '$(LUADIR)',
    },
}
