-- test for https://github.com/tarantool/crud-ee/issues/16

local t = require('luatest')
local vshard_utils = require('crud.common.vshard_utils')

local g = t.group('get_self_vshard_replicaset')

g.before_each(function(cg)
    cg.__get_storage_info = vshard_utils.__get_storage_info
    cg.__get_box_info = vshard_utils.__get_box_info
end)

g.after_each(function(cg)
    vshard_utils.__get_storage_info = cg.__get_storage_info
    vshard_utils.__get_box_info = cg.__get_box_info
end)

local storage_info_with_instances_names = {
    replicasets = {
        ["storage-1"] = {
            master = "auto",
            uuid = "ae2de070-8769-4ffa-9942-8141ce0b78cc",
            name = "storage-1"
        },
        ["storage-2"] = {
            master = "auto",
            uuid = "ef85f92c-9ad7-4bb3-b27a-8f1edb440ce3",
            name = "storage-2"
        }
    },
    bucket = {
        receiving = 0,
        active = 0,
        total = 0,
        garbage = 0,
        pinned = 0,
        sending = 0
    },
    uri = "admin@localhost:3303",
    identification_mode = "name_as_key",
    status = 0,
    replication = {
        status = "master"
    },
    alerts = {},
}

local box_info_with_instances_names = {
    version = "3.1.0-0-g663f509a2",
    id = 2,
    ro = true,
    uuid = "2cbd467d-4026-4f85-968d-622dea28fe5a",
    pid = 1,
    replicaset = {
        uuid = "764f6e67-17f7-4deb-a3f0-784436b0327d",
        name = "storage-1"
    },
    schema_version = 92,
    listen = "172.21.0.14:3301",
    replication_anon = {
        count = 0
    },
    replication = {
        {
            id = 1,
            uuid = "a56e901d-841f-46b8-99cf-0943fcc960b9",
            lsn = 30149,
            upstream = {
                status = "follow",
                idle = 0.047537165999984,
                peer = "replicator@tarantool-storage-1-msk:3301",
                lag = 0.00072526931762695,
                name = "storage-1-msk"
            },
            downstream = {
                status = "follow",
                idle = 0.54811758300002,
                vclock = { [1] = 30149 },
                lag = 0
            }
        },
        {
            id = 2,
            uuid = "2cbd467d-4026-4f85-968d-622dea28fe5a",
            lsn = 0,
            name = "storage-1-spb"
        },
        {
            id = 3,
            uuid = "b0b1037f-95e9-491b-b37c-298bed9286e9",
            lsn = 0,
            upstream = {
                status = "follow",
                idle = 0.97080358300002,
                peer = "replicator@tarantool-storage-1-brn:3301",
                lag = 0.00027346611022949,
                name = "storage-1-brn"
            },
            downstream = {
                status = "follow",
                idle = 0.94219958300005,
                vclock = { [1] = 30149 },
                lag = 0
            },
        },
    },
    hostname = "10848594b67d",
    election = {
        state = "follower",
        vote = 0,
        leader = 0,
        term = 1,
        signature = 30149,
    },
    synchro = {
        queue = {
            owner = 0,
            term = 0,
            len = 0,
            busy = false
        },
        quorum = 2,
        status = "running"
    },
    sql = {},
    vclock = { [1] = 30149 },
    uptime = 547,
    lsn = 0,
    vinyl = {},
    ro_reason = "config",
    memory = {},
    gc = {},
    cluster = {
        name = "storage-1-spb"
    },
    package = "Tarantool Enterprise"
}

local box_info_with_instances_uuids = {
    version = "2.11.3-0-ge45691111",
    id = 1,
    ro = true,
    uuid = "ac9fcc18-e7e2-471b-bf46-97680a3615ad",
    pid = 5312,
    cluster = {
        uuid = "ae2de070-8769-4ffa-9942-8141ce0b78cc"
    },
    schema_version = 114,
    listen = "127.0.0.1:3303",
    replication_anon = {
        count = 0
    },
    replication = {
        {
            id = 1,
            uuid = "ac9fcc18-e7e2-471b-bf46-97680a3615ad",
            lsn = 30141
        },
        {
            id = 2,
            uuid = "14de6d0b-78ae-43c6-9f75-e41e15b72ff0",
            lsn = 0,
            upstream = {
                status = "follow",
                idle = 0.52450900012627,
                peer = "admin@localhost:3304",
                lag = 9.8943710327148e-05
            },
            downstream = {
                status = "follow",
                idle = 0.49833300011232,
                vclock = { [1] = 30141 },
                lag = 0
            }
        }
    },
    election = {
        state = "follower",
        vote = 0,
        leader = 0,
        term = 1,
        signature = 30141
    },
    synchro = {
        queue = {
            owner = 0,
            term = 0,
            len = 0,
            busy = false
        },
        quorum = 2,
        status = "running"
    },
    vclock = { [1] = 30141 },
    uptime = 272,
    lsn = 30141,
    ro_reason = "config",
    sql = {},
    gc = {},
    vinyl = {},
    memory = {},
    package = "Tarantool Enterprise"
}


local storage_info_with_instances_uuids = {
    replicasets = {
        ["ef85f92c-9ad7-4bb3-b27a-8f1edb440ce3"] = {
            master = nil,
            uri = "admin@localhost:3306",
            uuid = "ef85f92c-9ad7-4bb3-b27a-8f1edb440ce3"
        },
        ["ae2de070-8769-4ffa-9942-8141ce0b78cc"] = {
            master = nil,
            uri = "admin@localhost:3304",
            uuid = "ae2de070-8769-4ffa-9942-8141ce0b78cc"
        }
    },
    bucket = {
        receiving = 0,
        active = 15000,
        total = 15000,
        garbage = 0,
        pinned = 0,
        sending = 0
    },
    uri = "admin@localhost:3303",
    identification_mode = "uuid_as_key",
    status = 0,
    replication = {
        status = "follow",
        lag = 0.00037693977355957
    },
    alerts = {}
}

local storage_info_with_instances_names_on_2_11_upgrage = {
    replicasets = {
        ["storage-1"] = {
            master = "auto",
            uuid = "ae2de070-8769-4ffa-9942-8141ce0b78cc",
            name = "storage-1"
        },
        ["storage-2"] = {
            master = "auto",
            uuid = "ef85f92c-9ad7-4bb3-b27a-8f1edb440ce3",
            name = "storage-2"
        }
    },
    bucket = {
        receiving = 0,
        active = 15000,
        total = 15000,
        garbage = 0,
        pinned = 0,
        sending = 0
    },
    uri = "admin@localhost:3303",
    identification_mode = "name_as_key",
    status = 2,
    replication = {
        status = "master"
    },
    alerts = {
        {"UNREACHABLE_REPLICA", "Replica cdata<void *>: NULL isn't active"},
        {"LOW_REDUNDANCY", "Only one replica is active"}
    }
}

local box_info_with_instances_names_on_2_11_upgrage = {
    version = "3.1.0-0-g663f509a2",
    id = 1,
    ro = false,
    uuid = "ac9fcc18-e7e2-471b-bf46-97680a3615ad",
    pid = 23500,
    cluster = {
        name = box.NULL
    },
    schema_version = 114,
    listen = "[::1]:3303",
    replication_anon = {
        count = 0
    },
    replication = {
        {
            id = 1,
            uuid = "ac9fcc18-e7e2-471b-bf46-97680a3615ad",
            lsn = 30141,
            name = box.NULL
        },
        {
            id = 2,
            uuid = "14de6d0b-78ae-43c6-9f75-e41e15b72ff0",
            lsn = 0,
            upstream = {
                status = "follow",
                idle = 0.017113999929279,
                peer = "admin@localhost:3304",
                lag = 0.00025582313537598
            },
            name = box.NULL,
            downstream = {
                status = "follow",
                idle = 0.042480000061914,
                vclock = { [1] = 30141 },
                lag = 0
            }
        }
    },
    election = {
        state = "follower",
        vote = 0,
        leader = 0,
        term = 1
    },
    signature = 30141,
    synchro = {
        queue = {
            owner = 0,
            term = 0,
            len = 0,
            busy = false
        },
        quorum = 2
    },
    status = "running",
    hostname = "hostname",
    vclock = { [1] = 30141 },
    uptime = 2398,
    lsn = 30141,
    sql = {},
    vinyl = {},
    memory = {},
    gc = {},
    replicaset = {
        uuid = "ae2de070-8769-4ffa-9942-8141ce0b78cc",
        name = box.NULL
    },
    name = box.NULL,
    package = "Tarantool Enterprise"
}

g.test_use_names = function()
    -- happens when tarantool 3.1 starts on 3.1 data
    vshard_utils.__get_box_info = function()
        return box_info_with_instances_names
    end
    vshard_utils.__get_storage_info = function()
       return true, storage_info_with_instances_names
    end
    vshard_utils.is_schema_needs_upgrade_from_2_11 = function()
        return false
    end
    vshard_utils.get_vshard_identification_mode = function()
        return "name_as_key"
    end
    local name, respicaset = vshard_utils.get_self_vshard_replicaset()
    t.assert_equals(name, "storage-1")
    t.assert_equals(respicaset, {master = "auto", name = "storage-1", uuid = "ae2de070-8769-4ffa-9942-8141ce0b78cc"})
end

g.test_before_2_11_upgrage = function()
    -- happens when tarantool 3.1 starts on 2.11 data
    vshard_utils.__get_box_info = function()
        return box_info_with_instances_names_on_2_11_upgrage
    end
    vshard_utils.__get_storage_info = function()
       return true, storage_info_with_instances_names_on_2_11_upgrage
    end
    vshard_utils.is_schema_needs_upgrade_from_2_11 = function()
        return true
    end
    vshard_utils.get_vshard_identification_mode = function()
        return 'name_as_key'
    end
    local uuid, respicaset = vshard_utils.get_self_vshard_replicaset()
    t.assert_equals(uuid, "ae2de070-8769-4ffa-9942-8141ce0b78cc")
    t.assert_equals(respicaset, {
        master = "auto",
        name = "storage-1",
        uuid = "ae2de070-8769-4ffa-9942-8141ce0b78cc",
    })
end

g.test_use_uuid = function()
    -- happens when tarantool 2.11 starts on 2.11 data
    vshard_utils.__get_box_info = function()
        return box_info_with_instances_uuids
    end
    vshard_utils.__get_storage_info = function()
       return true, storage_info_with_instances_uuids
    end
    vshard_utils.is_schema_needs_upgrade_from_2_11 = function()
        return false
    end
    vshard_utils.get_vshard_identification_mode = function()
        return 'uuid_as_key'
    end
    local uuid, respicaset = vshard_utils.get_self_vshard_replicaset()
    t.assert_equals(uuid, "ae2de070-8769-4ffa-9942-8141ce0b78cc")
    t.assert_equals(respicaset, {uri = "admin@localhost:3304", uuid = "ae2de070-8769-4ffa-9942-8141ce0b78cc"})
end
