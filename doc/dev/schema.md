# Database schema information design document

Two types of schema are used in ``crud`` requests: ``net.box`` spaces
schema and ``ddl`` sharding schema. If a change had occurred in one of
those, router instances should reload the schema and reevaluate
a request using an updated one. This document clarifies how schema
is obtained, used and reloaded.

## Table of Contents
<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->

- [Space schema](#space-schema)
  - [How schema is stored](#how-schema-is-stored)
  - [When schema is used](#when-schema-is-used)
  - [How schema is reloaded](#how-schema-is-reloaded)
  - [When schema is reloaded and operation is retried](#when-schema-is-reloaded-and-operation-is-retried)
  - [When schema is reloaded depending on the user option](#when-schema-is-reloaded-depending-on-the-user-option)
  - [Alternative approaches](#alternative-approaches)
- [Sharding schema](#sharding-schema)
  - [How schema is stored](#how-schema-is-stored-1)
  - [When schema is used](#when-schema-is-used-1)
  - [How schema is reloaded](#how-schema-is-reloaded-1)
  - [When schema is reloaded and operation is retried](#when-schema-is-reloaded-and-operation-is-retried-1)
  - [Alternative approaches](#alternative-approaches-1)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

## Space schema

Related links: [#98](https://github.com/tarantool/crud/issues/98),
[PR#111](https://github.com/tarantool/crud/pull/111).

### How schema is stored

Every ``crud`` router is a [``vshard``](https://www.tarantool.io/en/doc/latest/reference/reference_rock/vshard/)
router, the same applies to storages. In ``vshard`` clusters, spaces are
created on storages. Thus, each storage has a schema (space list,
space formats and indexes) on it.

Every router has a [``net.box``](https://www.tarantool.io/en/doc/latest/reference/reference_lua/net_box/)
connection to each storage it could interact with. (They can be
retrieved with [``vshard.router.routeall``](https://www.tarantool.io/en/doc/latest/reference/reference_rock/vshard/vshard_router/#lua-function.vshard.router.routeall)
call.) Each ``net.box`` connection has space schema for an instance it
is connected to. Router can access space schema by using ``net.box``
connection object contents.

Tarantool instance may have several vshard routers. For each request, only
one router is used (specified in `vshard_router` option or a default one).
Each router has its own ``net.box`` connections. Fetching and reload processes
are different for each router and do not affect each other.

### When schema is used

Space schema is used
- to flatten objects on `crud.*_object*` requests;
- to resolve update operation fields if updating
  by field name is not supported natively;
- to calculate ``bucket_id`` to choose replicaset for a request
  (together with sharding info);
- in `metadata` field of request response, so it could be used later
  for `crud.unflatten_rows`.

### How schema is reloaded

``net.box`` schema reload works as follows. For each request (we use ``call``s
to execute our procedures) from client (router) to server (storage) we
receive a schema version together with response data. If schema versions mismatch,
client reloads schema. The request is not retried. ``net.box`` reloads
schema before returning synchronous ``call`` result to a user, so a next
request in the fiber will use the updated schema. (See [tarantool/tarantool/6169](https://github.com/tarantool/tarantool/issues/6169).)

``crud`` cannot implicitly rely on ``net.box`` reloads: ``crud`` requests
use ``net.box`` spaces schema to build ``net.box`` ``call`` request data,
so if something is not relevant anymore, a part of this request data need
to be recalculated. ``crud`` uses connection ``reload_schema`` handle
(see [PR#111 comment](https://github.com/tarantool/crud/pull/111#issuecomment-765811556))
to ping the storage and updates `net.box` schema if it is outdated. ``crud``
reloads each replicaset schema. If there are several requests to reload info,
the only one reload fiber is started and every request waits for its completion.

### When schema is reloaded and operation is retried

The basic logic is as follows: if something had failed and space schema
mismatch could be the reason, reload the schema and retry. If it didn't
help after ``N`` retries (now ``N`` is ``1``), pass the error to the
user.

Retry with reload is triggered
- before a network request if an action that depends on the schema has failed:
  - space not found;
  - object flatten on `crud.*_object*` request has failed;
  - ``bucket_id`` calculation has failed due to any reason;
  - if updating by field name is not supported natively and id resolve
    has failed;
- network operation had failed on storage, hash check is on and hashes mismatch.

Let's talk a bit more about the last one. To enable hash check, the user
should pass `add_space_schema_hash` option to a request. This option is
always enabled for `crud.*_object*` requests. If hashes mismatch, it
means the router schema of this space is inconsistent with the storage
space schema, so we reload it. For ``*_many`` methods, reload and retry
happens only if all tuples had failed with hash mismatch; otherwise,
errors are passed to a user.

Retries with reload are processed by wrappers that wraps "pure" crud operations
and utilities. Retries are counted per function, so in fact there could be more
reloads than ``N`` for a single request. For example, `crud.*_object*` code looks
like this:
```lua
local function reload_schema_wrapper(func, ...)
    for i = 1, N do
        local status, res, err, reload_needed = pcall(func, ...)
        if err ~= nil and reload_needed then
            process_reload()
            -- ...
        end
    end
end

function crud.insert_object(args)
    local flatten_obj, err = reload_schema_wrapper(flatten_obj_func, space_name, obj)
    -- ...
    return reload_schema_wrapper(call_insert_func, space_name, flatten_obj, opts)
end
```

For `crud.*_object_many`, each tuple flatten is retried separately (though it
is hard to imagine a real case of multiple successful flatten retries):
```lua
function crud.insert_object_many(args)
    for _, obj in ipairs(obj_array) do
        local flatten_obj, err = reload_schema_wrapper(flatten_obj_func, space_name, obj)
        -- ...
    end
    -- ...
    return reload_schema_wrapper(call_insert_many_func, space_name, flatten_obj_array, opts)
end
```

### When schema is reloaded depending on the user option

Related link: [PR#359](https://github.com/tarantool/crud/pull/359)

Conditionally if the flag `fetch_latest_metadata` for DML operation that
return metadata (or uses metadata directly) is used.
Before receiving the space format, a mismatch check will be performed between the scheme version
on all involved storage and the scheme version in the net_box connection of the router.
In case of mismatch, the schema reload will be triggered.

### Alternative approaches

One of the alternatives considered was to ping a storage instance on
each request to refresh schema (see [PR#111 comment](https://github.com/tarantool/crud/pull/111#discussion_r562757016)),
but it was declined due to performance degradation.


## Sharding schema

Related links: [#166](https://github.com/tarantool/crud/issues/166),
[PR#181](https://github.com/tarantool/crud/pull/181),
[#237](https://github.com/tarantool/crud/issues/237),
[PR#239](https://github.com/tarantool/crud/pull/239),
[#212](https://github.com/tarantool/crud/issues/212),
[PR#268](https://github.com/tarantool/crud/pull/268).

### How schema is stored

Again, ``crud`` cluster is a [``vshard``](https://www.tarantool.io/en/doc/latest/reference/reference_rock/vshard/)
cluster. Thus, data is sharded based on some key. To extract the key
from a tuple and compute a [``bucket_id``](https://www.tarantool.io/en/doc/latest/reference/reference_rock/vshard/vshard_architecture/),
we use [``ddl``](https://github.com/tarantool/ddl) module data.
``ddl`` module schema describes sharding key (how to extract data
required to compute ``bucket_id`` from a tuple based on space schema)
and sharding function (how to compute ``bucket_id`` based on the data).
This information is stored on storages in some tables and Tarantool
spaces: ``_ddl_sharding_key`` and ``_ddl_sharding_func``. ``crud``
module uses `_ddl_sharding_key` and `_ddl_sharding_func` spaces to fetch
sharding schema: thus, you don't obliged to use ``ddl`` module and can
setup only ``_ddl_*`` spaces manually, if you want. ``crud`` module uses
plain Lua tables to store sharding info on routers and storages.

Tarantool instance may have several vshard routers. For each request, only
one router is used (specified in `vshard_router` option or a default one).
Each router has its own ``net.box`` connections. Fetching and reload processes
are different for each router and do not affect each other since they
have separate caches.

### When schema is used

Sharding schema is used
- to compute ``bucket_id``. Thus, it is used each time we need to execute
  a non-map-reduce request[^1] and `bucket_id` is not specified by user.
  If there is no sharding schema specified, we use defaults: sharding key
  is primary key, sharding function is ``vshard.router.bucket_id_strcrc32``.

[^1]: It includes all ``insert_*``, ``replace_*``, ``update_*``,
      ``delete_*``, ``upsert_*``, ``get_*`` requests. ``*_many`` requests
      also use sharding schema to find a storage for each tuple. ``select``,
      ``count`` and ``pairs`` requests use sharding info if user conditions
      have equal condition for a sharding key. (User still can force map-reduce
      with `force_map_call`. In this case sharding schema won't be used.)

### How schema is reloaded

Storage sharding schema (internal ``crud`` Lua tables) is updated on
initialization (first time when info is requested) and each time someone
changes ``_ddl_sharding_key`` or ``_ddl_sharding_func`` data — we use
``on_replace`` triggers.

Routers fetch sharding schema if cache wasn't initialized yet or each
time reload was requested. Reload could be requested with ``crud``
itself (see below) or by user (with ``require('crud.common.sharding_key').update_cache()``
or ``require('crud.common.sharding_func').update_cache()`` handles).
The handles was deprecated after introducing automatic reload.

The sharding information reload procedure always fetches all sharding keys
and all sharding functions disregarding of a reason that triggers the reload.
If there are several requests to reload info, the only one
reload fiber is started and every request waits for its completion.

### When schema is reloaded and operation is retried

Retry with reload is triggered
- if router and storage schema hashes mismatch on request that
  uses sharding info.

Each request that uses sharding info passes sharding hashes from
a router with a request. If hashes mismatch with storage ones, we return
a specific error. The request retries if it receives a hash mismatch
error from the storage. If it didn't help after ``N`` retries (now ``N``
is ``1``), the error is passed to the user. For ``*_many`` methods,
reload and retry happens only if all tuples had failed with hash
mismatch; otherwise, errors are passed to the user.

Retries with reload are processed by wrappers that wraps "pure" crud
operations, same as in space schema reload. Since there is only one
use case for sharding info, there is only one wrapper per operation:
around the main "pure" crud function with network call.

### Alternative approaches

There were different implementations of working with hash: we tried
to compute it instead of storing pre-computed values (both on router
and storage), but pre-computed approach with triggers was better in
terms of performance. It was also an option to ping storage before
sending a request and verify sharding info relevance before sending
the request with separate call, but it was also declined due to
performance degradation.
