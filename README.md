# CRUD

[![Run static analysis](https://github.com/tarantool/crud/actions/workflows/check_on_push.yaml/badge.svg)](https://github.com/tarantool/crud/actions/workflows/check_on_push.yaml)
[![Run tests](https://github.com/tarantool/crud/actions/workflows/test_on_push.yaml/badge.svg)](https://github.com/tarantool/crud/actions/workflows/test_on_push.yaml)
[![Coverage Status](https://coveralls.io/repos/github/tarantool/crud/badge.svg?branch=master)](https://coveralls.io/github/tarantool/crud?branch=master)

The `CRUD` module allows to perform CRUD operations on the cluster.
It also provides the `crud-storage` and `crud-router` roles for
[Tarantool Cartridge](https://github.com/tarantool/cartridge).

## Table of Contents

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->

- [Quickstart](#quickstart)
- [API](#api)
  - [Insert](#insert)
  - [Get](#get)
  - [Update](#update)
  - [Delete](#delete)
  - [Replace](#replace)
  - [Upsert](#upsert)
  - [Select](#select)
    - [Select conditions](#select-conditions)
  - [Pairs](#pairs)
  - [Min and max](#min-and-max)
  - [Cut extra rows](#cut-extra-rows)
  - [Cut extra objects](#cut-extra-objects)
  - [Truncate](#truncate)
  - [Len](#len)
  - [Count](#count)
  - [Call options for crud methods](#call-options-for-crud-methods)
  - [Statistics](#statistics)
- [Cartridge roles](#cartridge-roles)
  - [Usage](#usage)
- [License](#license)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

## Quickstart

First, [install Tarantool](https://www.tarantool.io/en/download).

Now you have the following options how to learn crud API and use it in a
project:

* Play with crud on a testing dataset on a single instance:

  ```shell
  $ git clone https://github.com/tarantool/crud.git
  $ cd crud
  $ tarantoolctl rocks make
  $ ./doc/playground.lua
  tarantool> crud.select('customers', {{'<=', 'age', 35}}, {first = 10})
  tarantool> crud.select('developers', nil, {first = 6})
  ```
* Add crud into dependencies of a Cartridge application and add crud roles into
  dependencies of your roles (see [Cartridge roles](#cartridge-roles) section).
* Add crud into dependencies of your application (rockspec, RPM spec -- depends
  of your choice) and call crud initialization code from storage and router
  code (see [API](#api) section).

## API

The CRUD operations should be called from router.
All storage replica sets should call `crud.init_storage()`
(or enable the `crud-storage` role)
first to initialize storage-side functions that are used to manipulate data
across the cluster.
All routers should call `crud.init_router()` (or enable the `crud-router` role)
to make `crud` functions callable via `net.box`.

All operations return a table that contains rows (tuples) and metadata
(space format).
It can be used to convert received tuples to objects via `crud.unflatten_rows` function.

For example:

```lua
res, err = crud.select('customers', nil, {first = 2})
res
---
- metadata:
  - {'name': 'id', 'type': 'unsigned'}
  - {'name': 'bucket_id', 'type': 'unsigned'}
  - {'name': 'name', 'type': 'string'}
  - {'name': 'age', 'type': 'number'}
  rows:
  - [1, 12477, 'Elizabeth', 12]
  - [2, 21401, 'David', 33]
...
crud.unflatten_rows(res.rows, res.metadata)
---
- - bucket_id: 12477
    age: 12
    name: Elizabeth
    id: 1
  - bucket_id: 21401
    age: 33
    name: David
    id: 2
...
```

**Notes:**

* A space should have a format.

**Sharding key and bucket id calculation**

*Sharding key* is a set of tuple field values used for calculation *bucket ID*.
*Sharding key definition* is a set of tuple field names that describe what
tuple field should be a part of sharding key. *Bucket ID* determines which
replicaset stores certain data. Function that used for bucket ID calculation is
named *sharding function*.

By default CRUD calculates bucket ID using primary key and a function
`vshard.router.bucket_id_strcrc32(key)`, it happen automatically and doesn't
require any actions from user side. However, for operations that accepts
tuple/object bucket ID can be specified as tuple/object field as well as
`opts.bucket_id` value.

Starting from 0.10.0 users who don't want to use primary key as a sharding key
may set custom sharding key definition as a part of [DDL
schema](https://github.com/tarantool/ddl#input-data-format) or insert manually
to the space `_ddl_sharding_key` (for both cases consider a DDL module
documentation). As soon as sharding key for a certain space is available in
`_ddl_sharding_key` space CRUD will use it for bucket ID calculation
automatically. Note that CRUD methods `delete()`, `get()` and `update()`
requires that sharding key must be a part of primary key.

Starting from 0.11.0 you can specify sharding function to calculate bucket_id
with sharding func definition as a part of
[DDL schema](https://github.com/tarantool/ddl#input-data-format)
or insert manually to the space `_ddl_sharding_func`.

Automatic sharding key and function reload is supported since version 0.11.0.
Version 0.11.0 contains critical bug that causes some CRUD methods to fail
with "Sharding hash mismatch" error if ddl is set and bucket_id is provided
explicitly ([#278](https://github.com/tarantool/crud/issues/278)). Please,
upgrade to 0.11.1 instead.

CRUD uses `strcrc32` as sharding function by default.
The reason why using of `strcrc32` is undesirable is that
this sharding function is not consistent for cdata numbers.
In particular, it returns 3 different values for normal Lua
numbers like 123, for `unsigned long long` cdata
(like `123ULL`, or `ffi.cast('unsigned long long',
123)`), and for `signed long long` cdata (like `123LL`, or
`ffi.cast('long long', 123)`).

We cannot change default sharding function `strcrc32`
due to backward compatibility concerns, but please consider
using better alternatives for sharding function.
`mpcrc32` is one of them.

Table below describe what operations supports custom sharding key:

| CRUD method                      | Sharding key support       |
| -------------------------------- | -------------------------- |
| `get()`                          | Yes                        |
| `insert()` / `insert_object()`   | Yes                        |
| `delete()`                       | Yes                        |
| `replace()` / `replace_object()` | Yes                        |
| `upsert()` / `upsert_object()`   | Yes                        |
| `select()` / `pairs()`           | Yes                        |
| `count()`                        | Yes                        |
| `update()`                       | Yes                        |
| `min()` / `max()`                | No (not required)          |
| `cut_rows()` / `cut_objects()`   | No (not required)          |
| `truncate()`                     | No (not required)          |
| `len()`                          | No (not required)          |

Current limitations for using custom sharding key:

- No support of JSON path for sharding key, see
  [#219](https://github.com/tarantool/crud/issues/219).
- `primary_index_fieldno_map` is not cached, see
  [#243](https://github.com/tarantool/crud/issues/243).

### Insert

```lua
-- Insert tuple
local result, err = crud.insert(space_name, tuple, opts)
-- Insert object
local result, err = crud.insert_object(space_name, object, opts)
```

where:

* `space_name` (`string`) - name of the space to insert an object
* `tuple` / `object` (`table`) - tuple/object to insert
* `opts`:
  * `timeout` (`?number`) - `vshard.call` timeout (in seconds)
  * `bucket_id` (`?number|cdata`) - bucket ID
  * `fields` (`?table`) - field names for getting only a subset of fields

Returns metadata and array contains one inserted row, error.

**Example:**

```lua
crud.insert('customers', {1, box.NULL, 'Elizabeth', 23})
---
- metadata:
  - {'name': 'id', 'type': 'unsigned'}
  - {'name': 'bucket_id', 'type': 'unsigned'}
  - {'name': 'name', 'type': 'string'}
  - {'name': 'age', 'type': 'number'}
  rows:
  - [1, 477, 'Elizabeth', 23]
...
crud.insert_object('customers', {
    id = 2, name = 'Elizabeth', age = 24,
})
---
- metadata:
  - {'name': 'id', 'type': 'unsigned'}
  - {'name': 'bucket_id', 'type': 'unsigned'}
  - {'name': 'name', 'type': 'string'}
  - {'name': 'age', 'type': 'number'}
  rows:
  - [2, 401, 'Elizabeth', 24]
...
```

### Get

```lua
local object, err = crud.get(space_name, key, opts)
```

where:

* `space_name` (`string`) - name of the space
* `key` (`any`) - primary key value
* `opts`:
  * `fields` (`?table`) - field names for getting only a subset of fields
  * `bucket_id` (`?number|cdata`) - bucket ID
  * `timeout` (`?number`) - `vshard.call` timeout (in seconds)
  * `mode` (`?string`, `read` or `write`) - if `write` is specified then `get` is
    performed on master
  * `prefer_replica` (`?boolean`) - if `true` then the preferred target is one of
    the replicas
  * `balance` (`?boolean`) - use replica according to vshard load balancing policy

Returns metadata and array contains one row, error.

**Example:**

```lua
crud.get('customers', 1)
---
- metadata:
  - {'name': 'id', 'type': 'unsigned'}
  - {'name': 'bucket_id', 'type': 'unsigned'}
  - {'name': 'name', 'type': 'string'}
  - {'name': 'age', 'type': 'number'}
  rows:
  - [1, 477, 'Elizabeth', 23]
...
```

### Update

```lua
local object, err = crud.update(space_name, key, operations, opts)
```

where:

* `space_name` (`string`) - name of the space
* `key` (`any`) - primary key value
* `operations` (`table`) - update [operations](https://www.tarantool.io/en/doc/latest/reference/reference_lua/box_space/update/)
* `opts`:
  * `timeout` (`?number`) - `vshard.call` timeout (in seconds)
  * `bucket_id` (`?number|cdata`) - bucket ID
  * `fields` (`?table`) - field names for getting only a subset of fields

Returns metadata and array contains one updated row, error.

**Example:**

```lua
crud.update('customers', 1, {{'+', 'age', 1}})
---
- metadata:
  - {'name': 'id', 'type': 'unsigned'}
  - {'name': 'bucket_id', 'type': 'unsigned'}
  - {'name': 'name', 'type': 'string'}
  - {'name': 'age', 'type': 'number'}
  rows:
  - [1, 477, 'Elizabeth', 24]
...
```

### Delete

```lua
local object, err = crud.delete(space_name, key, opts)
```

where:

* `space_name` (`string`) - name of the space
* `key` (`any`) - primary key value
* `opts`:
  * `timeout` (`?number`) - `vshard.call` timeout (in seconds)
  * `bucket_id` (`?number|cdata`) - bucket ID
  * `fields` (`?table`) - field names for getting only a subset of fields

Returns metadata and array contains one deleted row (empty for vinyl), error.

**Example:**

```lua
crud.delete('customers', 1)
---
- metadata:
  - {'name': 'id', 'type': 'unsigned'}
  - {'name': 'bucket_id', 'type': 'unsigned'}
  - {'name': 'name', 'type': 'string'}
  - {'name': 'age', 'type': 'number'}
  rows:
  - [1, 477, 'Elizabeth', 24]
```

### Replace

```lua
-- Replace tuple
local result, err = crud.replace(space_name, tuple, opts)
-- Replace object
local result, err = crud.replace_object(space_name, object, opts)
```

where:

* `space_name` (`string`) - name of the space
* `tuple` / `object` (`table`) - tuple/object to insert or replace exist one
* `opts`:
  * `timeout` (`?number`) - `vshard.call` timeout (in seconds)
  * `bucket_id` (`?number|cdata`) - bucket ID
  * `fields` (`?table`) - field names for getting only a subset of fields

Returns inserted or replaced rows and metadata or nil with error.

**Example:**

```lua
crud.replace('customers', {1, box.NULL, 'Alice', 22})
---
- metadata:
  - {'name': 'id', 'type': 'unsigned'}
  - {'name': 'bucket_id', 'type': 'unsigned'}
  - {'name': 'name', 'type': 'string'}
  - {'name': 'age', 'type': 'number'}
  rows:
  - [1, 477, 'Alice', 22]
...
crud.replace_object('customers', {
    id = 1, name = 'Alice', age = 22,
})
---
- metadata:
  - {'name': 'id', 'type': 'unsigned'}
  - {'name': 'bucket_id', 'type': 'unsigned'}
  - {'name': 'name', 'type': 'string'}
  - {'name': 'age', 'type': 'number'}
  rows:
  - [1, 477, 'Alice', 22]
...
```

### Upsert

```lua
-- Upsert tuple
local result, err = crud.upsert(space_name, tuple, operations, opts)
-- Upsert object
local result, err = crud.upsert_object(space_name, tuple, operations, opts)
```

where:

* `space_name` (`string`) - name of the space
* `tuple` / `object` (`table`) - tuple/object to insert if there is no existing tuple which matches the key fields
* `operations` (`table`) - update [operations](https://www.tarantool.io/en/doc/latest/reference/reference_lua/box_space/update/) if there is an existing tuple which matches the key fields of tuple
* `opts`:
  * `timeout` (`?number`) - `vshard.call` timeout (in seconds)
  * `bucket_id` (`?number|cdata`) - bucket ID
  * `fields` (`?table`) - field names for getting only a subset of fields

Returns metadata and empty array of rows or nil, error.

**Example:**

```lua
crud.upsert('customers',
    {1, box.NULL, 'Alice', 22},
    {{'+', 'age', 1}})
---
- metadata:
  - {'name': 'id', 'type': 'unsigned'}
  - {'name': 'bucket_id', 'type': 'unsigned'}
  - {'name': 'name', 'type': 'string'}
  - {'name': 'age', 'type': 'number'}
  rows: []
...
crud.upsert_object('customers',
    {id = 1, name = 'Alice', age = 22},
    {{'+', 'age', 1}})
---
- metadata:
  - {'name': 'id', 'type': 'unsigned'}
  - {'name': 'bucket_id', 'type': 'unsigned'}
  - {'name': 'name', 'type': 'string'}
  - {'name': 'age', 'type': 'number'}
  rows: []
...
```


### Select

`CRUD` supports multi-conditional selects, treating a cluster as a single space.
The conditions may include field names or numbers, as well as index names.
The recommended first condition is a TREE index; this helps reducing the number
of tuples to scan. Otherwise a full scan is performed.

```lua
local objects, err = crud.select(space_name, conditions, opts)
```

where:

* `space_name` (`string`) - name of the space
* `conditions` (`?table`) - array of [select conditions](#select-conditions)
* `opts`:
  * `first` (`?number`) - the maximum count of the objects to return.
     If negative value is specified, the objects behind `after` are returned
     (`after` option is required in this case). [See pagination examples](doc/select.md#pagination).
  * `after` (`?table`) - tuple after which objects should be selected
  * `batch_size` (`?number`) - number of tuples to process per one request to storage
  * `bucket_id` (`?number|cdata`) - bucket ID
  * `force_map_call` (`?boolean`) - if `true`
     then the map call is performed without any optimizations even
     if full primary key equal condition is specified
  * `timeout` (`?number`) - `vshard.call` timeout (in seconds)
  * `fields` (`?table`) - field names for getting only a subset of fields
  * `fullscan` (`?boolean`) - if `true` then a critical log entry will be skipped
    on potentially long `select`, see [avoiding full scan](doc/select.md#avoiding-full-scan).
  * `mode` (`?string`, `read` or `write`) - if `write` is specified then `select` is
    performed on master
  * `prefer_replica` (`?boolean`) - if `true` then the preferred target is one of
    the replicas
  * `balance` (`?boolean`) - use replica according to vshard load balancing policy


Returns metadata and array of rows, error.

#### Select conditions

Select conditions are very similar to Tarantool update
[operations](https://www.tarantool.io/en/doc/latest/reference/reference_lua/box_space/update/).

Each condition is a table `{operator, field-identifier, value}`:

* Supported operators are: `=` (or `==`), `>`, `>=`, `<`, `<=`.
* Field identifier can be field name, field number, or index name.

**Example:**

```lua
crud.select('customers', {{'<=', 'age', 35}}, {first = 10})
---
- metadata:
  - {'name': 'id', 'type': 'unsigned'}
  - {'name': 'bucket_id', 'type': 'unsigned'}
  - {'name': 'name', 'type': 'string'}
  - {'name': 'age', 'type': 'number'}
  rows:
  - [5, 1172, 'Jack', 35]
  - [3, 2804, 'David', 33]
  - [6, 1064, 'William', 25]
  - [7, 693, 'Elizabeth', 18]
  - [1, 477, 'Elizabeth', 12]
...
```

**Note**: tuples are sorted by age because space has index `age`.
Otherwise, tuples are sorted by primary key.

See more examples of select queries [here.](https://github.com/tarantool/crud/blob/master/doc/select.md)

### Pairs

You can iterate across a distributed space using the `crud.pairs` function.
Its arguments are the same as [`crud.select`](#select) arguments except
`fullscan` (it does not exist because `crud.pairs` does not generate a critical
log entry on potentially long requests) and negative `first` values aren't
allowed.
User could pass use_tomap flag (false by default) to iterate over flat tuples or objects.

**Example:**

```lua
local tuples = {}
for _, tuple in crud.pairs('customers', {{'<=', 'age', 35}}, {use_tomap = false}) do
    -- {5, 1172, 'Jack', 35}
    table.insert(tuples, tuple)
end

local objects = {}
for _, object in crud.pairs('customers', {{'<=', 'age', 35}}, {use_tomap = true}) do
    -- {id = 5, name = 'Jack', bucket_id = 1172, age = 35}
    table.insert(objects, object)
end
```

See more examples of pairs queries [here.](https://github.com/tarantool/crud/blob/master/doc/pairs.md)

### Min and max

```lua
-- Find the minimum value in the specified index
local result, err = crud.min(space_name, 'age', opts)
---
- metadata:
  - {'name': 'id', 'type': 'unsigned'}
  - {'name': 'bucket_id', 'type': 'unsigned'}
  - {'name': 'name', 'type': 'string'}
  - {'name': 'age', 'type': 'number'}
  rows:
  - [1, 477, 'Elizabeth', 12]

-- Find the maximum value in the specified index
local result, err = crud.max(space_name, 'age', opts)
---
- metadata:
  - {'name': 'id', 'type': 'unsigned'}
  - {'name': 'bucket_id', 'type': 'unsigned'}
  - {'name': 'name', 'type': 'string'}
  - {'name': 'age', 'type': 'number'}
  rows:
  - [5, 1172, 'Jack', 35]
```

### Cut extra rows

You could use `crud.cut_rows` function to cut off scan key and primary key values that were merged to the select/pairs partial result (select/pairs with `fields` option).

```lua
local res, err = crud.cut_rows(rows, metadata, fields)
```

where:

* `rows` (`table`) - array of tuples for cutting
* `matadata` (`?metadata`) - metadata about `rows` fields
* `fields` (`table`) - field names of fields that should be contained in the result

Returns metadata and array of rows, error.

See more examples of `crud.cut_rows` usage [here](https://github.com/tarantool/crud/blob/master/doc/select.md) and [here.](https://github.com/tarantool/crud/blob/master/doc/pairs.md)

### Cut extra objects

If you use `pairs` with `use_tomap` flag and you need to cut off scan key and primary key values that were merged to the pairs partial result (pairs with `fields` option) you should use `crud.cut_objects`.

```lua
local new_objects = crud.cut_objects(objects, fields)
```

where:

* `objects` (`table`) - array of objects for cutting
* `fields` (`table`) - field names of fields that should be contained in the result

Returns array of objects.

See more examples of `crud.cut_objects` usage [here.](https://github.com/tarantool/crud/blob/master/doc/pairs.md)

### Truncate

```lua
-- Truncate space
local result, err = crud.truncate(space_name, opts)
```

where:

* `space_name` (`string`) - name of the space
* `opts`:
  * `timeout` (`?number`) - `vshard.call` timeout (in seconds)

Returns true or nil with error.

**Example:**

```lua
#crud.select('customers', {{'<=', 'age', 35}}, {first = 10})
---
- 1
...
crud.truncate('customers', {timeout = 2})
---
- true
...
#crud.select('customers', {{'<=', 'age', 35}}, {first = 10})
---
- 0
...
```

### Len

```lua
-- Calculates the number of tuples in the space for memtx engine
-- Calculates the maximum approximate number of tuples in the space for vinyl engine
local result, err = crud.len(space_name, opts)
```

where:

* `space_name` (`string|number`) - name of the space as well
  as numerical id of the space
* `opts`:
  * `timeout` (`?number`) - `vshard.call` timeout (in seconds)

Returns number or nil with error.

**Example:**

Using `memtx`:

```lua
#crud.select('customers', nil, {fullscan = true})
---
- 5
...
crud.len('customers', {timeout = 2})
---
- 5
...
```

Using `vinyl`:

```lua
crud.len('customers')
---
- 0
...
crud.delete('customers', 1)
---
...
crud.len('customers')
---
- 1
...
```

### Count

`CRUD` supports multi-conditional count, treating a cluster as a single space.
The same as with `select()` the conditions may include field names or numbers,
as well as index names. The recommended first condition is a TREE index; this
helps to reduce the number of tuples to scan. Otherwise a full scan is performed.
If compared with `len()`, `count()` method scans the entire space to count the
tuples according user conditions. This method does yield that's why result may
be approximate. Number of tuples before next `yield()` is under control with
option `yield_every`.

```lua
local result, err = crud.count(space_name, conditions, opts)
```

where:

* `space_name` (`string`) - name of the space
* `conditions` (`?table`) - array of [conditions](#select-conditions)
* `opts`:
  * `yield_every` (`?number`) - number of tuples processed to yield after,
    `yield_every` should be > 0, default value is 100
  * `timeout` (`?number`) - `vshard.call` timeout (in seconds), default value is 2
  * `bucket_id` (`?number|cdata`) - bucket ID
  * `force_map_call` (`?boolean`) - if `true`
    then the map call is performed without any optimizations even,
    default value is `false`
  * `mode` (`?string`, `read` or `write`) - if `write` is specified then `count` is
    performed on master, default value is `read`
  * `prefer_replica` (`?boolean`) - if `true` then the preferred target is one of
    the replicas, default value is `false`
  * `balance` (`?boolean`) - use replica according to
    [vshard load balancing policy](https://www.tarantool.io/en/doc/latest/reference/reference_rock/vshard/vshard_api/#router-api-call),
    default value is `false`

```lua
crud.count('customers', {{'<=', 'age', 35}})
---
- 5
...
```

### Call options for crud methods

Combinations of `mode`, `prefer_replica` and `balance` options lead to:

* `mode` == `write` - method performed on master with vshard call `callrw`
* `mode` == `read`
  * not prefer_replica, not balance -
    [vshard call `callro`](https://www.tarantool.io/en/doc/latest/reference/reference_rock/vshard/vshard_api/#router-api-callro)
  * not prefer_replica, balance -
    [vshard call `callbro`](https://www.tarantool.io/en/doc/latest/reference/reference_rock/vshard/vshard_api/#router-api-callbro)
  * prefer_replica, not balance -
    [vshard call `callre`](https://www.tarantool.io/en/doc/latest/reference/reference_rock/vshard/vshard_api/#router-api-callre)
  * prefer_replica, balance -
    [vshard call `callbre`](https://www.tarantool.io/en/doc/latest/reference/reference_rock/vshard/vshard_api/#router-api-callbre)

### Statistics

`crud` routers can provide statistics on called operations.
```lua
-- Enable statistics collect.
crud.cfg{ stats = true }

-- Returns table with statistics information.
crud.stats()

-- Returns table with statistics information for specific space.
crud.stats('my_space')

-- Disables statistics collect and destroys all collectors.
crud.cfg{ stats = false }

-- Destroys all statistics collectors and creates them again.
crud.reset_stats()
```

If [`metrics`](https://github.com/tarantool/metrics) `0.10.0` or greater
found, metrics collectors will be used by default to store statistics
instead of local collectors. Quantiles in metrics summary collections
are disabled by default. You can manually choose driver and enable quantiles.
```lua
-- Use simple local collectors (default if no required metrics version found).
crud.cfg{ stats = true, stats_driver = 'local' }

-- Use metrics collectors (default if metrics rock found).
crud.cfg{ stats = true, stats_driver = 'metrics' }

-- Use metrics collectors with 0.99 quantiles.
crud.cfg{ stats = true, stats_driver = 'metrics', stats_quantiles = true }
```

You can use `crud.cfg` to check current stats state.
```lua
crud.cfg
---
- stats_quantiles: true
  stats: true
  stats_driver: metrics
...
```
Performance overhead is 3-10% in case of `local` driver and
5-15% in case of `metrics` driver, up to 20% for `metrics` with quantiles.

Beware that iterating through `crud.cfg` with pairs is not supported yet,
refer to [tarantool/crud#265](https://github.com/tarantool/crud/issues/265).

Format is as follows.
```lua
crud.stats()
---
- spaces:
    my_space:
      insert:
        ok:
          latency: 0.0015
          latency_average: 0.002
          latency_quantile_recent: 0.0015
          count: 19800
          time: 39.6
        error:
          latency: 0.0000008
          latency_average: 0.000001
          latency_quantile_recent: 0.0000008
          count: 4
          time: 0.000004
...
crud.stats('my_space')
---
- insert:
    ok:
      latency: 0.0015
      latency_average: 0.002
      latency_quantile_recent: 0.0015
      count: 19800
      time: 39.6
    error:
      latency: 0.0000008
      latency_average: 0.000001
      latency_quantile_recent: 0.0000008
      count: 4
      time: 0.000004
...
```
`spaces` section contains statistics for each observed space.
If operation has never been called for a space, the corresponding
field will be empty. If no requests has been called for a
space, it will not be represented. Space data is based on
client requests rather than storages schema, so requests
for non-existing spaces are also collected.

Possible statistics operation labels are
`insert` (for `insert` and `insert_object` calls),
`get`, `replace` (for `replace` and `replace_object` calls), `update`,
`upsert` (for `upsert` and `upsert_object` calls), `delete`,
`select` (for `select` and `pairs` calls), `truncate`, `len`, `count`
and `borders` (for `min` and `max` calls).

Each operation section consists of different collectors
for success calls and error (both error throw and `nil, err`)
returns. `count` is the total requests count since instance start
or stats restart.  `time` is the total time of requests execution.
`latency_average` is `time` / `count`.
`latency_quantile_recent` is the 0.99 quantile of request execution
time for a recent period (see 
[`metrics` summary API](https://www.tarantool.io/ru/doc/latest/book/monitoring/api_reference/#summary)).
It is computed only if `metrics` driver is used and quantiles are
enabled. `latency_quantile_recent` value may be `-nan` if there
wasn't any observations for several ages, see
[tarantool/metrics#303](https://github.com/tarantool/metrics/issues/303).
`latency` is a `latency_quantile_recent` if `metrics` driver is used
and quantiles are enabled, otherwise it's `latency_average`.

In [`metrics`](https://www.tarantool.io/en/doc/latest/book/monitoring/)
registry statistics are stored as `tnt_crud_stats` metrics
with `operation`, `status` and `name` labels.
```
metrics:collect()
---
- - label_pairs:
      status: ok
      operation: insert
      name: customers
    value: 221411
    metric_name: tnt_crud_stats_count
  - label_pairs:
      status: ok
      operation: insert
      name: customers
    value: 10.49834896344692
    metric_name: tnt_crud_stats_sum
  - label_pairs:
      status: ok
      operation: insert
      name: customers
      quantile: 0.99
    value: 0.00023606420935973
    metric_name: tnt_crud_stats
...
```
If you see `-Inf` value in quantile metrics, try to decrease the tolerated error:
```lua
crud.cfg{stats_quantile_tolerated_error = 1e-4}
```
See [tarantool/metrics#189](https://github.com/tarantool/metrics/issues/189) for
details about the issue.
You can also configure quantile `age_bucket_count` (default: 2) and
`max_age_time` (in seconds, default: 60):
```lua
crud.cfg{
    stats_quantile_age_bucket_count = 3,
    stats_quantile_max_age_time = 30,
}
```
See [`metrics` summary API](https://www.tarantool.io/ru/doc/latest/book/monitoring/api_reference/#summary)
for details. These parameters can be used to smooth time window move
or reduce the amount on `-nan` gaps for low request frequency applications.

`select` section additionally contains `details` collectors.
```lua
crud.stats('my_space').select.details
---
- map_reduces: 4
  tuples_fetched: 10500
  tuples_lookup: 238000
...
```
`map_reduces` is the count of planned map reduces (including those not
executed successfully). `tuples_fetched` is the count of tuples fetched
from storages during execution, `tuples_lookup` is the count of tuples
looked up on storages while collecting responses for calls (including
scrolls for multibatch requests). Details data is updated as part of
the request process, so you may get new details before `select`/`pairs`
call is finished and observed with count, latency and time collectors.
In [`metrics`](https://www.tarantool.io/en/doc/latest/book/monitoring/)
registry they are stored as `tnt_crud_map_reduces`,
`tnt_crud_tuples_fetched` and `tnt_crud_tuples_lookup` metrics
with `{ operation = 'select', name = space_name }` labels.

Since `pairs` request behavior differs from any other crud request, its
statistics collection also has specific behavior. Statistics (`select`
section) are updated after `pairs` cycle is finished: you
either have iterated through all records or an error was thrown.
If your pairs cycle was interrupted with `break`, statistics will
be collected when pairs objects are cleaned up with Lua garbage
collector.

Statistics are preserved between package reloads. Statistics are preserved
between [Tarantool Cartridge role reloads](https://www.tarantool.io/en/doc/latest/book/cartridge/cartridge_api/modules/cartridge.roles/#reload)
if you use CRUD Cartridge roles. Beware that metrics 0.12.0 and below do not
support preserving stats between role reload
(see [tarantool/metrics#334](https://github.com/tarantool/metrics/issues/334)),
thus this feature will be unsupported for `metrics` driver.

## Cartridge roles

`cartridge.roles.crud-storage` is a Tarantool Cartridge role that depends on the
`vshard-storage` role, but also initializes functions that
are used on the storage side to perform CRUD operations.

`cartridge.roles.crud-router` is a role that depends on the
`vshard-router` role, but also exposes public `crud` functions in the global
scope, so that you can call them via `net.box`.


### Usage

1. Add `crud` to dependencies in the project rockspec.

**Note**: it's better to use tagged version than `scm-1`.
Check the latest available [release](https://github.com/tarantool/crud/releases) tag and use it.

```lua
-- <project-name>-scm-1.rockspec
dependencies = {
    ...
    'crud == <the-latest-tag>-1',
    ...
}
```

2. Create the role that stores your data and depends on `crud-storage`.

```lua
-- app.roles.customers-storage.lua
local cartridge = require('cartridge')

return {
        role_name = 'customers-storage',
        init = function()
            local customers_space = box.schema.space.create('customers', {
                format = {
                    {name = 'id', type = 'unsigned'},
                    {name = 'bucket_id', type = 'unsigned'},
                    {name = 'name', type = 'string'},
                    {name = 'age', type = 'number'},
                },
                if_not_exists = true,
            })
            customers_space:create_index('id', {
                parts = { {field ='id', is_nullable = false} },
                if_not_exists = true,
            })
            customers_space:create_index('bucket_id', {
                parts = { {field ='bucket_id', is_nullable = false} },
                if_not_exists = true,
            })
            customers_space:create_index('age', {
                parts = { {field ='age'} },
                unique = false,
                if_not_exists = true,
            })
        end,
        dependencies = {'cartridge.roles.crud-storage'},
    }
```

```lua
-- app.roles.customers-router.lua
local cartridge = require('cartridge')
return {
        role_name = 'customers-router',
        dependencies = {'cartridge.roles.crud-router'},
    }
```

3. Start the application and create `customers-storage` and
   `customers-router` replica sets.

4. Don't forget to bootstrap vshard.

Now your cluster contains storages that are configured to be used for
CRUD-operations.
You can simply call CRUD functions on the router to insert, select, and update
data across the cluster.

## License

BSD-2-Clause. See the [LICENSE](LICENSE) file.
