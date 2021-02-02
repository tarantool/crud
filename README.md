# CRUD

The `CRUD` module allows to perform CRUD operations on the cluster.
It also provides the `crud-storage` role for
[Tarantool Cartridge](https://github.com/tarantool/cartridge).

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
res, err = crud.select('customers')
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
* By default, `bucket_id` is computed as `vshard.router.bucket_id_strcrc32(key)`,
  where `key` is the primary key value.
  Custom bucket ID can be specified as `opts.bucket_id` for each operation.
  For operations that accepts tuple/object bucket ID can be specified as
  tuple/object field as well as `opts.bucket_id` value.

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
  * `timeout` (`?number`) - `vshard.call` timeout (in seconds)
  * `bucket_id` (`?number|cdata`) - bucket ID

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
* `operations` (`table`) - update [operations](https://www.tarantool.io/en/doc/latest/reference/reference_lua/box_space/#box-space-update)
* `opts`:
  * `timeout` (`?number`) - `vshard.call` timeout (in seconds)
  * `bucket_id` (`?number|cdata`) - bucket ID

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
* `operations` (`table`) - update [operations](https://www.tarantool.io/en/doc/latest/reference/reference_lua/box_space/#box-space-update) if there is an existing tuple which matches the key fields of tuple
* `opts`:
  * `timeout` (`?number`) - `vshard.call` timeout (in seconds)
  * `bucket_id` (`?number|cdata`) - bucket ID

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
     If negative value is specified, the last objects are returned
     (`after` option is required in this case).
  * `after` (`?table`) - tuple after which objects should be selected
  * `batch_size` (`?number`) - number of tuples to process per one request to storage
  * `timeout` (`?number`) - `vshard.call` timeout (in seconds)
  * `bucket_id` (`?number|cdata`) - bucket ID
    (is used when select by full primary key is performed)

Returns metadata and array of rows, error.

#### Select conditions

Select conditions are very similar to Tarantool update
[operations](https://www.tarantool.io/en/doc/latest/reference/reference_lua/box_space/#box-space-update).

Each condition is a table `{operator, field-identifier, value}`:

* Supported operators are: `=` (or `==`), `>`, `>=`, `<`, `<=`.
* Field identifier can be field name, field number, or index name.

**Example:**

```lua
crud.select('customers', {{'<=', 'age', 35}})
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
Its arguments are the same as [`crud.select`](#select) arguments,
but negative `first` values aren't allowed.
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
#crud.select('customers', {{'<=', 'age', 35}})
---
- 1
...
crud.truncate('customers', {timeout = 2})
---
- true
...
#crud.select('customers', {{'<=', 'age', 35}})
---
- 0
...
```

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
