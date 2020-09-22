# CRUD

CRUD module allows to perform CRUD operations on the cluster.
It also provides `crud-storage` role for [Tarantool Cartridge](https://github.com/tarantool/cartridge).

## API

The CRUD operations should be called from storage.
All storage replicasets should call `crud.init()` (or enable `crud-storage` role) 
first to initialize storage-side functions that are used to manipulate data across the cluster.

**Note**, that space should have format.

**Note**, that all non-TREE indexes will be ignored.

**Note**, that `bucket_id` is computed as
`vshard.router.bucket_id_mpcrc32(key)`, where `key` is primary key value

### Insert

```lua
local object, err = crud.insert(space_name, object, opts)
```

* `space_name` (`string`) - name of the space to insert object
* `object` (`table`) - an object to insert
* `opts`:
  * `timeout` (`?number`) - `vshard.call` timeout (in seconds)

Returns inserted object, error.

**Example**

```lua
crud.insert('customers', {
    id = 1, name = 'Elizabeth', age = 23,
})
---
- bucket_id: 7614
  age: 23
  name: Elizabeth
  id: 1
...
```

### Get

```lua
local object, err = crud.get(space_name, key, opts)
```

* `space_name` (`string`) - name of the space
* `key` (`any`) - primary key value
* `opts`:
  * `timeout` (`?number`) - `vshard.call` timeout (in seconds)

Returns object, error.

**Example**

```lua
crud.get('customers', 1)
---
- bucket_id: 7614
  age: 23
  name: Elizabeth
  id: 1
...
```

### Update

```lua
local object, err = crud.update(space_name, key, operations, opts)
```

* `space_name` (`string`) - name of the space
* `key` (`any`) - primary key value
* `operations` (`table`) - update [operations](https://www.tarantool.io/en/doc/latest/reference/reference_lua/box_space/#box-space-update)
* `opts`:
  * `timeout` (`?number`) - `vshard.call` timeout (in seconds)

Returns updated object, error.

**Example**

```lua
crud.update('customers', 1, {{'+', 'age', 1}})
---
- bucket_id: 7614
  age: 24
  name: Elizabeth
  id: 1
...
```

### Delete

```lua
local object, err = crud.delete(space_name, key, opts)
```

* `space_name` (`string`) - name of the space
* `key` (`any`) - primary key value
* `opts`:
  * `timeout` (`?number`) - `vshard.call` timeout (in seconds)

Returns deleted object, error.

**Example**

```lua
crud.delete('customers', 1)
---
- bucket_id: 7614
  age: 24
  name: Elizabeth
  id: 1
...
```

### Select

```lua
local objects, err = crud.select(space_name, conditions, opts)
```

* `space_name` (`string`) - name of the space
* `conditions` (`?table`) - an array of [select conditions](#select-conditions)
* `opts`:
  * `limit` (`?number`) - the maximum limit of the result objects
  * `after` (`?table`) - an object after which object should be selected
  * `batch_size` (`?number`) - a number of tuples to process per one request to storage
  * `timeout` (`?number`) - `vshard.call` timeout (in seconds)

Returns selected objects, error.

#### Select conditions

Select conditions are very similar to Tarantool update
[operations](https://www.tarantool.io/en/doc/latest/reference/reference_lua/box_space/#box-space-update).

Each condition is a table `{operator, field-identifier, value}`:

* supported operators are: `=` (or `==`), `>`, `>=`, `<`, `<=`.
* field identifier can be field name, field number or index name.

**Example**

```lua
crud.select('customers', {{'<=', 'age', 35}})
---
- - bucket_id: 10755
    age: 35
    name: Jack
    id: 5
  - bucket_id: 8011
    age: 33
    name: David
    id: 3
  - bucket_id: 16055
    age: 25
    name: William
    id: 6
  - bucket_id: 2998
    age: 18
    name: Elizabeth
    id: 7
  - bucket_id: 7614
    age: 12
    name: Elizabeth
    id: 1
```

### Pairs

You can iterate across the distributed space using `crud.pairs` function.
It's arguments are the same as [`crud.select`](#select) arguments.

**Example**

```lua
for _, obj in crud.pairs('customers', {{'<=', 'age', 35}}) do 
    -- do smth with object 
end
```

## Cartridge role

`cartridge.roles.crud-storage` is a Tarantool Cartridge role that depends on
`vshard-storage` role, but also initializes functions that
are used on storage-side to perform CRUD operations.

### Usage

1. Add the `crud` to dependencies in the project rockspec.

```lua
-- <project-name>-scm-1.rockspec
dependencies = {
    ...
    'crud >= 0.1.0-1',
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

3. Start the application and create `customers-storage` and
`vshard-router` replicasets.

4. Don't forget to bootstrap vshard.

Now your cluster contains storages that are configured to be used for
CRUD-operations.
You can simply call CRUD functions on the router to insert, select and update data across the cluster.
