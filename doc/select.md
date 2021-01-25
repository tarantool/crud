# Select examples

## Filtering

The second parameter passed to ``crud.select`` is an array of conditions. Below are examples of filtering data using these conditions. 

### Getting full space

To get a full space without filtering, you need to pass ``nil`` as a condition.

**Example:**

```lua
crud.select('customers', nil)
---
- metadata:
  - {'name': 'id', 'type': 'unsigned'}
  - {'name': 'bucked_id', 'type': 'unsigned'}
  - {'name': 'name', 'type': 'string'}
  - {'name': 'surname', 'type': 'string'}
  - {'name': 'age', 'type': 'number'}
  rows:
  - [1, 635, 'John', 'Adams', 65]
  - [2, 2364, 'John', 'Sidorov', 28]
  - [3, 6517, 'Ronald', 'Dump', 77]
  - [4, 563, 'Sergey', 'Lee', 21]
  - [5, 2313, 'Tatyana', 'May', 20]
...
```

### Select using index

Let's say we have a ``age`` index. Example below gets a list of ``customers`` over 30 years old.

**Example:**

```lua
res, err = crud.select('customers', {{'>=', 'age', 30}})
res.rows
---
- - [1, 635, 'John', 'Adams', 65]
  - [3, 6517, 'Ronald', 'Dump', 77]
...
```

### Select using composite index

Suppose we have a composite index consisting of the ``name`` and ``surname`` fields. See example of select queries using such a composited index below.

**Example**:

```lua
res, err = crud.select('customers', {{'==', 'full_name', {"John", "Sidorov"}}})
res.rows
---
- - [1, 2364, 'John', 'Sidorov', 28]
...
```

### Select using partial key 

Alternatively, you can use a partial key for a composite index.

**Example**:

```lua
res, err = crud.select('customers', {{'==', 'full_name', "John"}})
res.rows
---
- - [1, 2364, 'John', 'Sidorov', 28]
  - [2, 635, 'John', 'Adams', 65]
...
```

### Select using non-indexed field

You can also make a selection using a non-indexed field.

**Example:**

```lua
res, err = crud.select('customers', {{'>=', 'id', 3}})
res.rows
---
- - [3, 6517, 'Ronald', 'Dump', 77]
  - [4, 563, 'Sergey', 'Lee', 21]
  - [5, 2313, 'Tatyana', 'May', 20]
...
```

**Note:** in this case full scan is performed.

Note that the search condition for the indexed field must be placed first to avoi a full scan.

**Example:**

```lua
res, err = crud.select('customers', {{'>=', 'id', 3}, {'>=', 'age', 30}})
res.rows
---
- - [3, 6517, 'Ronald', 'Dump', 77]
...
```

In this case, a full scan will be performed, since non-indexed field is placed first in search conditions. Example below shows how you can avoid a full scan.

**Example:**

```lua
res, err = crud.select('customers', {{'>=', 'age', 30}, {'>=', 'id', 3}})
res.rows
---
- - [3, 6517, 'Ronald', 'Dump', 77]
...
```

## Pagination

The third (but optional) parameter in ``crud.select`` is array of [``options``](https://github.com/tarantool/crud#select). With this parameter, we can implement pagination. 

### ``First`` parameter

Using the ``first`` option we will get the first **N** results for the query.

**Example:**

```lua
crud.select('developers', nil, { first = 3 })
- metadata:
  - {'name': 'id', 'type': 'unsigned'}
  - {'name': 'bucked_id', 'type': 'unsigned'}
  - {'name': 'name', 'type': 'string'}
  - {'name': 'age', 'type': 'number'}
  rows:
  - [1, 7331, 'Alexey', 20]
  - [2, 899, 'Sergey', 21]
  - [3, 9661, 'Pavel', 27]
...
```

Thus, we got the first three objects from the ``developers`` space.

### ``After`` parameter

Using ``after``, we can get the objects after specified tuple.

**Example:**

```lua
res, err = crud.select('developers', nil, { after = res.rows[3] })
res.rows
---
- - [4, 501, 'Mikhail', 31]
  - [5, 1997, 'Dmitry', 16]
  - [6, 8765, 'Artyom', 51]
```

With this request, we got objects behind the objects from the [previous example](https://github.com/tarantool/crud/doc/select#first-parameter)

### Combine ``first`` and ``after`` 

To use pagination, we have to combine ``after`` and ``first`` parameters. 

**Example:**

```lua
res, err = crud.select('developers', nil,  { first = 3 })
res.rows
--- Got first three objects
- - [1, 7331, 'Alexey', 20]
  - [2, 899, 'Sergey', 21]
  - [3, 9661, 'Pavel', 27]
...
res, err = crud.select('developers', nil, { after = res.rows[3], first = 3 })
res.rows
--- Get the next three objects
- - [4, 501, 'Mikhail', 31]
  - [5, 1997, 'Dmitry', 16]
  - [6, 8765, 'Artyom', 51]
...
```

### Reverse pagination

Select also supports reverse pagination. To use it, pass a negative value to the ``first`` parameter.

**Example:**

```lua
res, err = crud.select('developers', nil, { after = res.rows[3], first = 3 })
res.rows
---
- - [4, 501, 'Mikhail', 31]
  - [5, 1997, 'Dmitry', 16]
  - [6, 8765, 'Artyom', 51]
...
res, err = crud.select('developers', nil, { after = res.rows[1], first = -3 })
res.rows
---
- - [1, 7331, 'Alexey', 20]
  - [2, 899, 'Sergey', 21]
  - [3, 9661, 'Pavel', 27]
...
```
