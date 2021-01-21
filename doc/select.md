# Select examples

## Pagination

Select supports pagination. To use it, use the [``after``](https://github.com/tarantool/crud#select) and [``first``](https://github.com/tarantool/crud#select) parameter.

**Example**:

```lua
res, err = crud.select('developers', nil,  { first = 3 })
res.rows
---
- - [1, 7331, 'Alexey', 20]
  - [2, 899, 'Sergey', 21]
  - [3, 9661, 'Pavel', 27]
...
res, err = crud.select('developers', nil, { after = res.rows[3], first = 3 })
res.rows
---
- - [4, 501, 'Mikhail', 31]
  - [5, 1997, 'Dmitry', 16]
  - [6, 8765, 'Artyom', 51]
...
```

Select also supports reverse pagination.

**Example**:

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

## Select using composite index

Suppose we have a composite index consisting of the ``name`` and ``surname`` fields. See example of select queries using such a composited index below.

**Example**:

```lua
crud.select('customers', {{'==', 'full_name', {"John", "Sidorov"}}})
---
- metadata:
  - {'name': 'id', 'type': 'unsigned'}
  - {'name': 'bucked_id', 'type': 'unsigned'}
  - {'name': 'name', 'type': 'string'}
  - {'name': 'surname', 'type': 'string'}
  - {'name': 'age', 'type': 'number'}
  rows:
  - [37, 2364, 'John', 'Sidorov', 28]
...
```

Alternatively, you can use a partial key for a composite index.

**Example**:

```lua
crud.select('customers', {{'==', 'full_name', "John"}})
---
- metadata:
  - {'name': 'id', 'type': 'unsigned'}
  - {'name': 'bucked_id', 'type': 'unsigned'}
  - {'name': 'name', 'type': 'string'}
  - {'name': 'surname', 'type': 'string'}
  - {'name': 'age', 'type': 'number'}
  rows:
  - [37, 2364, 'John', 'Sidorov', 28]
  - [99, 635, 'John', 'Adams', 65]
...
```
