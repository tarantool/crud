# Select examples

## Filtering

The second parameter passed to ``crud.select`` is an array of conditions. Below are examples of filtering data using these conditions. 

### Getting full space

To get a full space without filtering, you need to pass nil as a condition.

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

Let's say we have a ``age`` index. Example below 

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

**Note:**

## Pagination

### First parameter

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

### After parameter

**Example:**

```lua
res, err = crud.select('developers', nil, { after = res.rows[3] })
res.rows
---
- - [4, 501, 'Mikhail', 31]
  - [5, 1997, 'Dmitry', 16]
  - [6, 8765, 'Artyom', 51]
```

### Combine first and after parameters

Select supports pagination. To use it, we have to combine ``after`` and ``first`` parameters. 

**Example:**

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

### Reverse pagination

Select also supports reverse pagination.

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
