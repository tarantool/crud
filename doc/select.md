# Select examples

## Filtering

``CRUD`` allows to filter tuples by conditions. Each condition can use field name (or number) or index name. The first condition that uses index name is used to iterate over space. If there is no conditions that match index names, full scan is performed. Other conditions are used as additional filters. Search condition for the indexed field must be placed first to avoid a full scan.

**Note:** If you specify sharding key or ``bucket_id`` select will be performed on single node. Otherwise Map-Reduce over all nodes will be occurred.

Below are examples of filtering data using these conditions. 

### Getting space

Let's check ``developers`` space content to make other examples more clear. Just select first 6 values without conditions.

**Example:**

```lua
crud.select('developers', nil, { first = 6 })
---
- metadata:
  - {'name': 'id', 'type': 'unsigned'}
  - {'name': 'bucked_id', 'type': 'unsigned'}
  - {'name': 'name', 'type': 'string'}
  - {'name': 'surname', 'type': 'string'}
  - {'name': 'age', 'type': 'number'}
  rows:
  - [1, 7331, 'Alexey', 'Adams', 20]
  - [2, 899, 'Sergey', 'Allred', 21]
  - [3, 9661, 'Pavel', 'Adams', 27]
  - [4, 501, 'Mikhail', 'Liston', 51]
  - [5, 1993, 'Dmitry', 'Jacobi', 16]
  - [6, 8765, 'Alexey', 'Sidorov', 31]
...
```

### Select using index

Let's say we have a ``age`` index. Example below gets a list of ``customers`` over 30 years old.

**Example:**

```lua
crud.select('developers', {{'>=', 'age', 30}})
---
- metadata:
  - {'name': 'id', 'type': 'unsigned'}
  - {'name': 'bucked_id', 'type': 'unsigned'}
  - {'name': 'name', 'type': 'string'}
  - {'name': 'surname', 'type': 'string'}
  - {'name': 'age', 'type': 'number'}
  rows:
  - [6, 8765, 'Alexey', 'Sidorov', 31]
  - [4, 501, 'Mikhail', 'Liston', 51]
...
```

**Note:** results are sorted by age, because first condition is ``age`` index.

### Select using composite index

Suppose we have a composite index consisting of the ``name`` and ``surname`` fields. See example of select queries using such a composited index below.

**Example**:

```lua
crud.select('developers', {{'==', 'full_name', {"Alexey", "Adams"}}})
---
- metadata:
  - {'name': 'id', 'type': 'unsigned'}
  - {'name': 'bucked_id', 'type': 'unsigned'}
  - {'name': 'name', 'type': 'string'}
  - {'name': 'surname', 'type': 'string'}
  - {'name': 'age', 'type': 'number'}
  rows:
  - [1, 7331, 'Alexey', 'Adams', 20]
...
```

### Select using partial key 

Alternatively, you can use a partial key for a composite index.

**Example**:

```lua
crud.select('developers', {{'==', 'full_name', "Alexey"}})
---
- metadata:
  - {'name': 'id', 'type': 'unsigned'}
  - {'name': 'bucked_id', 'type': 'unsigned'}
  - {'name': 'name', 'type': 'string'}
  - {'name': 'surname', 'type': 'string'}
  - {'name': 'age', 'type': 'number'}
  rows:
  - [1, 7331, 'Alexey', 'Adams', 20]
  - [6, 8765, 'Alexey', 'Sidorov', 31]
...
```

**Note:** If you specify partial key not at the first parameter (e.g. ``{{'==', 'full_name', {nil, "Sidorov"}}}``), then full scan will be performed.

### Select using non-indexed field

You can also make a selection using a non-indexed field.

**Example:**

```lua
crud.select('developers', {{'==', 'surname', "Adams"}})
---
- metadata:
  - {'name': 'id', 'type': 'unsigned'}
  - {'name': 'bucked_id', 'type': 'unsigned'}
  - {'name': 'name', 'type': 'string'}
  - {'name': 'surname', 'type': 'string'}
  - {'name': 'age', 'type': 'number'}
  rows:
  - [1, 7331, 'Alexey', 'Adams', 20]
  - [3, 9661, 'Pavel', 'Adams', 27]
...
```

**Note:** in this case full scan is performed.

### Avoiding full scan

**Example:**

```lua
crud.select('developers', {{'==', 'surname', "Adams"}, {'>=', 'age', 25}})
---
- metadata:
  - {'name': 'id', 'type': 'unsigned'}
  - {'name': 'bucked_id', 'type': 'unsigned'}
  - {'name': 'name', 'type': 'string'}
  - {'name': 'surname', 'type': 'string'}
  - {'name': 'age', 'type': 'number'}
  rows:
  - [3, 9661, 'Pavel', 'Adams', 27]
...
```

In this case, a full scan will be performed, since non-indexed field is placed first in search conditions. Example below shows how you can avoid a full scan.

**Example:**

```lua
crud.select('developers', {{'>=', 'age', 30}, {'==', 'surname', "Adams"}})
---
- metadata:
  - {'name': 'id', 'type': 'unsigned'}
  - {'name': 'bucked_id', 'type': 'unsigned'}
  - {'name': 'name', 'type': 'string'}
  - {'name': 'surname', 'type': 'string'}
  - {'name': 'age', 'type': 'number'}
  rows:
  - [3, 9661, 'Pavel', 'Adams', 27]
...
```

## Pagination

[See more](https://github.com/tarantool/crud#select) about ``opts`` parameter.

### ``first`` parameter

Using the ``first`` option we will get the first **N** results of the query.

**Example:**

```lua
res, err = crud.select('developers', nil, { first = 3 })
res
- metadata:
  - {'name': 'id', 'type': 'unsigned'}
  - {'name': 'bucked_id', 'type': 'unsigned'}
  - {'name': 'name', 'type': 'string'}
  - {'name': 'surname', 'type': 'string'}
  - {'name': 'age', 'type': 'number'}
  rows:
  - [1, 7331, 'Alexey', 'Adams', 20]
  - [2, 899, 'Sergey', 'Allred', 21]
  - [3, 9661, 'Pavel', 'Adams', 27]
...
```

Thus, we got the first three objects from the ``developers`` space.

### ``after`` parameter

Using ``after``, we can get the objects after specified tuple.

**Example:**

```lua
res, err = crud.select('developers', nil, { after = res.rows[3] })
res
---
- metadata:
  - {'name': 'id', 'type': 'unsigned'}
  - {'name': 'bucked_id', 'type': 'unsigned'}
  - {'name': 'name', 'type': 'string'}
  - {'name': 'surname', 'type': 'string'}
  - {'name': 'age', 'type': 'number'}
  rows:
  - [4, 501, 'Mikhail', 'Liston', 51]
  - [5, 1993, 'Dmitry', 'Jacobi', 16]
  - [6, 8765, 'Alexey', 'Sidorov', 31]
...
```

With this request, we got objects behind the objects from the [previous example](https://github.com/tarantool/crud/blob/master/doc/select.md#first-parameter)

### Combine ``first`` and ``after`` 

To use pagination, we have to combine ``after`` and ``first`` parameters. 

**Example:**

```lua
res, err = crud.select('developers', nil, { first = 3 })
res
--- Got first three objects
- metadata:
  - {'name': 'id', 'type': 'unsigned'}
  - {'name': 'bucked_id', 'type': 'unsigned'}
  - {'name': 'name', 'type': 'string'}
  - {'name': 'surname', 'type': 'string'}
  - {'name': 'age', 'type': 'number'}
  rows:
  - [1, 7331, 'Alexey', 'Adams', 20]
  - [2, 899, 'Sergey', 'Allred', 21]
  - [3, 9661, 'Pavel', 'Adams', 27]
...
res, err = crud.select('developers', nil, { after = res.rows[3], first = 3 })
res
--- Got the next three objects
- metadata:
  - {'name': 'id', 'type': 'unsigned'}
  - {'name': 'bucked_id', 'type': 'unsigned'}
  - {'name': 'name', 'type': 'string'}
  - {'name': 'surname', 'type': 'string'}
  - {'name': 'age', 'type': 'number'}
  rows:
  - [4, 501, 'Mikhail', 'Liston', 51]
  - [5, 1993, 'Dmitry', 'Jacobi', 16]
  - [6, 8765, 'Alexey', 'Sidorov', 31]
...
```

### Reverse pagination

Select also supports reverse pagination. To use it, pass a negative value to the ``first`` parameter and combine it with ``after`` parameter.

**Example:**

```lua
-- Imagine that user looks at his friends list using very small pages
-- He opens first page, then presses '->' and take next page
-- Then, he wants to return back and presses '<-'
res, err = crud.select('developers', nil, { first = 3 })
res
--- Got first page (first three objects)
- metadata:
  - {'name': 'id', 'type': 'unsigned'}
  - {'name': 'bucked_id', 'type': 'unsigned'}
  - {'name': 'name', 'type': 'string'}
  - {'name': 'surname', 'type': 'string'}
  - {'name': 'age', 'type': 'number'}
  rows:
  - [1, 7331, 'Alexey', 'Adams', 20]
  - [2, 899, 'Sergey', 'Allred', 21]
  - [3, 9661, 'Pavel', 'Adams', 27]
...
res, err = crud.select('developers', nil, { after = res.rows[3], first = 3 })
res
--- Got the next page (next three objects)
- metadata:
  - {'name': 'id', 'type': 'unsigned'}
  - {'name': 'bucked_id', 'type': 'unsigned'}
  - {'name': 'name', 'type': 'string'}
  - {'name': 'surname', 'type': 'string'}
  - {'name': 'age', 'type': 'number'}
  rows:
  - [4, 501, 'Mikhail', 'Liston', 51]
  - [5, 1993, 'Dmitry', 'Jacobi', 16]
  - [6, 8765, 'Alexey', 'Sidorov', 31]
...
res, err = crud.select('developers', nil, { after = res.rows[1], first = -3 })
res
--- Got first page again
- metadata:
  - {'name': 'id', 'type': 'unsigned'}
  - {'name': 'bucked_id', 'type': 'unsigned'}
  - {'name': 'name', 'type': 'string'}
  - {'name': 'surname', 'type': 'string'}
  - {'name': 'age', 'type': 'number'}
  rows:
  - [1, 7331, 'Alexey', 'Adams', 20]
  - [2, 899, 'Sergey', 'Allred', 21]
  - [3, 9661, 'Pavel', 'Adams', 27]
...
```

## `fields` parameter

Result contains only fields specified by `fields` parameter, but scan key and primary key values are merged to the result fields to support pagination (any tuple from result can be simply passed to `after` option).
Using `fields` parameters allows to reduce amount of data transferred from storage.

**Example:**

```lua
-- list space fields
format = box.space.developers:format()
format
- {'name': 'id', 'type': 'unsigned'}
- {'name': 'bucked_id', 'type': 'unsigned'}
- {'name': 'name', 'type': 'string'}
- {'name': 'surname', 'type': 'string'}
- {'name': 'age', 'type': 'number'}
...
-- get names of users that are 27 year old or older
res, err = crud.select('developers', {{'>=', 'age', 27}}, { fields = {'id', 'name'} })
res
- metadata:
  - {'name': 'id', 'type': 'unsigned'}
  - {'name': 'name', 'type': 'string'}
  - {'name': 'age', 'type': 'number'}
  rows:
  - [3, 'Pavel', 27]
  - [6, 'Alexey', 31]
  - [4, 'Mikhail', 51]
```
We got `name` field as it was specified, `age` field because space was scanned by `age` index and primary key `id`.

`after` tuple should contain the same fields as we receive on `select` call with such `fields` parameters.

**Example:**

```lua
-- list space fields
format = box.space.developers:format()
format
- {'name': 'id', 'type': 'unsigned'} 
- {'name': 'bucked_id', 'type': 'unsigned'}
- {'name': 'name', 'type': 'string'}
- {'name': 'surname', 'type': 'string'}
- {'name': 'age', 'type': 'number'}
...
-- get names of users that are 27 year old or older
res, err = crud.select('developers', {{'>=', 'age', 27}}, { fields = {'id', 'name'} })
res
- metadata: 
  - {'name': 'id', 'type': 'unsigned'}
  - {'name': 'name', 'type': 'string'}
  - {'name': 'age', 'type': 'number'}
  rows:
  - [3, 'Pavel', 27]
  - [6, 'Alexey', 31]
  - [4, 'Mikhail', 51]
...
-- get names of users that are 27 year old or older
res, err = crud.select('developers', {{'>=', 'age', 27}}, { fields = {'id', 'name'}, after = res.rows[1] })
res
- metadata:
  - {'name': 'id', 'type': 'unsigned'}
  - {'name': 'name', 'type': 'string'}
  - {'name': 'age', 'type': 'number'}
  rows:
  - [6, 'Alexey', 31]
  - [4, 'Mikhail', 51]
...
```
**THIS WOULD FAIL**
```lua
-- 'fields' isn't specified
res, err = crud.select('developers', {{'>=', 'age', 27}})

-- THIS WOULD FAIL
-- call 'select' with 'fields' option specified 
-- and pass to 'after' tuple that were got without 'fields' option
res, err = crud.select('developers', {{'>=', 'age', 27}}, { fields = {'id', 'name'}, after = res.rows[1] })
```
