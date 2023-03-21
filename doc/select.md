# Select examples

## Table of Contents

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->

- [Filtering](#filtering)
  - [Examples schema](#examples-schema)
  - [Getting space](#getting-space)
  - [Select using index](#select-using-index)
  - [Select using composite index](#select-using-composite-index)
  - [Select using partial key](#select-using-partial-key)
  - [Select using non-indexed field](#select-using-non-indexed-field)
  - [Avoiding full scan](#avoiding-full-scan)
- [Pagination](#pagination)
  - [``first`` parameter](#first-parameter)
  - [``after`` parameter](#after-parameter)
  - [Combine ``first`` and ``after``](#combine-first-and-after)
  - [Reverse pagination](#reverse-pagination)
- [`fields` parameter](#fields-parameter)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

## Filtering

``CRUD`` allows to filter tuples by conditions. Each condition can use field name or index name. (Refer to [#352](https://github.com/tarantool/crud/issues/352) for field number.) The first condition that uses index name is used to iterate over space. If there is no conditions that match index names, full scan is performed. Other conditions are used as additional filters. Search condition for the indexed field must be placed first to avoid a full scan. In additional, don't forget to limit amount of results with ``first`` parameter. This will help to avoid too long selects in production.

**Note:** If you specify sharding key or ``bucket_id`` select will be performed on single node. Otherwise Map-Reduce over all nodes will be occurred.

Below are examples of filtering data using these conditions.

### Examples schema

```lua
box.space.developers:format()
---
- {'name': 'id', 'type': 'unsigned'}
- {'name': 'bucket_id', 'type': 'unsigned'}
- {'name': 'name', 'type': 'string'}
- {'name': 'surname', 'type': 'string'}
- {'name': 'age', 'type': 'number'}
...
box.space.developers.index
- 0: &0
    unique: true
    parts:
    - type: unsigned
      is_nullable: false
      fieldno: 1
    id: 0
    type: TREE
    name: primary_index
  1: &1
    unique: false
    parts:
    - type: number
      is_nullable: false
      fieldno: 5
    id: 1
    type: TREE
    name: age_index
  2: &2
    unique: false
    parts:
    - type: string
      is_nullable: false
      fieldno: 3
    - type: string
      is_nullable: false
      fieldno: 4
    id: 2
    type: TREE
    name: full_name
...
```

### Getting space

Let's check ``developers`` space content to make other examples more clear. Just select first 6 values without conditions.

**Example:**

```lua
crud.select('developers', nil, { first = 6 })
---
- metadata:
  - {'name': 'id', 'type': 'unsigned'}
  - {'name': 'bucket_id', 'type': 'unsigned'}
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

We have an ``age_index`` index. Example below gets a list of ``customers`` over 30 years old.

**Example:**

```lua
crud.select('developers', {{'>=', 'age_index', 30}}, {first = 10})
---
- metadata:
  - {'name': 'id', 'type': 'unsigned'}
  - {'name': 'bucket_id', 'type': 'unsigned'}
  - {'name': 'name', 'type': 'string'}
  - {'name': 'surname', 'type': 'string'}
  - {'name': 'age', 'type': 'number'}
  rows:
  - [6, 8765, 'Alexey', 'Sidorov', 31]
  - [4, 501, 'Mikhail', 'Liston', 51]
...
```

**Note:** results are sorted by age, because first condition is ``age`` index.

**Note**: index is named ``age_index``, but we can still query by the field name ``age``
and the search will be done using index without a full scan. If the names of index and
field match, the search will also be performed using index.

These two queries are equivalent, the search will be done using index in both cases:

```lua
crud.select('developers', {{'>=', 'age_index', 30}}, {first = 10})
crud.select('developers', {{'>=', 'age', 30}}, {first = 10})
```

### Select using composite index

Suppose we have a composite index consisting of the ``name`` and ``surname`` fields. See example of select queries using such a composited index below.

**Example**:

```lua
crud.select('developers', {{'==', 'full_name', {"Alexey", "Adams"}}}, {first = 10})
---
- metadata:
  - {'name': 'id', 'type': 'unsigned'}
  - {'name': 'bucket_id', 'type': 'unsigned'}
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
crud.select('developers', {{'==', 'full_name', "Alexey"}}, {first = 10})
---
- metadata:
  - {'name': 'id', 'type': 'unsigned'}
  - {'name': 'bucket_id', 'type': 'unsigned'}
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
crud.select('developers', {{'==', 'surname', "Adams"}}, {first = 10})
---
- metadata:
  - {'name': 'id', 'type': 'unsigned'}
  - {'name': 'bucket_id', 'type': 'unsigned'}
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

Most requests lead to a full scan. A critical log entry containing the current stack traceback is created upon such calls with a message: `Potentially long select from space '%space_name%'`.

**Example:**

```lua
crud.select('developers', {{'==', 'surname', "Adams"}, {'>=', 'age', 25}})
---
2022-05-24 14:06:31.748 [25108] main/103/playground.lua C> Potentially long select from space 'developers'
 stack traceback:
	.rocks/share/tarantool/crud/select/compat/common.lua:24: in function 'check_select_safety'
	.rocks/share/tarantool/crud/select/compat/select.lua:298: in function <.rocks/share/tarantool/crud/select/compat/select.lua:252>
	[C]: in function 'pcall'
	.rocks/share/tarantool/crud/common/sharding/init.lua:163: in function <.rocks/share/tarantool/crud/common/sharding/init.lua:158>
	[C]: in function 'xpcall'
	.rocks/share/tarantool/errors.lua:145: in function <.rocks/share/tarantool/errors.lua:139>
	[C]: in function 'pcall'
	builtin/box/console.lua:403: in function 'eval'
	builtin/box/console.lua:709: in function 'repl'
	builtin/box/console.lua:758: in function 'start'
	doc/playground.lua:164: in main chunk
- metadata:
  - {'name': 'id', 'type': 'unsigned'}
  - {'name': 'bucket_id', 'type': 'unsigned'}
  - {'name': 'name', 'type': 'string'}
  - {'name': 'surname', 'type': 'string'}
  - {'name': 'age', 'type': 'number'}
  rows:
  - [3, 9661, 'Pavel', 'Adams', 27]
...
```

You can avoid the full scan with '=' or '==' condition on index fields.

**Example:**

```lua
crud.select('developers', {{'==', 'surname', "Adams"}, {'==', 'age', 27}})
---
- metadata:
  - {'name': 'id', 'type': 'unsigned'}
  - {'name': 'bucket_id', 'type': 'unsigned'}
  - {'name': 'name', 'type': 'string'}
  - {'name': 'surname', 'type': 'string'}
  - {'name': 'age', 'type': 'number'}
  rows:
  - [3, 9661, 'Pavel', 'Adams', 27]
...
```

The order of conditions is important. A first condition on index fields determines whether a full scan will be performed or not.

**Example:**

```lua
crud.select('developers', {{'==', 'surname', "Adams"}, {'>', 'id', 0}, {'==', 'age', 27}})
2022-05-24 14:07:26.289 [29561] main/103/playground.lua C> Potentially long select from space 'developers'
 stack traceback:
	.rocks/share/tarantool/crud/select/compat/common.lua:24: in function 'check_select_safety'
	.rocks/share/tarantool/crud/select/compat/select.lua:298: in function <.rocks/share/tarantool/crud/select/compat/select.lua:252>
	[C]: in function 'pcall'
	.rocks/share/tarantool/crud/common/sharding/init.lua:163: in function <.rocks/share/tarantool/crud/common/sharding/init.lua:158>
	[C]: in function 'xpcall'
	.rocks/share/tarantool/errors.lua:145: in function <.rocks/share/tarantool/errors.lua:139>
	[C]: in function 'pcall'
	builtin/box/console.lua:403: in function 'eval'
	builtin/box/console.lua:709: in function 'repl'
	builtin/box/console.lua:758: in function 'start'
	doc/playground.lua:164: in main chunk
- metadata:
  - {'name': 'id', 'type': 'unsigned'}
  - {'name': 'bucket_id', 'type': 'unsigned'}
  - {'name': 'name', 'type': 'string'}
  - {'name': 'surname', 'type': 'string'}
  - {'name': 'age', 'type': 'number'}
  rows:
  - [3, 9661, 'Pavel', 'Adams', 27]
...
```

Also, you can avoid the critical message with parameter ``first`` <= 1000.

**Example:**

```lua
crud.select('developers', {{'==', 'surname', "Adams"}, {'>=', 'age', 25}}, {first = 10})
---
- metadata:
  - {'name': 'id', 'type': 'unsigned'}
  - {'name': 'bucket_id', 'type': 'unsigned'}
  - {'name': 'name', 'type': 'string'}
  - {'name': 'surname', 'type': 'string'}
  - {'name': 'age', 'type': 'number'}
  rows:
  - [3, 9661, 'Pavel', 'Adams', 27]
...
```

Or you can do it with parameter ``fullscan=true`` if you know what you're doing (a small space).

**Example:**

```lua
crud.select('developers', {{'==', 'surname', "Adams"}, {'>=', 'age', 25}}, {fullscan = true})
---
- metadata:
  - {'name': 'id', 'type': 'unsigned'}
  - {'name': 'bucket_id', 'type': 'unsigned'}
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
  - {'name': 'bucket_id', 'type': 'unsigned'}
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
res, err = crud.select('developers', nil, { after = res.rows[3], first = 5 })
res
---
- metadata:
  - {'name': 'id', 'type': 'unsigned'}
  - {'name': 'bucket_id', 'type': 'unsigned'}
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
  - {'name': 'bucket_id', 'type': 'unsigned'}
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
  - {'name': 'bucket_id', 'type': 'unsigned'}
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
  - {'name': 'bucket_id', 'type': 'unsigned'}
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
  - {'name': 'bucket_id', 'type': 'unsigned'}
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
  - {'name': 'bucket_id', 'type': 'unsigned'}
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
- {'name': 'bucket_id', 'type': 'unsigned'}
- {'name': 'name', 'type': 'string'}
- {'name': 'surname', 'type': 'string'}
- {'name': 'age', 'type': 'number'}
...
-- get names of users that are 27 years old or older
res, err = crud.select('developers', {{'>=', 'age', 27}}, { fields = {'id', 'name'}, first = 10 })
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
We got `name` field as it was specified, `age` field because space was scanned by `age_index` index and primary key `id`.

`after` tuple should contain the same fields as we receive on `select` call with such `fields` parameters.

**Example:**

```lua
-- list space fields
format = box.space.developers:format()
format
- {'name': 'id', 'type': 'unsigned'} 
- {'name': 'bucket_id', 'type': 'unsigned'}
- {'name': 'name', 'type': 'string'}
- {'name': 'surname', 'type': 'string'}
- {'name': 'age', 'type': 'number'}
...
-- get names of users that are 27 years old or older
res, err = crud.select('developers', {{'>=', 'age', 27}}, { fields = {'id', 'name'}, first = 10 })
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
-- get names of users that are 27 years old or older
res, err = crud.select('developers', {{'>=', 'age', 27}}, { fields = {'id', 'name'}, after = res.rows[1], first = 10 })
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
res, err = crud.select('developers', {{'>=', 'age', 27}}, {first = 10})

-- THIS WOULD FAIL
-- call 'select' with 'fields' option specified 
-- and pass to 'after' tuple that were got without 'fields' option
res, err = crud.select('developers', {{'>=', 'age', 27}}, { fields = {'id', 'name'}, after = res.rows[1], first = 10 })
```
You could use `crud.cut_rows` function to cut off scan key and primary key values that were merged to the result fields.

**Example:**

```lua
-- get names of users that are 27 years old or older
res, err = crud.select('developers', {{'>=', 'age', 27}}, { fields = {'id', 'name'}, first = 10 })
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
res, err = crud.cut_rows(res.rows, res.metadata, {'id', 'name'})
res
- metadata:
  - {'name': 'id', 'type': 'unsigned'}
  - {'name': 'name', 'type': 'string'}
  rows:
  - [3, 'Pavel']
  - [6, 'Alexey']
  - [4, 'Mikhail']
...
```
