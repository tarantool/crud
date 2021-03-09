# Pairs examples

With ``crud.pairs``, you can iterate across a distributed space.  
The arguments are the same as [``crud.select``](https://github.com/tarantool/crud/blob/master/doc/select.md), except of the ``use_tomap`` parameter.  
Below are examples that may help you.

## Getting space

Let's check ``developers`` space contents to make other examples more clear. Just select first 4 values without conditions.

**Example:**

```lua
tuples = {}
for _, tuple in crud.pairs('developers', nil, { first = 4 }) do
  table.insert(tuples, tuple)
end

tuples
---
- - - 1 -- id
    - 7331 -- bucket_id
    - Alexey -- name
    - Adams -- surname
    - 20 -- age
  - - 2
    - 899
    - Sergey
    - Allred
    - 21
  - - 3
    - 9661
    - Pavel
    - Adams
    - 27
  - - 4
    - 501
    - Mikhail
    - Liston
    - 51
...
```

## ``use_tomap`` parameter

With ``use_tomap`` flag, you can choose to iterate over objects or over tuples.  
If ``use_tomap = true``, you will iterate over objects. This parameter is false by default.

**Example:**

```lua
objects = {}
for _, obj in crud.pairs('developers', nil, { use_tomap = true, first = 3 }) do
    table.insert(tuples, tuple)
end

objects
---
- - id: 1
    bucket_id: 7331
    name: Alexey
    surname: Adams
    age: 20
  - id: 2
    bucket_id: 899
    name: Sergey
    surname: Allred
    age: 21
  - id: 3
    bucket_id: 9661
    name: Pavel
    surname: Adams
    age: 27
...
```

## Pagination

``crud.pairs``, like [``crud.select``](https://github.com/tarantool/crud/blob/master/doc/select.md#pagination), supports pagination.  
To use it, combine the ``first`` and ``after`` parameters. 

**Example:**

```lua
tuples = {}
for _, tuple in crud.pairs('developers', nil, { first = 2 }) do
    table.insert(tuples, tuple) -- Got first two tuples
end

tuples
--- 
- - - 1
    - 7331
    - Alexey
    - Adams
    - 20
  - - 2
    - 899
    - Sergey
    - Allred
    - 21
...
new_tuples = {}
for _, tuple in crud.pairs('developers', nil, { after = tuples[2], first = 2 }) do
    table.insert(new_tuples, tuple) -- Got next two tuples
end

new_tuples
--- 
- - - 3
    - 9661
    - Pavel
    - Adams
    - 27
  - - 4
    - 501
    - Mikhail
    - Liston
    - 51
...
```

Note that ``crud.pairs``, unlike ``crud.select``, **doesn't support reverse pagination.**

## Lua Fun

[``Pairs``](https://github.com/tarantool/crud#pairs) is [Lua Fun](https://github.com/luafun/luafun) compatible. Some examples of working with basic functional functions below.

**Filter example:**

```lua
objects = {}
for _, obj in crud.pairs('developers', {{'>=', 'age', 20}}, { use_tomap = true }):filter(function(x) return x.age % 5 == 0 end) do
    table.insert(objects, obj)
end

objects
---
- - id: 1
    bucket_id: 7331
    name: Alexey
    surname: Adams
    age: 20
...
```

**Reduce (foldl) example:**

```lua
age_sum = crud.pairs('developers', nil, { use_tomap = true }):reduce(function(acc, x) return acc + x.age end, 0)
age_sum
---
- 166
....
```

**Map example:**

```lua
objects = {}
for _, obj in crud.pairs('developers', nil, { use_tomap = true }):map(function(x) return {id = obj.id, name = obj.name, age = obj.age * 2}) do
    table.insert(objects, obj)
end

objects
---
- - id: 1
    name: Alexey
    age: 40
  - id: 2
    name: Sergey 
    age: 42
  - id: 3
    name: Pavel
    age: 54
  - id: 4
    name: Mikhail
    age: 102
  - id: 5
    name: Dmitry
    age: 32
  - id: 6
    name: Alexey
    age: 62
...
```

**Take example**:

```lua
tuples = {}
for _, tuple in crud.pairs('developers', {{'>=', 'age', 25}}):take(2) do
    table.insert(tuples, tuple)
end

tuples
---
- - - 3
    - 9661
    - Pavel
    - Adams
    - 27
  - - 4
    - 501
    - Mikhail
    - Liston
    - 51
...
```
## ``fields`` parameter

Result contains only fields specified by ``fields`` parameter, but scan key and primary key values are merged to the result fields to support pagination (any tuple from result can be simply passed to ``after`` option).
Using ``fields`` parameters allows to reduce amount of data transferred from storage.

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
objects = {}
-- condition by indexed non-unique non-primary field
for _, obj in crud.pairs('developers', {{'>=', 'age', 31}},  {use_tomap = true, fields = {'name'}}) do
    table.insert(objects, obj)
end

objects
---
- - id: 6
    name: Alexey
    age: 31
  - id: 4
    name: Mikhail
    age: 51
...
```
We got ``name`` field as it was specified, ``age`` field because space was scanned by ``age`` index and primary key ``id``.

``after`` tuple should contain the same fields as we receive on ``pairs`` call with such ``fields`` parameters.

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
tuples = {}
for _, tuple in crud.pairs('developers', {{'>=', 'age', 27}}, { fields = {'id', 'name'} }) do
    table.insert(tuples, tuple)
end

tuples
---
- - - 3
    - Pavel
    - 27
  - - 6
    - Alexey
    - 31
  - - 4
    - Mikhail
    - 51
...
new_tuples = {}
for _, tuple in crud.pairs('developers', {{'>=', 'age', 27}}, { fields = {'id', 'name'}, after = tuples[1]}) do
    table.insert(new_tuples, tuple)
end

new_tuples
---
- - - 6
    - Alexey
    - 31
  - - 4
    - Mikhail
    - 51
...

tuples = {}
for _, tuple in crud.pairs('developers', {{'>=', 'age', 27}}) do  -- 'fields' isn't specified
    table.insert(tuples, tuple)
end

-- THIS WOULD FAIL
-- call 'pairs' with 'fields' option specified 
-- and pass to 'after' tuple that were got without 'fields' option
new_tuples = {}
for _, tuple in crud.pairs('developers', {{'>=', 'age', 27}}, { fields = {'id', 'name'}, after = tuples[1]}) do
    table.insert(new_tuples, tuple)
end
```
