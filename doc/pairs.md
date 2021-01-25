# Pairs examples

With ``crud.pairs``, you can iterate across a distributed space. The arguments are the same as [``crud.select``](https://github.com/tarantool/crud/docs/select.md), except or the ``use_tomap`` parameter. Below are examples that may help you.

## ``Use_tomap`` parameter

With ``use_tomap`` flag, you can choose to iterate over objects or over tuples. If ``use_tomap = false``, you will iterate over tuples. This parameter is false by default.

**Example:**

```lua
tuples = {}
for _, tuple in crud.pairs('customers', nil, {use_tomap = false}) do
    table.insert(tuples, tuple)
end

tuples
---
- - - 1
    - 2313
    - Alexey
    - 20
  - - 2
    - 241
    - Vladimir
    - 18
  - - 3
    - 571
    - Alexander
    - 24
...
```

If ``use_tomap = true``, you will iterate over objects.

**Example:**

```lua
objects = {}
for _, obj in crud.pairs('customers', nil, {use_tomap = true}) do
    table.insert(tuples, tuple)
end

objects
---
- - id: 1
    bucket_id: 2313
    name: Alexey
    age: 20
  - id: 2
    bucket_id: 241
    name: Vladimir
    age: 14
  - id: 3
    bucket_id: 571
    name: Alexander
    age: 24
...
```

## Pagination

``crud.pairs``, like [``crud.select``](https://github.com/tarantool/crud/doc/select#pagination), supports pagination. To use it, combine the ``first`` and ``after`` parameters. 

**Example:**

```lua
TODO
```

Note that ``crud.pairs``, unlike ``crud.select``, **don't support reverse pagination.**

## Lua Fun

[``Pairs``](https://github.com/tarantool/crud#pairs) is [Lua Fun](https://github.com/luafun/luafun) compatible. Some examples of working with basic functional functions below.

**Filter example:**

```lua
objects = {}
for _, obj in crud.pairs('customers', {{'>=', 'age', 20}}, {use_tomap = true}):filter(function(x) return x.age % 5 == 0 end) do
    table.insert(objects, obj)
end

objects
---
- - id: 1
    bucket_id: 2313
    name: Alexey
    age: 20
...
```

**Reduce (foldl) example:**

```lua
age_sum = crud.pairs('customers', nil, {use_tomap = true}):reduce(function(acc, x) return acc + x.age end, 0)
age_sum
---
- 30
....
```

**Map example:**

```lua
objects = {}
for _, obj in crud.pairs('customers', nil, {use_tomap = true}):map(function(x) return {id = obj.id, name = obj.name, age = obj.age * 2}) do
    table.insert(objects, obj)
end

objects
---
- - id: 1
    name: Alexey
    age: 40
  - id: 2
    name: Vladimir
    age: 28
  - id: 3
    name: Alexander
    age: 48
...
```

**Take example**:

```lua
tuples = {}
for _, tuple in crud.pairs('customers', {{'>=', 'age', 18}}):take(1) do
    table.insert(tuples, tuple)
end

tuples
---
- - - 1
    - 2313
    - Alexey
    - 20
...
```
