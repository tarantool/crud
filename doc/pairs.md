# Pairs examples

## Lua Fun

Queries using pairs support the [Lua Fun](https://github.com/luafun/luafun) library. Some examples of working with basic functional functions below.

**Filter example:**
```lua
local objects = {}
for _, obj in fun.filter(function(x) return x.age % 5 == 0 end, crud.pairs('customers', {{'==', 'name', 'Alexey'}}, {use_tomap = true})) do
    table.insert(objects, obj)
end
```

**Reduce (foldl) example:**
```lua
local age_sum = fun.reduce(function(acc, x) return acc + x.age end, 0, crud.pairs('customers', nil, {use_tomap = true}))
```

**Map example:**
```lua
local objects = {}
for _, obj in fun.map(function(x) return {obj.id, obj.name, obj.age * 2} end, crud.pairs('customers', nil, {use_tomap = true}))
```

**Take example**:

```lua
local tuples = {}
for _, tuple in fun.take(3, crud.pairs('customers', {{'>=', 'age', 18}})) do
    table.insert(tuples, tuple)
end
```
