# Calling stored functions on storages

The `crud.storage_call()` and `crud.storage_call_many()` methods route named
stored functions to vshard storage masters. See the [API section in the
README](../README.md#storage-call) for argument and result formats.

## Registering a target function

Create the target with a persistent `body` in `box.func` on every storage where
its bucket may be located. Apply this schema change through a storage migration
so that the definition is replicated and survives restarts. The function must
have `setuid = false`:

```lua
box.schema.func.create('app_process_handler', {
    body = [[
        function(event, handler_id)
            return box.atomic(function()
                -- Apply the event and write an idempotency marker here.
                return handler_id
            end)
        end
    ]],
    is_sandboxed = false,
    setuid = false,
    if_not_exists = true,
})

box.schema.user.grant(
    'application_user',
    'execute',
    'function',
    'app_process_handler',
    {if_not_exists = true}
)
```

The user must also have access to every space and object used by the function.
CRUD checks the stored body, registration and `execute` access on the target
storage. It does not resolve functions through Lua globals. A `box.func` entry
without `body` is rejected before execution.

Functions with `setuid = true` are rejected before execution. CRUD calls a
target from Lua under the original caller, where `setuid` does not switch to the
function owner. Rejecting such registrations keeps the contract explicit: a
target always runs with the original caller's privileges.

## Transaction and retry contract

Each target function owns its local transaction. Prefer `box.atomic()` or make
sure that every explicit `box.begin()` is followed by `box.commit()` or
`box.rollback()` before returning. If a function leaves a transaction open,
CRUD rolls it back and reports an item error.

A batch is not a distributed transaction. Calls that already committed remain
committed when another item fails.

CRUD does not add retries of target functions. Vshard may update a route or
repeat the Ref stage before a target starts. A timeout or connection loss may
happen after a function has committed but before the router receives its
response. In that case `may_have_side_effects` is `true`. Use an application
idempotency key and marker if clients may retry the operation.

`storage_call_many()` passes `map_callrw()` a table that maps each bucket ID to
its calls. Vshard groups the table by current bucket location and appends only
the corresponding part to each storage call. An infrastructure error in the
Ref or Map stage is a top-level error of the whole method; partial results from
other replica sets are not returned. Such an error is conservatively marked
with `may_have_side_effects = true`.

## Rolling upgrade

Deploy in this order:

1. Apply storage migrations that create the target functions with `body` in
   `box.func`.
2. Verify that the migrations reached every replicaset and grant caller
   privileges.
3. Update and initialize CRUD on all storages.
4. Verify that `_crud.storage_call_on_storage` and
   `_crud.storage_call_many_on_storage` are registered on each storage.
5. Update CRUD on routers.
6. Grant clients permission to call `crud.storage_call` and/or
   `crud.storage_call_many` through the product ACL.
7. Switch client traffic to the new API.

A new router cannot execute this API through an old storage. The call returns
an initialization/version mismatch error; it never falls back to directly
executing the target name.

## Operational notes

- The timeout covers routing, request dispatch and response collection.
- Batch calls use the Ref and Map stages of `vshard.router:map_callrw()`; stages
  for different replica sets are sent in parallel.
- Calls for one replica set are executed sequentially. Calls for the same
  bucket preserve input order; relative execution order between different
  buckets is not guaranteed.
- Vshard refreshes routes for buckets that moved before the Map stage and pins
  the affected storages while the dispatchers are running.
- A client timeout does not cancel a target function. During the Map stage the
  vshard Ref has no expiration and is released only after the storage dispatcher
  finishes. A stuck function can therefore delay rebalancing of the entire
  storage after the client has already received a timeout.
- CRUD does not enforce a server-side execution timeout. Target functions must
  bound their own execution time and use application-level idempotency.
- Function arguments and return values must be MessagePack-serializable.
- Avoid putting function names or arguments into metric labels. Function names
  have unbounded cardinality, and arguments may contain sensitive data.
