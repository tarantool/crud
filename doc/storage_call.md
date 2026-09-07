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
If rollback fails, CRUD checks the transaction state again before continuing.
If the transaction cannot be closed, the dispatcher stops and releases its
bucket reference. No subsequent item runs inside that transaction.

A batch is not a distributed transaction. Calls that already committed remain
committed when another item fails.

CRUD does not add retries of target functions. Vshard may update a route or
repeat the Ref stage before a target starts. A timeout or connection loss may
happen after a function has committed but before the router receives its
response. In that case `may_have_side_effects` is `true`. Use an application
idempotency key and marker if clients may retry the operation.
Exceptions from invoking a target are marked `may_have_side_effects = true`,
including access errors: the same error can originate in a nested call after
the target has already committed changes. CRUD does not infer whether a
function started from its error text.

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

A new router cannot execute this API through an old storage. A missing
dispatcher is reported as a call error; there is no dedicated version-mismatch
error code and no fallback to directly executing the target name. In a mixed
cluster, calls on updated storages may already have executed before an old
storage returns an error. Do not enable the API until every storage is ready.

## Operational notes

- The timeout covers routing, request dispatch and response collection. CRUD
  keeps one absolute deadline and passes the remaining time to each blocking
  metadata lookup and to `callrw()` or `map_callrw()`.
- A sharding checksum mismatch invalidates the router metadata cache. The
  next request fetches fresh metadata within its own deadline. CRUD does not
  perform additional blocking metadata reloads while collecting results or
  automatically retry the target.
- Batch calls use the Ref and Map stages of `vshard.router:map_callrw()`; stages
  for different replica sets are sent in parallel.
- Calls for one replica set are executed sequentially. Calls for the same
  bucket preserve input order; relative execution order between different
  buckets is not guaranteed.
- Vshard refreshes routes for buckets that moved before the Map stage. On the
  storage, CRUD also takes a write reference for each declared bucket before
  running that bucket's calls and releases it after execution and transaction
  cleanup.
- If the replicaset master changes between the Ref and Map stages, the new
  master does not have the session-bound Ref. It rejects the Map request before
  the CRUD dispatcher and target functions are called. The batch returns a
  top-level error.
- If one Map request fails, vshard releases its Ref-stage references on all
  affected replica sets. A dispatcher on another replica set may still be
  running, so its per-bucket CRUD reference remains held until the target
  function returns.
- A client timeout does not cancel a target function. Vshard may finish its
  Map cleanup after the timeout, but the per-bucket CRUD reference remains held
  until the target function returns. A stuck function can therefore delay
  movement of its declared bucket after the client has already received a
  timeout.
- Bucket references are local to an instance. A reference held by an old
  master is not replicated or transferred to its successor. It protects
  against bucket movement on the current instance, not against failover or
  the loss of that instance. Applications must handle ambiguous completion
  during failover using their own idempotency and transaction policy.
- The reference protects only the declared routing bucket. A target function
  that accesses data from other buckets must provide its own consistency
  guarantees. Other buckets can reside on different replica sets; this API
  neither places them together nor routes accesses made inside the function.
- A top-level batch error does not mean that every item was rolled back. A
  target on another replica set may already have committed, while its partial
  result is not returned. Retrying the whole batch requires idempotency.
- CRUD does not enforce a server-side execution timeout. Target functions must
  bound their own execution time and use application-level idempotency.
- Function arguments and return values must be MessagePack-serializable.
  CRUD freezes each successful result by encoding and decoding it before
  processing the next item. Later changes to shared Lua tables cannot corrupt
  an earlier result. Result serialization hooks run with the original caller's
  privileges; any transaction they leave open is also rolled back and reported
  as an item error. `nil` return values use `box.NULL`, including trailing
  ones; an arbitrary second return value is not an error.
- An unserializable argument passed by a local Lua caller is discovered when
  vshard serializes Map arguments. This is a top-level Map error, not an item
  validation error; other replica sets may already have received their Map.
- There is no built-in limit on batch size or argument/result bytes. Bound
  them in the application. Results, MessagePack snapshots and routing tables
  consume memory proportional to their sizes. Capacity limits should account
  for concurrent requests and target functions still running after a timeout.
- Avoid putting function names or arguments into metric labels. Function names
  have unbounded cardinality, and arguments may contain sensitive data.
