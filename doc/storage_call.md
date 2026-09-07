# Calling stored functions on storages

The `crud.storage_call()` and `crud.storage_call_many()` methods route named
stored functions to storage masters. See the [API section in the
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

Functions with `setuid = true` are rejected before execution. Targets always
run with the original caller's privileges.

## Transaction and retry contract

Each target function owns its local transaction. Prefer `box.atomic()` or make
sure that every explicit `box.begin()` is followed by `box.commit()` or
`box.rollback()` before returning. If a function leaves a transaction open,
CRUD rolls it back and reports an item error.
If rollback fails, CRUD checks the transaction state again before continuing.
If the transaction cannot be closed, execution on that storage stops.
No subsequent item runs inside that transaction.

A batch is not a distributed transaction. Calls that already committed remain
committed when another item fails.

CRUD does not add retries of target functions. A timeout or connection loss may
happen after a function has committed but before the router receives its
response. In that case `may_have_side_effects` is `true`. Use an application
idempotency key and marker if clients may retry the operation.
Exceptions from invoking a target are marked `may_have_side_effects = true`,
including access errors: the same error can originate in a nested call after
the target has already committed changes. CRUD does not infer whether a
function started from its error text.

An infrastructure error while dispatching calls or collecting responses is a
top-level error of the whole method; partial results from other replica sets
are not returned. Such an error is conservatively marked with
`may_have_side_effects = true`.

## Rolling upgrade

Deploy in this order:

1. Apply storage migrations that create the target functions with `body` in
   `box.func`.
2. Verify that the migrations reached every replicaset and grant caller
   privileges.
3. Update and initialize CRUD on all storages.
4. Verify that CRUD initialization completed successfully on every storage.
5. Update CRUD on routers.
6. Grant clients permission to call `crud.storage_call` and/or
   `crud.storage_call_many` through the product ACL.
7. Switch client traffic to the new API.

A new router cannot execute this API through an old storage that does not
support it. Such a storage returns a call error, without a dedicated
version-mismatch error code. In a mixed cluster, calls on updated storages may
already have executed before an old storage returns an error. Do not enable
the API until every storage is ready.

## Operational notes

- `opts.timeout` is a common time budget for routing, dispatching calls and
  waiting for responses, rather than a separate budget for each batch item.
- If a call fails because sharding metadata is outdated, a subsequent request
  obtains fresh metadata. CRUD does not automatically retry the failed target.
- Batch calls are sent concurrently to the affected replica sets. Calls for
  one replica set are executed sequentially. Calls for the same bucket preserve
  input order; relative execution order between different buckets is not
  guaranteed.
- A master change during a request may result in a top-level error. Applications
  must handle ambiguous completion during failover using their own idempotency
  and transaction policy.
- A client timeout or a top-level batch error does not cancel functions already
  running on storages. While a function runs, its declared bucket is protected
  from movement on that instance. A stuck function can therefore delay bucket
  movement even after the client has received an error. This protection does
  not guarantee successful completion during failover or loss of the instance.
- Routing selects the storage for the declared bucket. A target function that
  accesses data from other buckets must provide its own consistency guarantees.
  Other buckets can reside on different replica sets; this API neither places
  them together nor routes accesses made inside the function.
- A top-level batch error does not mean that every item was rolled back. A
  target on another replica set may already have committed, while its partial
  result is not returned. Retrying the whole batch requires idempotency.
- CRUD does not enforce a server-side execution timeout. Target functions must
  bound their own execution time and use application-level idempotency.
- Function arguments and return values must be MessagePack-serializable.
  Each successful result captures the values returned by that call. Later
  changes to shared Lua tables cannot corrupt an earlier result. Result
  serialization hooks run with the original caller's privileges; any
  transaction they leave open is also rolled back and reported as an item
  error. `nil` return values use `box.NULL`, including trailing ones; an
  arbitrary second return value is not an error.
- An unserializable argument passed by a local Lua caller can cause a top-level
  batch error while sending requests, rather than an item validation error.
  Calls on other replica sets may already have been sent.
- There is no built-in limit on batch size or argument/result bytes. Bound
  them in the application. Larger batches and results consume more memory.
  Capacity limits should account for concurrent requests and target functions
  still running after a timeout.
- Avoid putting function names or arguments into metric labels. Function names
  have unbounded cardinality, and arguments may contain sensitive data.
