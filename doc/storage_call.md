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
box.schema.func.create('app.process_handler', {
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
    'app.process_handler',
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
`box.rollback()` on both success and error paths. If a function leaves a
transaction open, CRUD rolls it back and reports an item error before running
the next function in the batch. Changes committed earlier are not rolled back.

A batch is not a distributed transaction. Calls that already committed remain
committed when another item fails.

CRUD retries a sharding metadata mismatch if the target has not started. For a
batch, it retries only if every item failed with such a mismatch. A timeout or
connection loss may happen after a function has committed but before the router
receives its response. `err.may_have_side_effects` for a single call, or
`results[i].error.may_have_side_effects` for a failed batch item, is `false`
when the target did not start. If it is `true`, the target may have run and a
retry requires an application idempotency key. Exceptions from invoking a
target are conservatively marked `true`,
including access errors: the same error can originate in a nested call after
the target has already committed changes. CRUD does not infer whether a
function started from its error text.

An infrastructure error while dispatching calls or collecting responses is a
top-level error of the whole method; partial results from other replica sets
are not returned. Such an error is conservatively marked with
`may_have_side_effects = true`.

## Rolling upgrade

Apply the target-function migrations and upgrade CRUD on all storages before
upgrading routers and enabling the new API. An old storage returns a function-
not-found error; in a batch, items on upgraded storages may already have run.

For Tarantool 3, grant the application user access in the configuration on the
appropriate instances. For example:

```yaml
credentials:
  users:
    application_user:
      privileges:
        - permissions: [execute]
          lua_call: [crud.storage_call, crud.storage_call_many] # routers
        - permissions: [execute]
          functions: [app.process_handler] # storages
```

The user also needs access to the spaces used by the target function.

## Operational notes

- `opts.timeout` is a common budget used by CRUD for routing, dispatching and
  waiting, rather than a separate budget for each batch item. It is not a
  strict wall-clock limit: vshard's discovery of a bucket absent from the
  router cache and a schema refresh can exceed the remaining budget.
- If sharding metadata is outdated, CRUD refreshes it and retries only when
  no target function has run. A batch with partial success is not retried.
- Batch calls are sent concurrently to the affected replica sets. Calls for
  one replica set are executed sequentially. Calls for the same bucket preserve
  input order; relative execution order between different buckets is not
  guaranteed.
- A client timeout or a top-level batch error does not cancel functions already
  running on storages. A single `callrw()` protects its declared bucket. A
  batch `map_callrw()` holds a reference to each affected storage, preventing
  movement of all its buckets until that storage's part of the batch finishes.
  A long-running function can therefore delay movement of unrelated buckets
  even after the client has received an error. Neither kind of reference
  guarantees completion after failover or loss of the instance.
- A function should access data from its declared bucket. Another bucket may
  live on a different replica set; a batch's storage reference does not bring
  that bucket to the same instance.
- A top-level batch error does not mean that every item was rolled back. A
  target on another replica set may already have committed, while its partial
  result is not returned. Retrying the whole batch requires idempotency.
- CRUD does not enforce a server-side execution timeout. Target functions must
  bound their own execution time and use application-level idempotency.
- Function arguments and return values must be MessagePack-serializable. A
  result serialization failure produces a top-level error for the whole call
  or batch, with `may_have_side_effects = true` and no partial results. CRUD
  does not validate or copy results before sending the RPC response. Later
  functions in a batch may mutate an earlier result if they share a Lua table.
  A custom `__serialize` hook may run after the caller's effective-user context
  has ended; do not rely on it running with the caller's privileges.
- A returned `nil` is represented by `box.NULL`. An arbitrary second return
  value is data, not an error. For example, assuming the following persistent
  functions are registered on the storage:

  ```lua
  -- app.ok:       function() return 1, nil, 'x' end
  -- app.soft_err: function() return nil, 'not found' end
  -- app.hard_err: function() error('not found') end

  crud.storage_call('app.ok', {}, {bucket_id = 1})
  -- {returns = {1, box.NULL, 'x'}}, nil
  crud.storage_call('app.soft_err', {}, {bucket_id = 1})
  -- {returns = {box.NULL, 'not found'}}, nil
  crud.storage_call('app.hard_err', {}, {bucket_id = 1})
  -- nil, err
  ```

  To return an error, raise it with `error()` or `box.error()`.
- There is no built-in limit on batch size or argument/result bytes. Bound
  them in the application. Larger batches and results consume more memory.
  Capacity limits should account for concurrent requests and target functions
  still running after a timeout.
