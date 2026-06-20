# Query Reference (`storage.query`)

## Overview

`StoredQuery` is a transient query helper for change synchronization.
It is not persisted in storage. Instead, it describes a live view over a
stored object type and can be used to:

- list the current matching objects
- subscribe to future changes
- receive membership deltas as objects are created, updated, or removed

The first implementation is intentionally narrow:

- object queries only
- owner-scoped queries only
- delta delivery over the journal / SSE channel system

## Model

`StoredQuery` wraps:

- a `StoredObject` subclass
- an optional owner
- export options used when serializing snapshot values

The owner scope is derived from the existing storage key layout:

```text
<Collection>.<OwnerId>.<LocalId>
```

This makes owner-scoped queries efficient without a separate index.

## Python Usage

```python
from storage.query import StoredQuery

# List all members owned by a given user
query = StoredQuery(Member, owner=user)
members = query.list()

# Build a snapshot payload for sync bootstrapping
snapshot = query.snapshot(cursor=backend.getCursor())
```

### Current API

- `StoredQuery(storableClass, owner=None, target=None, export=None)`
- `query.prefix()` -> storage key prefix used for subscription routing
- `query.list(start=0, end=None, count=None)` -> current matching objects
- `query.snapshot(cursor=None, ...)` -> snapshot payload for sync bootstrap
- `query.eventFor(entry, backend=None)` -> convert a journal entry into a query delta

## JavaScript Usage

The JavaScript bridge exposes owner-scoped live queries through
`StoredObjectBridge` and `StoredType`.

```js
import bridge from "storage/object.js"

const storage = bridge({ path: "/api", live: true })
const members = storage.type("members").query({ owner: "user-123" })

members.sub((change, query, direction) => {
	console.log(direction, change.kind, query.values())
})

await members.sync()
console.log(members.values())
```

### JavaScript Query API

- `bridge.query(type, {owner})`
- `bridge.type(type).query({owner})`
- `query.values()` -> current cached list
- `query.sub(callback)` / `query.unsub(callback)` -> subscribe to local query state changes
- `await query.sync()` -> subscribe live and wait for the initial snapshot

Subscriber callbacks receive `change.kind` values of:

- `snapshot`
- `added`
- `updated`
- `removed`

For delta changes, `change.object` contains the hydrated `StoredObject` when
the event included a `value` payload.

## Web Usage

Queries are exposed through the existing storage channel endpoint.
Subscribe with a target of kind `query`:

```json
{
  "commands": [
    {
      "op": "subscribe",
      "snapshot": true,
      "target": {
        "kind": "query",
        "type": "members",
        "owner": "user-123"
      }
    }
  ]
}
```

### Events

When `snapshot` is enabled, the channel first emits a snapshot event:

```json
{
  "event": "snapshot",
  "cursor": 42,
  "target": {
    "kind": "query",
    "type": "members",
    "owner": "user-123"
  },
  "count": 2,
  "values": [
    {"id": ["user-123", "a"], "type": "members", "value": "alpha"},
    {"id": ["user-123", "b"], "type": "members", "value": "beta"}
  ]
}
```

Subsequent journal changes are mapped to query deltas:

```json
{
  "event": "query",
  "change": "added",
  "seq": 43,
  "key": "members.user-123.c",
  "type": "members",
  "id": ["user-123", "c"],
  "target": {
    "kind": "query",
    "type": "members",
    "owner": "user-123"
  },
  "value": {"id": ["user-123", "c"], "type": "members", "value": "gamma"}
}
```

`change` values are:

- `added` for a new object in the query scope
- `updated` for an existing matching object that changed
- `removed` for an object removed from the query scope

## When To Use It

Use `StoredQuery` when a client needs to keep an internal list in sync with
the storage backend, especially for:

- lists of objects owned by a user
- dashboards that need incremental refreshes
- UI collections that should update without polling

## Limitations

Current limitations are deliberate and tracked for later work:

- no persisted query definition
- no arbitrary predicates
- no index-backed filtering yet
- no relation-backed queries yet
- no stable ordered result sets beyond the underlying storage order
- no durable resumable query state yet

## Notes

For the first release, owner-scoped queries are the recommended shape.
They map directly to storage keys and work well with the existing journal
subscription machinery.

When the storage key layout changes, owner-scoped querying should move to a
dedicated owner index or query planner.

## Related APIs

- `storage.objects.StoredObject.OwnedBy(owner)`
- `storage.web.StorageChannel`
- `storage.web.StorageServer.resolveJournalTarget()`
- `storage.backends.journal.JournalBackend`
