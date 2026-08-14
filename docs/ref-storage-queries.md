# Storage Query Reference (`storage.query`)

Part of the [Storage Module Reference](ref-storage.md).

## Overview

`StoredQuery` is a transient query abstraction designed for live change synchronization and real-time subscription feeds.

Rather than being persisted in storage, a `StoredQuery` represents an active view over a stored object type. It is used to:
1. **List the current matching objects** from storage.
2. **Generate initial snapshots** for client synchronization bootstrapping.
3. **Stream real-time deltas** (`added`, `updated`, `removed`) over SSE storage channels as objects are created, modified, or deleted.

The primary implementation focuses on **owner-scoped queries**, leveraging the hierarchical storage key layout.

---

## Storage Key & Partitioning Model

Stored objects declaring an `OWNERSHIP` relationship are partitioned into keys structured as:

```text
<Collection>.<OwnerPartition>.<LocalID>
```

For example, a `Member` owned by user `USER-123` is stored under key:
```text
Member.USER-123.MBR-456
```

Because all records for a given owner share the key prefix `Member.USER-123.`, `StoredQuery` can efficiently:
- List objects belonging to a specific owner without secondary index overhead.
- Route journal events from `JournalBackend` matching that prefix directly to interested subscribers.

---

## Python Usage

### Creating and Inspecting Queries

```python
from storage.query import StoredQuery
from app.models import Member, User

user = User.Get("USER-123")

# Create an owner-scoped query
query = StoredQuery(Member, owner=user, export={"profile": "summary"})

# 1. Inspect the matching storage key prefix
prefix = query.prefix()  # e.g., "Member.USER-123."

# 2. List current matching objects
members = query.list(start=0, count=10)

# 3. Build a snapshot payload for sync bootstrapping
snapshot = query.snapshot(cursor=backend.getCursor())
```

### Mapping Journal Entries to Query Deltas

When using `JournalBackend`, storage mutations generate journal entries. `query.eventFor()` determines if a journal entry affects the query scope and converts it to a query delta:

```python
# Convert raw journal entry to query change event
event = query.eventFor(journalEntry, backend=storage.backend)
if event:
	# event["change"] is "added", "updated", or "removed"
	print(event["change"], event["id"], event.get("value"))
```

### Python API Reference

```python
StoredQuery(storableClass, owner=None, target=None, export=None)
```

* `storableClass`: The `StoredObject` subclass.
* `owner`: Instance, dictionary with `id`, tuple/list, or raw owner ID string.
* `target`: Optional target dictionary included in snapshot and delta event payloads.
* `export`: Optional serialization options passed to `storable.export(**options)` during snapshot generation.

#### Methods

* `query.prefix() -> str`: Returns the storage key prefix used for subscription routing.
* `query.list(start=0, end=None, count=None) -> list[StoredObject]`: Returns currently matching objects from storage.
* `query.snapshot(cursor=None, start=0, end=None, count=None) -> dict`: Generates a snapshot event dictionary containing current values and cursor.
* `query.eventFor(entry: dict, backend=None) -> Optional[dict]`: Converts a journal entry into a query delta event dictionary (`None` if unrelated).

---

## Web & SSE Protocol

Queries integrate with `StorageServer` real-time push channels (see [Web Storage Reference](ref-storage-web.md)).

### 1. Subscribing to a Query

Clients open an SSE channel and issue a `subscribe` command targeting a `query`:

```json
POST /api/channel/{channelId}/commands
{
  "commands": [
    {
      "op": "subscribe",
      "snapshot": true,
      "target": {
        "kind": "query",
        "type": "members",
        "owner": "USER-123"
      }
    }
  ]
}
```

### 2. Initial Snapshot Event

When `snapshot: true` is requested, the channel immediately emits a `snapshot` event before any live deltas:

```json
{
  "event": "snapshot",
  "cursor": 142,
  "target": {
    "kind": "query",
    "type": "members",
    "owner": "USER-123"
  },
  "count": 2,
  "values": [
    {"id": "MBR-1", "type": "app.models.Member", "name": "Alice"},
    {"id": "MBR-2", "type": "app.models.Member", "name": "Bob"}
  ]
}
```

### 3. Live Query Delta Events

As changes occur in the storage backend, matching journal events are streamed as `query` events:

```json
{
  "event": "query",
  "change": "added",
  "seq": 143,
  "operation": "=",
  "key": "Member.USER-123.MBR-3",
  "type": "members",
  "id": "MBR-3",
  "target": {
    "kind": "query",
    "type": "members",
    "owner": "USER-123"
  },
  "patch": [],
  "value": {"id": "MBR-3", "type": "app.models.Member", "name": "Charlie"}
}
```

#### Change Types (`change`)
* `added`: A new object was created within the query scope.
* `updated`: An existing object in the query scope was modified (includes `patch` diff).
* `removed`: An object in the query scope was deleted.

---

## JavaScript Bridge Usage

The JavaScript bridge (`src/js/storage/index.js` and `src/js/storage/object.js`) provides client-side query handles:

```javascript
import bridge from "storage/index.js"

const storage = bridge({ path: "/api", live: true })

// Create query handle
const members = storage.objects.type("members").query({ owner: "USER-123" })
// or: storage.objects.query("members", { owner: "USER-123" })

// Subscribe to local state updates
const unsubscribe = members.sub((change, query, direction) => {
	console.log(`[${change.kind}]`, change.object, query.values())
})

// Subscribe live SSE channel and wait for initial snapshot
await members.sync()

// Access current cached values
console.log("Current members:", members.values())
```

### JavaScript Query Methods

* `query.values()`: Returns the local array of hydrated `StoredObject` instances.
* `query.sub(callback)`: Registers a listener receiving `(change, query, direction)`.
* `query.unsub(callback)`: Removes a registered listener.
* `await query.sync(options)`: Opens the SSE subscription and resolves after receiving the initial snapshot.

---

## When to Use `StoredQuery`

- **Owner-Scoped Views**: Managing user-owned items, team members, tenant resources.
- **Reactive Client Collections**: Powering real-time dashboards and UI list components without polling.
- **Offline & Reconnect Synchronization**: Bootstrapping state via `snapshot` and applying incremental deltas.

---

## Related References

- [Storage Module Reference](ref-storage.md): Core abstractions, `StoredObject`, ownership, and backends.
- [Web Storage Reference](ref-storage-web.md): REST endpoints, SSE channels, command processing, and JavaScript bridge.
