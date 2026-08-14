# Web Storage Reference (`storage.web`)

Part of the [Storage Module Reference](ref-storage.md).

## Overview

The `storage.web` module exposes storable classes (`StoredObject`, `StoredRaw`) and key-value stores (`KVStorage`) over HTTP as a REST API with real-time push channels.

Built on the `extra` microservice framework, it automatically handles:
- **Automatic CRUD routing**: Read, create, update, delete, pagination, and relations.
- **Multiple representations**: Content negotiation returning JSON by default, or Markdown (`.md`) and XML (`.xml`) via route suffixes.
- **Real-time SSE push channels**: Server-Sent Events streaming create, update, remove, query deltas, and batched transaction notifications.
- **Transactional command batching**: Executing multiple mutations atomically with deferred notifications.
- **Custom method RPC**: Exposing domain methods decorated with `@http`.
- **Key-Value API**: Mounting `KVStorage` stores with single, batch, and pagination operations.
- **JavaScript client bridge**: Isomorphic browser/Node.js client (`src/js/storage/index.js`).

---

## Decorators (`@http`)

The `@http` decorator marks classes and custom methods for web exposure.

### 1. Class-Level Exposure

Decorate a `StoredObject` or `StoredRaw` class to expose it as an HTTP resource:

```python
from storage import Types
from storage.objects import StoredObject
from storage.web import http

@http(url="items", export="default", restrict=None)
class WebItem(StoredObject):
	PROPERTIES = dict(
		name=Types.STRING,
		value=Types.STRING,
	)
```

**Parameters for `@http` at Class Level:**
* `url` (*str*): Base URL segment under which this class is exposed (e.g. `items`). Defaults to the lowercase class name if omitted.
* `restrict` (*any*): Access control metadata evaluated by request filters.
* `methods` (*str | list[str]*): Allowed HTTP methods (defaults to `["GET", "POST"]`).
* `export` (*str | dict*): Export options passed to `storable.export(**options)` on serialization. A string is converted to `{"profile": export}`.

### 2. Method-Level Exposure

Decorate custom instance methods to expose them as RPC endpoints on specific objects:

```python
from storage.objects import StoredObject
from storage.web import http

@http("items")
class WebItem(StoredObject):
	PROPERTIES = dict(value=Types.STRING)

	@http("rename", methods="POST")
	def rename(self, value: str):
		self.value = value
		self.save()
		return {"success": True, "value": self.value}

	@http("describe", methods="GET")
	def describe(self, prefix: str = ""):
		return {"description": prefix + self.value}
```

**Parameters for `@http` at Method Level:**
* `url` (*str*): Sub-segment triggering this method (e.g. `/api/items/{id}/rename`).
* `restrict` (*any*): Method-level access control metadata.
* `methods` (*str | list[str]*): Allowed HTTP methods (defaults to `("GET", "POST")`).
* `contentType` (*str | callable*): Response media type. If a callable is passed, it receives the storable instance as an argument.

---

## The `StorageServer` Service

`StorageServer` is an `extra.Service` router that publishes endpoints for registered storable classes and KV stores.

### Initialization & Registration

```python
from storage.web import StorageServer
from storage.kv import KVStorage, StringKVKeyNormalizer
from storage.formats import JSONCodec
from storage.backends.memory import KVMemoryBackend
from app.models import WebItem, Tag

# Create server
server = StorageServer(prefix="/api", classes=[WebItem], readonly=False)

# Register storable classes dynamically
server.use(Tag)
# or: server.add(Tag)

# Mount key-value stores
cacheStore = KVStorage(KVMemoryBackend(), normalizer=StringKVKeyNormalizer(), codec=JSONCodec())
server.useKV("cache", cacheStore)
```

**Constructor Parameters:**
* `prefix` (*str*): Base URL prefix for all endpoints. Defaults to `"/api"`.
* `classes` (*iterable*): Initial list of decorated storable classes.
* `readonly` (*bool*): When `True`, write operations return `403 Forbidden` (`notAuthorized`).

---

## Multiple Representations (JSON, Markdown, XML)

Read endpoints support three representations:
1. **JSON (Default)**: Standard JSON envelopes and payloads.
2. **Markdown (`.md`)**: Appending `.md` to any GET route renders the payload as structured `text/markdown`.
3. **XML (`.xml`)**: Appending `.xml` to any GET route renders the payload as `application/xml`.

Example:
* `GET /api/items/123` -> JSON
* `GET /api/items/123.md` -> Markdown
* `GET /api/items/123.xml` -> XML

---

## Real-Time SSE Push Channels

`StorageServer` provides Server-Sent Events (SSE) channels streaming live object changes, query deltas, and transaction batches.

### 1. Channel Lifecycle

```
Client                                  Server
  │                                       │
  ├─── POST /api/channel ───────────────>│ (Create channel)
  │<── {"id": "CH-1", "events": "..."} ───┤
  │                                       │
  ├─── GET /api/channel/CH-1/events ─────>│ (Open SSE stream)
  │                                       │
  ├─── POST /api/channel/CH-1/commands ──>│ (Subscribe to target)
  │    {"op": "subscribe", "target": ...} │
  │                                       │
  │<── SSE: event="update" ───────────────┤ (Stream live changes)
  │                                       │
  ├─── POST /api/channel/CH-1/heartbeat ─>│ (Keep alive)
  ├─── POST /api/channel/CH-1/close ─────>│ (Close channel)
```

### 2. Channel Subscriptions

Subscribe to an object, collection, or query:

```json
POST /api/channel/{channelId}/commands
{
  "commands": [
    {
      "op": "subscribe",
      "target": {
        "kind": "object",
        "type": "items",
        "id": "123"
      }
    },
    {
      "op": "subscribe",
      "snapshot": true,
      "target": {
        "kind": "query",
        "type": "members",
        "owner": "user-456"
      }
    }
  ]
}
```

### 3. Buffering and Flow Control

Control delivery buffering for a channel:
* `{"op": "block"}`: Starts buffering notifications for the channel.
* `{"op": "flush"}`: Emits buffered changes as a single `batch` SSE event while keeping the channel blocked.
* `{"op": "unblock"}`: Emits all remaining buffered changes as one `batch` event and resumes immediate delivery.

### 4. SSE Event Payloads

#### Single Mutation Event (`create`, `update`, `remove`)

```json
{
  "event": "update",
  "seq": 12,
  "operation": "+",
  "key": "items:123",
  "type": "items",
  "id": "123",
  "revision": {"name": 1710000000},
  "patch": [
    {"op": "replace", "path": "/name", "value": "New Item Name"}
  ],
  "relations": {
    "tags": {
      "added": [{"id": "7", "type": "Tag"}],
      "removed": [{"id": "2", "type": "Tag"}]
    }
  },
  "target": {"kind": "object", "type": "items", "id": "123"},
  "value": {"id": "123", "name": "New Item Name"}
}
```

#### Batch Event (`batch`)

Emitted for transactional command batches and blocked channel flushes:

```json
{
  "event": "batch",
  "count": 2,
  "changed": ["items:123", "items:456"],
  "from": 12,
  "to": 13,
  "events": [
    {"event": "update", "id": "123", "type": "items"},
    {"event": "update", "id": "456", "type": "items"}
  ]
}
```

---

## Command Endpoint (`POST /api/commands`)

The command endpoint executes multiple mutating operations in a single request. When `"transaction": true` is specified, changes are applied atomically and SSE notifications are deferred until completion, emitting a single `batch` event per subscriber.

```json
POST /api/commands
{
  "transaction": true,
  "commands": [
    {"op": "create", "type": "items", "fields": {"name": "Alpha"}},
    {"op": "update", "type": "items", "id": "123", "fields": {"name": "Beta"}},
    {"op": "remove", "type": "items", "id": "789"},
    {"op": "relation.append", "type": "items", "id": "123", "relation": "tags", "values": [{"id": "7"}]},
    {"op": "invoke", "type": "items", "id": "123", "method": "rename", "body": {"value": "Gamma"}}
  ]
}
```

### Supported Command Operations

* `create`: Create a new object.
* `update`: Update properties on an existing object.
* `remove`: Delete an object.
* `relation.*`: `relation.set`, `relation.append`, `relation.prepend`, `relation.insert`, `relation.delete`, `relation.remove`, `relation.swap`, `relation.move`, `relation.clear`.
* `invoke`: Execute an `@http` method on an object.

---

## REST API Endpoint Routing Table

For a class `WebItem` exposed at `items` with prefix `/api`:

| Method | Route Pattern | Formats | Description |
| :--- | :--- | :--- | :--- |
| `GET`/`POST` | `/api/items` | JSON | Create new object |
| `GET` | `/api/items/{id}` | JSON, `.md`, `.xml` | Retrieve object |
| `POST` | `/api/items/{id}` | JSON | Update object properties |
| `POST` | `/api/items/{id}/remove` | JSON | Delete object |
| `GET` | `/api/items/list` | JSON, `.md`, `.xml` | List objects (default count 20) |
| `GET` | `/api/items/list/{start}:{end}` | JSON, `.md`, `.xml` | Paginated list of objects |
| `GET`/`POST` | `/api/items/{id}/{method}` | JSON, `.md`, `.xml` (GET) | Invoke custom instance method |
| `GET` | `/api/items/{id}/relations` | JSON, `.md`, `.xml` | List all relations for object |
| `GET` | `/api/items/{id}/relations/{rel}/count` | JSON, `.md`, `.xml` | Count items in relation |
| `GET` | `/api/items/{id}/relations/{rel}/list` | JSON, `.md`, `.xml` | Paginate relation items |
| `POST` | `/api/items/{id}/relations/{rel}/{op}` | JSON | Mutate relation (`append`, `remove`, etc.) |
| `GET` | `/api/blobs/{id}/data` | Binary stream | Stream raw file data (`StoredRaw`) |
| `POST` | `/api/commands` | JSON | Batch / transactional command execution |
| `POST` | `/api/channel` | JSON | Open real-time SSE channel |
| `GET` | `/api/channel/{id}/events` | SSE stream | Stream live change events |
| `POST` | `/api/channel/{id}/commands` | JSON | Send channel commands (`subscribe`, `block`, etc.) |

### Key-Value Store Endpoints (`/api/kv/{name}`)

For a KV store registered with `server.useKV("cache", store)`:

| Method | Route Pattern | Formats | Description |
| :--- | :--- | :--- | :--- |
| `GET` | `/api/kv/cache` | JSON, `.md`, `.xml` | Describe store capabilities |
| `GET` | `/api/kv/cache/size` | JSON, `.md`, `.xml` | Get entry count |
| `GET` | `/api/kv/cache/has/{key}` | JSON, `.md`, `.xml` | Check key presence |
| `GET` | `/api/kv/cache/get/{key}` | JSON, `.md`, `.xml` | Get value for key |
| `POST` | `/api/kv/cache/set/{key}` | JSON | Set value for key |
| `POST` | `/api/kv/cache/delete/{key}` | JSON | Delete key |
| `GET` | `/api/kv/cache/list` | JSON, `.md`, `.xml` | Paginate keys (supports `?prefix=`) |
| `GET` | `/api/kv/cache/items` | JSON, `.md`, `.xml` | Paginate key-value pairs |
| `POST` | `/api/kv/cache/clear` | JSON | Clear all entries |
| `POST` | `/api/kv/cache/commands` | JSON | Batch KV commands |

---

## Hosting with `extra`

Mount `StorageServer` on an `extra.model.Application`:

```python
from extra.model import Application
from storage.web import StorageServer
from app.models import Article, Account

server = StorageServer(prefix="/api", classes=[Article, Account])

app = Application()
app.mount(server)

if __name__ == "__main__":
	import asyncio
	asyncio.run(app.start())
```

---

## JavaScript Client Bridge

The JavaScript bridge (`src/js/storage/index.js`) provides an isomorphic client for browsers and Node.js.

### 1. Initialization

```javascript
import bridge from "storage/index.js"

// Unified bridge providing both objects and KV
const storage = bridge({ path: "/api", live: true })
```

### 2. Working with Stored Objects

```javascript
const items = storage.objects.type("items")

// Create
const item = await items.create({ name: "Widget", value: "100" })

// Get and update
const fetched = await items.get("123")
fetched.set("name", "Updated Widget")
await fetched.push()

// Custom RPC method
const response = await fetched.call("rename", { value: "Super Widget" })

// Relations
const tags = fetched.relation("tags")
await tags.append([{ id: "TAG-1" }])
const tagList = await tags.all()

// Paginated listing
for await (const obj of items.ilist({ count: 20 })) {
	console.log(obj.get("name"))
}
```

### 3. Working with Key-Value Stores

```javascript
const cache = storage.kv.store("cache")

await cache.set("user:123", { name: "Alice", active: true })
const user = await cache.get("user:123")
const exists = await cache.has("user:123")

// Paginate items
for await (const entry of cache.iitems({ prefix: "user:" })) {
	console.log(entry.key, entry.value)
}

await cache.delete("user:123")
```

---

## Related References

- [Storage Module Reference](ref-storage.md): Core abstractions, `StoredObject`, `StoredRaw`, `KVStorage`, and backends.
- [Storage Query Reference](ref-storage-queries.md): Live query synchronization and SSE query delta format.
- [Storage Migrations Reference](ref-storage-migrations.md): Schema management and data migrations.
