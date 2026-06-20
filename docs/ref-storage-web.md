# Web Storage Reference (`storage.web`)

## Overview

The `storage.web` module exposes storable classes (`StoredObject` and `StoredRaw`) over HTTP as a REST-like API. It integrates with the `extra` microservice framework to automatically handle requests, load data payloads, handle pagination, invoke custom domain methods, manage relations, and stream binary file data.

## Decorators

The module provides the `@http` decorator to mark storable classes and methods for web exposure.

### Class-Level Exposure

Decorate a `StoredObject` or `StoredRaw` class to expose it as a web service.

```python
from storage.web import http
from storage.objects import StoredObject

@http(url="items", export="default", restrict=None)
class WebItem(StoredObject):
	PROPERTIES = dict(
		name=Types.STRING,
		value=Types.STRING,
	)
```

**Parameters for `@http` at class level:**
*   `url` (str): The base URL segment under which this class is exposed (e.g., `items`). If omitted, the lowercase class name is used.
*   `restrict` (any): Metadata to control API access restrictions. Passed to the underlying `StorageDecoration`.
*   `methods` (str or list of str): Handled HTTP methods (defaults to `GET` and `POST`).
*   `export` (str or dict): Export options. If a string is provided, it is converted to `{"profile": export}`. These options are passed to `storable.export()` on serialization.

### Method-Level Exposure

Expose custom instance methods on a storable class.

```python
from storage.web import http
from storage.objects import StoredObject

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

**Parameters for `@http` at method level:**
*   `url` (str): The URL sub-segment to trigger this method (e.g., `/items/{id}/rename`).
*   `restrict` (any): Method-level access control metadata.
*   `methods` (str or list of str): Allowed HTTP methods (defaults to `("GET", "POST")`).
*   `contentType` (str or callable): The response media type. If a callable is passed, it receives the storable instance as an argument.

---

## The `StorageServer` Service

The `StorageServer` acts as an `extra.Service` router that publishes endpoints for registered storable classes.

Read endpoints can be served as JSON, Markdown, or XML. JSON remains the default representation; `.md` and `.xml` suffixes expose the same read payloads as `text/markdown` and `application/xml` respectively.

### Initialization

```python
from storage.web import StorageServer

server = StorageServer(prefix="/api", classes=[WebItem], readonly=False)
```

**Parameters:**
*   `prefix` (str): The HTTP URL prefix for all registered resources. Defaults to `"/api"`.
*   `classes` (iterable): List of decorated storable classes to register immediately.
*   `readonly` (bool): If set to `True`, all write operations (`create`, `update`, `remove`) will immediately return `403 Forbidden` (`notAuthorized`).

### Registering Classes Dynamically

Register classes after initialization:

```python
server.use(AnotherStorableClass)
# or
server.add(AnotherStorableClass)
```

### Event Callbacks

`StorageServer` does not invoke a zero-argument callback. Updates are delivered as SSE events from a storage channel.

Create a channel, subscribe to a target, then read events from `channel/{id}/events`:

```json
{
	"id": "...",
	"events": "channel/.../events",
	"commands": "channel/.../commands",
	"heartbeat": "channel/.../heartbeat",
	"close": "channel/.../close"
}
```

Subscribe with a channel command:

```json
{
	"commands": [
		{
			"op": "subscribe",
			"target": {
				"kind": "object",
				"type": "items",
				"id": "123"
			}
		}
	]
}
```

Whole-channel buffering is controlled through the same endpoint:

```json
{
	"commands": [
		{"op": "block"},
		{"op": "flush"},
		{"op": "unblock"}
	]
}
```

`block` starts buffering notifications for the whole channel. `flush` emits the buffered changes as a single `batch` SSE event and keeps the channel blocked. `unblock` emits any remaining buffered changes as one `batch` event and resumes immediate delivery.

Each event payload includes the data needed to see what changed:

```json
{
	"event": "update",
	"seq": 12,
	"operation": "+",
	"key": "items:123",
	"type": "items",
	"id": "123",
	"revision": {"...": "..."},
	"patch": [
		{"op": "replace", "path": "/name", "value": "New name"}
	],
	"relations": {
		"tags": {
			"added": [{"id": "7", "type": "Tag"}],
			"removed": [{"id": "2", "type": "Tag"}]
		}
	},
	"entry": {"...": "..."},
	"target": {"kind": "object", "type": "items", "id": "123"},
	"value": {"id": "123", "type": "items"}
}
```

Transactional command batches and blocked channels emit a `batch` event instead of individual `create`/`update`/`remove` events:

```json
{
	"count": 2,
	"changed": ["items:123"],
	"from": 12,
	"to": 13,
	"events": [
		{"event": "update", "id": "123", "type": "items"},
		{"event": "update", "id": "123", "type": "items"}
	]
}
```

Field meanings:

* `event`: `create`, `update`, `remove`, or `batch`
* `operation`: journal operation from `Operation` (`=`, `+`, `-`, `+R`)
* `patch`: JSON-patch-like field changes (`add`, `remove`, `replace`)
* `relations`: relation diffs with `added` and `removed` object references
* `value`: current object value when the backend can still fetch it

### Command Endpoint

`POST /api/commands` accepts mutating REST operations in command form:

```json
{
	"transaction": true,
	"commands": [
		{"op": "create", "type": "items", "fields": {"value": "alpha"}},
		{"op": "update", "type": "items", "id": "123", "fields": {"value": "beta"}},
		{"op": "remove", "type": "items", "id": "123"},
		{"op": "relation.append", "type": "items", "id": "123", "relation": "tags", "values": [{"id": "7"}]},
		{"op": "invoke", "type": "items", "id": "123", "method": "rename", "body": {"value": "gamma"}}
	]
}
```

Supported command operations mirror the mutating REST API:

* `create`
* `update`
* `remove`
* `relation.set`, `relation.append`, `relation.prepend`, `relation.insert`, `relation.delete`, `relation.remove`, `relation.swap`, `relation.move`, `relation.clear`
* `invoke` for custom `POST` methods exposed through `@http(...)`

When `transaction` is `true`, update notifications are deferred until the request completes and then delivered as a single `batch` event per subscribed channel.

---

## REST API Endpoint Routing

When a storable class `WebItem` is decorated with `@http("items")` and registered to a `StorageServer` with prefix `/api`, the following routes are automatically set up:

| Method | Route Pattern | Description |
| :--- | :--- | :--- |
| `GET`/`POST` | `/api/items` | [Create Object](#1-create-object) |
| `POST` | `/api/items/{id}` | [Update Object](#2-update-object) |
| `GET` | `/api/items/{id}` | [Retrieve Object](#3-retrieve-object) |
| `GET` | `/api/items/{id}.md` | [Retrieve Object as Markdown](#3-retrieve-object) |
| `GET` | `/api/items/{id}.xml` | [Retrieve Object as XML](#3-retrieve-object) |
| `POST` | `/api/items/{id}/remove` | [Delete Object](#4-delete-object) |
| `GET` | `/api/items/list` | [List/Paginate Objects](#5-list--pagination) |
| `GET` | `/api/items/list.md` | [List/Paginate Objects as Markdown](#5-list--pagination) |
| `GET` | `/api/items/list.xml` | [List/Paginate Objects as XML](#5-list--pagination) |
| `GET`/`POST` | `/api/items/{id}/{custom_method}` | [Invoke Custom Method](#6-custom-method-invocation) |
| `GET` | `/api/items/{id}/{custom_method}.md` | [Invoke Custom Method as Markdown](#6-custom-method-invocation) *(GET only, when no custom `contentType` is set)* |
| `GET` | `/api/items/{id}/{custom_method}.xml` | [Invoke Custom Method as XML](#6-custom-method-invocation) *(GET only, when no custom `contentType` is set)* |
| `GET` | `/api/items/{id}/relations` | [List Relations](#8-relation-listing) |
| `GET` | `/api/items/{id}/relations.md` | [List Relations as Markdown](#8-relation-listing) |
| `GET` | `/api/items/{id}/relations.xml` | [List Relations as XML](#8-relation-listing) |
| `GET` | `/api/items/{id}/relations/{name}/count` | [Relation Count](#9-relation-count) |
| `GET` | `/api/items/{id}/relations/{name}/count.md` | [Relation Count as Markdown](#9-relation-count) |
| `GET` | `/api/items/{id}/relations/{name}/count.xml` | [Relation Count as XML](#9-relation-count) |
| `GET` | `/api/items/{id}/relations/{name}/list` | [Relation List/Pagination](#10-relation-pagination-and-listing) |
| `GET` | `/api/items/{id}/relations/{name}/list.md` | [Relation List/Pagination as Markdown](#10-relation-pagination-and-listing) |
| `GET` | `/api/items/{id}/relations/{name}/list.xml` | [Relation List/Pagination as XML](#10-relation-pagination-and-listing) |
| `GET` | `/api/items/{id}/relations/{name}/list/{start}:{end}` | [Relation List/Pagination](#10-relation-pagination-and-listing) |
| `GET` | `/api/items/{id}/relations/{name}/list/{start}:{end}.md` | [Relation List/Pagination as Markdown](#10-relation-pagination-and-listing) |
| `GET` | `/api/items/{id}/relations/{name}/list/{start}:{end}.xml` | [Relation List/Pagination as XML](#10-relation-pagination-and-listing) |
| `POST` | `/api/items/{id}/relations/{name}/{operation}` | [Relation Mutation](#11-relation-mutation) |
| `GET` | `/api/blobs/{id}/data` | [Retrieve Raw File Data](#7-raw-file-data-retrieval) *(StoredRaw only)* |

---

### Detailed Endpoint Behavior

#### 1. Create Object
*   **Endpoints:** `GET /api/items`, `POST /api/items`
*   **Request Body:** Optional JSON or form data payload representing the properties of the object to create.
*   **Behavior:** Imports properties, calls `.save()`, and returns the serialized JSON representation of the new object. If no data is provided, returns an empty storable.

#### 2. Update Object
*   **Endpoints:** `POST /api/items/{id}`
*   **Request Body:** JSON or URL-encoded form data of properties to update.
*   **Behavior:** Loads the existing storable instance by `{id}`. If not found, imports the data as a new instance and saves. Otherwise, updates properties and saves.
*   **Returns:** JSON representation of the updated object.

#### 3. Retrieve Object
*   **Endpoints:** `GET /api/items/{id}`
    *   Markdown: `GET /api/items/{id}.md`
    *   XML: `GET /api/items/{id}.xml`
*   **Query Parameters:**
    *   `strict`: If `strict=1` or `strict` is present, returns `404 Not Found` if the object does not exist. Otherwise, a temporary empty instance with ID `{id}` is returned.
*   **Returns:** JSON by default; Markdown and XML suffixes return the same payload rendered as `text/markdown` or `application/xml`.

#### 4. Delete Object
*   **Endpoints:** `POST /api/items/{id}/remove`
*   **Behavior:** Deletes the storable by ID.
*   **Returns:** `true` on success. Returns `404 Not Found` if the object does not exist.

#### 5. List / Pagination
Allows querying lists of objects.
*   **Endpoints:**
    *   `GET /api/items/list` (Uses default count of `20`)
    *   `GET /api/items/list.md`
    *   `GET /api/items/list.xml`
    *   `GET /api/items/list/{start}`
    *   `GET /api/items/list/{start}:`
    *   `GET /api/items/list/{start}:{end}`
    *   `GET /api/items/list/{start}.md`
    *   `GET /api/items/list/{start}.xml`
    *   `GET /api/items/list/{start}:{end}.md`
    *   `GET /api/items/list/{start}:{end}.xml`
*   **Returns:** JSON by default; Markdown and XML suffixes render the same envelope.
    ```json
    {
	    "start": 0,
    	"end": 20,
    	"count": 1,
    	"values": [...]
    }
    ```

#### 6. Custom Method Invocation
Dynamically invokes custom instance methods decorated with `@http`.
*   **Endpoints:** `GET /api/items/{id}/{method_name}`, `POST /api/items/{id}/{method_name}`
    *   Markdown: `GET /api/items/{id}/{method_name}.md`
    *   XML: `GET /api/items/{id}/{method_name}.xml`
*   **Parameters Mapping:**
    *   For **`POST`** requests, if the payload is a list, it is passed as positional `*args`. If it is a dictionary, it is combined with query parameters and passed as `**kwargs`.
    *   For **`GET`** requests, URL query parameters are passed as `**kwargs`.
*   **Type Restoration:** Parameter values are automatically passed to `restore()` to map primitives back to database-managed objects or storage structures.
*   **Returns:** The method's return value serialized as JSON by default, or rendered as Markdown/XML for GET requests without a custom `contentType`.

#### 7. Raw File Data Retrieval
Only available on subclasses of `StoredRaw`.
*   **Endpoints:** `GET /api/blobs/{id}/data`
*   **Behavior:** Streams the raw binary file content from the backend path.
*   **Content-Type Header:** Determined from metadata (uses `contentType` or `mimeType` in metadata, falling back to `application/x-binary`).

#### 8. Relation Listing
*   **Endpoints:** `GET /api/items/{id}/relations`
    *   Markdown: `GET /api/items/{id}/relations.md`
    *   XML: `GET /api/items/{id}/relations.xml`
*   **Behavior:** Returns the relation map for the object.

#### 9. Relation Count
*   **Endpoints:** `GET /api/items/{id}/relations/{name}/count`
    *   Markdown: `GET /api/items/{id}/relations/{name}/count.md`
    *   XML: `GET /api/items/{id}/relations/{name}/count.xml`
*   **Behavior:** Returns the number of values in a relation.

#### 10. Relation Pagination and Listing
*   **Endpoints:** `GET /api/items/{id}/relations/{name}/list`, `GET /api/items/{id}/relations/{name}/list/{start}:{end}`
    *   Markdown: `.md` suffix on each GET variant
    *   XML: `.xml` suffix on each GET variant
*   **Query Parameters:**
    *   `resolve`, `depth`, `start`, `end`, `count`, `return`
*   **Behavior:** Returns relation counts or paginated relation values, formatted as JSON by default or Markdown/XML with suffixed GET routes.

#### 11. Relation Mutation
*   **Endpoints:** `POST /api/items/{id}/relations/{name}/{operation}`
*   **Operations:** `set`, `append`, `prepend`, `insert`, `delete`, `remove`, `swap`, `move`, `clear`
*   **Behavior:** Applies a relation update and returns the updated relation payload.

---

## Integration and Hosting

The `StorageServer` acts as an `extra.routing.Service` and can be mounted directly onto an `extra.model.Application`.

```python
from extra.model import Application
from storage.web import StorageServer

server = StorageServer(prefix="/api", classes=[WebItem])

app = Application()
app.mount(server)

# Start application server
await app.start()
```

---

## JavaScript Bridge (`src/js/storage/object.js`)

The JavaScript bridge mirrors the web API for browser and client-side usage. It exports `bridge()`, `StoredObjectBridge`, `StoredObject`, `StoredRelation`, `StoredQuery`, `StoredType`, and `StorageBridgeError`.

### Bridge Entry Point

```js
import bridge from "storage/object.js"

const storage = bridge({ path: "/api" })
```

`bridge()` returns a singleton `StoredObjectBridge` and reconfigures it when called again with new options.

### StoredObject

`StoredObject` represents a remote object instance.

Key methods:
* `get(name)`, `set(name, value)`, `update(fields)`
* `pull(options)`, `push()`, `pushChanges(changes)`, `remove()`
* `call(name, data, options)` for custom methods
* `relation(name)` to create a relation handle
* `relations()` to fetch all relations for the object

### StoredRelation

`StoredRelation` wraps relation endpoints for a specific object and relation name.

Key methods:
* `count()`
* `page(options)`, `list(options)`, `ilist(options)`, `all(options)`
* `set(values, options)`
* `append(values, options)`, `prepend(values, options)`, `insert(index, values, options)`
* `delete(indexOrRange, options)`, `remove(values, options)`
* `swap(a, b, options)`, `move(fromOrRange, to, options)`, `clear(options)`

### StoredQuery

`StoredQuery` wraps owner-scoped live query subscriptions.

Key methods:
* `values()`
* `sub(callback)`, `unsub(callback)`
* `sync(options)` to subscribe live and wait for the initial snapshot

Create queries through either the bridge or a stored type:

```js
const members = storage.query("members", { owner: "user-123" })
// or
const sameMembers = storage.type("members").query({ owner: "user-123" })

await members.sync()
console.log(members.values())
```

### StoredObjectBridge

`StoredObjectBridge` provides the transport layer and object cache.

Relevant methods:
* `type(name)`, `ref(type, id, data)`, `object(type, id, data)`
* `query(type, options)`
* `get(type, id, options)`, `create(type, fields)`
* `page(type, options)`, `list(type, options)`, `ilist(type, options)`
* `relations(type, id)`
* `relationCount(type, id, name)`
* `relationPage(type, id, name, options)`
* `relationList(type, id, name, options)`
* `relationOperation(type, id, name, operation, body, options)`

The bridge also supports batched updates and live subscriptions for objects, relations, and owner-scoped queries.
