# Web Storage Reference (`storage.web`)

## Overview

The `storage.web` module exposes storable classes (`StoredObject` and `StoredRaw`) over HTTP as a REST-like JSON API. It integrates with the `extra` microservice framework to automatically handle requests, load data payloads, handle pagination, invoke custom domain methods, and stream binary file data.

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

`StorageServer` allows subscribing to storage updates.

```python
def handle_updates():
	print("Data has been updated!")

# Subscribe to update events
server.onUpdate(handle_updates)

# Unsubscribe
server.offUpdate(handle_updates)
```

---

## REST API Endpoint Routing

When a storable class `WebItem` is decorated with `@http("items")` and registered to a `StorageServer` with prefix `/api`, the following routes are automatically set up:

| Method | Route Pattern | Description |
| :--- | :--- | :--- |
| `GET`/`POST` | `/api/items` | [Create Object](#1-create-object) |
| `POST` | `/api/items/{id}` | [Update Object](#2-update-object) |
| `GET` | `/api/items/{id}` | [Retrieve Object](#3-retrieve-object) |
| `POST` | `/api/items/{id}/remove` | [Delete Object](#4-delete-object) |
| `GET` | `/api/items/list` | [List/Paginate Objects](#5-list--pagination) |
| `GET`/`POST` | `/api/items/{id}/{custom_method}` | [Invoke Custom Method](#6-custom-method-invocation) |
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
*   **Query Parameters:**
    *   `strict`: If `strict=1` or `strict` is present, returns `404 Not Found` if the object does not exist. Otherwise, a temporary empty instance with ID `{id}` is returned.
*   **Returns:** JSON representation of the object.

#### 4. Delete Object
*   **Endpoints:** `POST /api/items/{id}/remove`
*   **Behavior:** Deletes the storable by ID.
*   **Returns:** `true` on success. Returns `404 Not Found` if the object does not exist.

#### 5. List / Pagination
Allows querying lists of objects.
*   **Endpoints:**
    *   `GET /api/items/list` (Uses default count of `20`)
    *   `GET /api/items/list/{start}`
    *   `GET /api/items/list/{start}:`
    *   `GET /api/items/list/{start}:{end}`
*   **Returns:** A JSON envelope:
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
*   **Parameters Mapping:**
    *   For **`POST`** requests, if the payload is a list, it is passed as positional `*args`. If it is a dictionary, it is combined with query parameters and passed as `**kwargs`.
    *   For **`GET`** requests, URL query parameters are passed as `**kwargs`.
*   **Type Restoration:** Parameter values are automatically passed to `restore()` to map primitives back to database-managed objects or storage structures.
*   **Returns:** The method's return value serialized as JSON (unless a custom `contentType` is configured).

#### 7. Raw File Data Retrieval
Only available on subclasses of `StoredRaw`.
*   **Endpoints:** `GET /api/blobs/{id}/data`
*   **Behavior:** Streams the raw binary file content from the backend path.
*   **Content-Type Header:** Determined from metadata (uses `contentType` or `mimeType` in metadata, falling back to `application/x-binary`).

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
