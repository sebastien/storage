# Storage Module Reference (`storage`)

The `storage` module provides a composable, local-first persistent object storage system for Python applications. It supports structured data models (`StoredObject`), raw binary payloads (`StoredRaw`), append-only metrics (`StoredMetric`), generic key-value stores (`KVStorage`), type-safe properties, bidirectional relations, indexing, schema evolution, and multiple backend implementations.

---

## Documentation Navigation

| Document | Focus Area |
| :--- | :--- |
| **`ref-storage.md`** *(this document)* | Core abstractions, `StoredObject`, `StoredRaw`, `StoredMetric`, `KVStorage`, `Types`, `Indexing`, `Backends`, and `Schema`. |
| **[`ref-storage-web.md`](ref-storage-web.md)** | HTTP REST API routing, `@http` decorators, SSE push channels, batch command endpoints, and JavaScript bridge. |
| **[`ref-storage-queries.md`](ref-storage-queries.md)** | `StoredQuery` live queries, owner-scoped synchronization, and SSE query delta streaming. |
| **[`ref-storage-migrations.md`](ref-storage-migrations.md)** | `MigrationOperator`, `@migration` context runner, checkpointing, and resumable data migrations. |

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────┐
│                           Application Layer                             │
├─────────────────┬─────────────────┬─────────────────┬───────────────────┤
│  StoredObject   │    StoredRaw    │  StoredMetric   │     KVStorage     │
│ (Structured DB) │  (Files/Blobs)  │  (Time-Series)  │ (Key-Value/Cache) │
├─────────────────┴─────────────────┴─────────────────┴───────────────────┤
│                    Indexes / Query / Migrations / Web                   │
├─────────────────────────────────────────────────────────────────────────┤
│                             StorageBackend                              │
│   ┌───────────────┬─────────────────┬──────────────┬────────────────┐   │
│   │ MemoryBackend │DirectoryBackend │  DBMBackend  │ SQLiteBackend  │   │
│   └───────────────┴─────────────────┴──────────────┴────────────────┘   │
│                   │ JournalBackend (Transactions / SSE) │               │
└───────────────────┴─────────────────────────────────────┴───────────────┘
```

---

## Core Abstractions

### 1. `Storable` & `Identifier`

All persistable classes inherit from `Storable`. `Identifier` provides distributed, sortable unique ID generation:

```python
from storage import Identifier

# Generate sortable 64-bit integer timestamp stamp
stamp = Identifier.Stamp()

# Generate UUID4 string
uid = Identifier.UUID()
```

### 2. Primitive Serialization Protocol

* `asPrimitive(value, depth=1)`: Converts stored objects, dates, lists, and dicts into storage-friendly primitive dictionaries.
* `restore(value)`: Reconstitutes primitive dictionaries with `{"type": "...", "id": "..."}` back into hydrated `StoredObject` instances.
* `asJSON(value)` / `unJSON(text)`: JSON encoding with automatic restoration.

---

## Structured Data Storage (`StoredObject` & `ObjectStorage`)

`StoredObject` provides structured object modeling with typed properties, relations, revision tracking, and weak-reference caching.

### Defining Models

```python
from storage import DirectoryBackend, Indexing, Ownership, PublicID, Types
from storage.objects import ObjectStorage, StoredObject

class User(StoredObject):
	ID_PREFIX = "USER"
	PROPERTIES = dict(
		name=Types.STRING,
		email=Types.EMAIL,
		roles=Types.LIST(Types.STRING),
	)
	INDEX_BY = dict(
		email=Indexing.Normalize,
		name=Indexing.Normalize,
	)

class Project(StoredObject):
	ID_PREFIX = "PRJ"
	PROPERTIES = dict(title=Types.STRING)

class Ticket(StoredObject):
	ID_PREFIX = "TCK"
	# Opt-in to partition-scoped sequential human IDs (#1, #2, ...)
	PUBLIC_ID = PublicID.Partition
	PUBLIC_ID_NAMESPACE = "tickets"
	
	# Scoped ownership
	OWNERSHIP = Project.Owns(required=True, cascade=False)
	
	PROPERTIES = dict(
		title=Types.STRING,
		status=Types.ENUM("open", "in_progress", "closed"),
	)
	RELATIONS = dict(
		assignee=User,
	)
```

### Initializing Storage

```python
# Bind models to a storage runtime
backend = DirectoryBackend("Data/")
storage = ObjectStorage(backend).use(User, Project, Ticket)
```

### CRUD Operations

```python
# 1. Create and save
user = User(name="Alice", email="alice@example.com", roles=["admin"])
user.save()

project = Project(title="Core Platform").save()

# 2. Create owned object with partition
ticket = Ticket(
	title="Implement caching",
	status="open",
	partition=project.id,
	owner=project,
)
ticket.assignee = user
ticket.save()

# 3. Retrieve by ID
fetched = User.Get(user.id)
assert fetched is user  # Same in-memory instance via weak-ref cache

# 4. List and count
total_users = User.Count()
for u in User.All():
	print(u.name, u.email)

# 5. Partition-scoped public ID lookup
ticket_one = Ticket.GetByPublicID(1, partition=project.id)

# 6. Delete
ticket.remove()
```

### Class Methods

* `Model.Get(id)`: Retrieve an object by ID.
* `Model.Has(id)`: Check if an object exists.
* `Model.All(order=1)`: Iterator over all objects of this class.
* `Model.Count()`: Total count of objects.
* `Model.List(count=20, start=0, end=None)`: Paginated iterator.
* `Model.OwnedBy(owner)`: Iterator of objects owned by `owner`.
* `Model.GetByPublicID(publicId, partition)`: Retrieve object by human-facing sequence number.
* `Model.Ensure(id)`: Retrieve existing or instantiate transient object with `id`.
* `Model.Import(properties)`: Restore instance from dictionary export.

### Instance Methods

* `obj.save()`: Persist changes to storage.
* `obj.remove()`: Delete from storage and remove from cache.
* `obj.export(depth=1)`: Serialize to primitive dictionary (depth 0=id/type, 1=properties+refs, 2=expanded relations).
* `obj.update(dict)`: Update multiple properties at once.
* `obj.getUpdateTime(property="id")`: Get timestamp of last modification.
* `obj.hasOwner()` / `obj.getOwner()`: Access object owner.

---

## Relations

Relations represent managed links between `StoredObject` instances with lazy loading.

```python
class Comment(StoredObject):
	PROPERTIES = dict(body=Types.STRING)

class Article(StoredObject):
	PROPERTIES = dict(title=Types.STRING)
	RELATIONS = dict(
		author=User,         # Single relation
		comments=[Comment],  # Many relation (list syntax)
	)
```

### Working with Relations

```python
article = Article(title="Hello World").save()
comment = Comment(body="Great read!").save()

# Single relation
article.author = alice
article.save()
print(article.author.one().name)

# Many relation
article.comments.add(comment)
article.comments.append(another_comment)

# Iteration & querying
for c in article.comments:
	print(c.body)

# Membership & counts
print("Total comments:", len(article.comments))
if article.comments.has(comment):
	print("Comment found")

# Removal
article.comments.remove(comment)
article.comments.clear()
```

### Relation API

* `.add(obj)` / `.append(obj)`: Append object reference.
* `.prepend(obj)` / `.insert(index, obj)`: Insert object reference.
* `.remove(obj)`: Remove specific object.
* `.delete(index)`: Remove item by position.
* `.clear()`: Clear all related items.
* `.one(index=0)`: Resolve and return a single related object.
* `.get(start=0, limit=None, resolve=True)`: Paginated resolution.
* `.list()` / `.all()`: Iterator over all related instances.
* `.has(obj_or_id)`: Check membership.
* `len(relation)`: Count related items.

### Inverse relations

`InverseRelation` is a parent-side view over a child foreign key. It is not stored on the parent. Lookup uses `INDEX_BY` on the child. HTTP `GET /api/{type}/{id}/relations/{name}` lists it. POST membership ops (`set`, `append`, `remove`, `clear`) write the child foreign key and do not save the parent. Ordered ops (`swap`, `move`, `prepend`, `insert`, `delete`) return `BADOP`.

```python
from storage.index import Indexes, Indexing
from storage.objects import InverseRelation, StoredObject

class Comment(StoredObject):
	PROPERTIES = dict(articleId=Types.STRING, body=Types.STRING)
	INDEX_BY = dict(articleId=Indexing.Value)

class Article(StoredObject):
	PROPERTIES = dict(title=Types.STRING)
	RELATIONS = lambda _: dict(
		comments=InverseRelation(Comment, "articleId"),
	)

Indexes(DirectoryBackend, "Data/").use(Article, Comment)
article = Article(title="Hello").save()
Comment(articleId=article.id, body="Nice").save()
list(article.comments)  # the comment
# GET /api/article/{article.id}/relations/comments
```

`article.comments.add(comment)` and `article.comments = [comment]` set `comment.articleId` and save the comment. HTTP `GET` with `target="web"` embeds the view; disk export does not.

---

## Binary & File Storage (`StoredRaw` & `RawStorage`)

`StoredRaw` stores binary payloads (images, documents, archives) with metadata dictionaries and data streaming.

```python
from storage import DirectoryBackend
from storage.raw import RawStorage, StoredRaw

class Document(StoredRaw):
	ID_PREFIX = "DOC"

raw_storage = RawStorage(DirectoryBackend("Data/")).use(Document)

# Save file with metadata
with open("report.pdf", "rb") as f:
	data = f.read()

doc = Document(data=data, filename="report.pdf", mimeType="application/pdf")
doc.save()

# Retrieve and stream data
loaded = Document.Get(doc.id)
print("Filename:", loaded.meta("filename"))

for chunk in loaded.data(size=64 * 1024):
	process_chunk(chunk)

# Filesystem path (if supported by backend)
local_path = loaded.path()
```

### Methods

* `doc.meta(name=None, value=None)`: Get or set metadata properties.
* `doc.setMeta(**kwargs)`: Update multiple metadata fields.
* `doc.data(size=None)`: Stream binary chunks without loading entire file into memory.
* `doc.loadData()`: Load complete byte array.
* `doc.path()`: Direct filesystem path (available on `DirectoryBackend`).

---

## Monotone Metric Storage (`StoredMetric` & `MetricStorage`)

`StoredMetric` provides append-only time-series metric storage.

```python
from storage.metrics import MetricStorage, MetricsDirectoryBackend, StoredMetric

metric_storage = MetricStorage(MetricsDirectoryBackend("Data/metrics"))

# Record metrics
metric_storage.add(StoredMetric("api.requests", 1, meta={"route": "/items"}))
metric_storage.add(StoredMetric("cpu.load", 0.42, timestamp=1710000000))

# Query by time range
for sample in metric_storage.get("cpu.load", after=1709990000, before=1710010000):
	print(sample.timestamp, sample.value)
```

---

## Key-Value Storage (`KVStorage`)

`KVStorage` provides a typed, prefixed key-value store with pluggable codecs and normalizers.

```python
from storage.backends.sqlite import KVSqliteBackend
from storage.formats import JSONCodec
from storage.kv import KVStorage, PathKVKeyNormalizer, StringKVKeyNormalizer

# String-keyed KV store with JSON codec
kv = KVStorage(
	KVSqliteBackend("Data/kv.sqlite3"),
	prefix="settings:",
	normalizer=StringKVKeyNormalizer(),
	codec=JSONCodec(),
)

# Single operations
kv.set("theme", {"dark": True, "fontSize": 14})
settings = kv.get("theme")
has_theme = kv.has("theme")
kv.delete("theme")

# Batch operations
kv.setm({"a": 1, "b": 2})
results = kv.getm(["a", "b"])

# Key iteration
for key in kv.ilist(prefix="user:"):
	print(key)

# Key-Value pair iteration
for key, value in kv.iitems():
	print(key, value)
```

### Normalizers

* `StringKVKeyNormalizer`: Plain string keys (`"user:123"`).
* `PathKVKeyNormalizer`: Path components (`["data", "2025", "report.json"]`).
* `TupleKVKeyNormalizer`: Tuple keys (`("tenant", "user", "id")`).

---

## Type System (`Types`)

| Category | Type Tag | Description |
| :--- | :--- | :--- |
| **Primitives** | `Types.BOOL` | Boolean value |
| | `Types.INTEGER` / `Types.POSITIVE` | Signed integer / integer > 0 |
| | `Types.FLOAT` / `Types.NUMBER` | Floating-point / general number |
| | `Types.DATE` / `Types.TIME` / `Types.DATETIME` | Time values |
| | `Types.BINARY` | Raw bytes |
| | `Types.ANY` / `Types.THIS` | Any primitive / Self reference |
| **Strings** | `Types.STRING` / `Types.LINE` | General string / single-line string |
| | `Types.EMAIL` / `Types.PASSWORD` / `Types.URL` | Validated string subtypes |
| | `Types.HTML` / `Types.MARKDOWN` / `Types.RICHTEXT`| Formatted text |
| | `Types.PATH` / `Types.ID` | Path / Identifier string |
| **Composites** | `Types.LIST(type)` | Homogeneous list, e.g. `Types.LIST(Types.STRING)` |
| | `Types.TUPLE(*types)` | Fixed tuple, e.g. `Types.TUPLE(Types.INT, Types.STRING)` |
| | `Types.ONE_OF(*types)` | Union type, e.g. `Types.ONE_OF(Types.INT, Types.STRING)` |
| | `Types.MAP(**spec)` | Keyed map, e.g. `Types.MAP(lat=Types.FLOAT, lng=Types.FLOAT)` |
| | `Types.ENUM(*values)` | Enumeration, e.g. `Types.ENUM("open", "closed")` |
| | `Types.REFERENCE(Model)` | StoredObject reference |
| | `Types.RANGE(min, max, type)` | Value bounds, e.g. `Types.RANGE(0, 100)` |

---

## Indexing (`Indexing` & `Indexes`)

Indexes enable fast object lookups by property value.

```python
from storage.backends.sqlite import SQLiteBackend
from storage.index import Indexes, Indexing

class Article(StoredObject):
	PROPERTIES = dict(
		title=Types.STRING,
		category=Types.STRING,
		tags=Types.LIST(Types.STRING),
	)
	INDEX_BY = dict(
		category=Indexing.Normalize,
		tags=lambda name, obj: Indexing.Keywords(obj.tags),
	)

# Register indexes with backend
indexes = Indexes(SQLiteBackend, "Data/indexes").use(Article)

# Query by index
for article in indexes.Article.by.category("engineering"):
	print(article.title)

# Get single match
first = indexes.Article.by.category.one("engineering")

# Rebuild indexes
indexes.rebuild(sync=True)
```

### Extraction Functions

* `Indexing.Value`: Pass-through exact value.
* `Indexing.Normalize`: Lowercase, strip accents and normalize whitespace.
* `Indexing.NoAccents`: Strip diacritics / accents.
* `Indexing.Keyword` / `Indexing.Keywords(values)`: Tokenize and extract keywords.
* `Indexing.Paths(separator="/")`: Index hierarchical subpaths.
* `Indexing.UpdateTime`: Index modification timestamps.

---

## Storage Backends

| Backend | Capabilities | Best Suited For |
| :--- | :--- | :--- |
| **`MemoryBackend`** / `KVMemoryBackend` | In-memory, fast, ephemeral | Unit tests, mock environments |
| **`DirectoryBackend`** / `KVFileBackend` | Direct file-per-object, path access | Development, simple file storage |
| **`DBMBackend`** | Key-value store via `dbm.ndbm` | Embedded single-file KV lookups |
| **`SQLiteBackend`** / `KVSqliteBackend` | WAL mode, ordering, raw blobs, sequence tables | Production local-first, multi-process |
| **`JournalBackend`** | Change log, snapshots, SSE pub/sub | Live sync, event sourcing, transaction replay |
| **`MultiBackend`** | Multiplexes reads/writes across backends | Tiered storage, replication |

---

## Schema Evolution & Validation

`storage.schema` automatically validates stored schemas on startup and triggers migrations when needed.

```python
from storage.objects import ObjectStorage
from storage.schema import SchemaValidator

# Validates schema and auto-runs pending migrations if covered
storage = ObjectStorage(backend, validateSchema=True).use(User, Article)
```

See [Storage Migrations Reference](ref-storage-migrations.md) for declarative migration authoring.

---

## Complete Application Pattern

```python
from storage import DirectoryBackend, Indexing, SQLiteBackend, Types
from storage.index import Indexes
from storage.kv import KVStorage, StringKVKeyNormalizer
from storage.formats import JSONCodec
from storage.migrations import MigrationOperator
from storage.objects import ObjectStorage, StoredObject
from storage.raw import RawStorage, StoredRaw
from storage.web import StorageServer, http

# 1. Models
@http("accounts")
class Account(StoredObject):
	PROPERTIES = dict(email=Types.EMAIL, name=Types.STRING)
	INDEX_BY = dict(email=Indexing.Normalize)

@http("documents")
class Document(StoredRaw):
	pass

# 2. Unified Application Data Interface
class AppData:
	def __init__(self, data_dir="Data"):
		self.backend = SQLiteBackend(f"{data_dir}/db")
		
		# Object & Raw Storage
		self.objects = ObjectStorage(self.backend).use(Account)
		self.raw = RawStorage(self.backend).use(Document)
		
		# Key-Value Cache
		self.cache = KVStorage(
			self.backend,
			prefix="cache:",
			normalizer=StringKVKeyNormalizer(),
			codec=JSONCodec(),
		)
		
		# Indexes
		self.indexes = Indexes(SQLiteBackend, f"{data_dir}/idx").use(Account)
		
		# Web Server
		self.server = StorageServer(prefix="/api", classes=[Account, Document])
		self.server.useKV("cache", self.cache)

	def migrate(self):
		return MigrationOperator(self.objects).apply()

	def sync(self):
		self.backend.sync()
```

---

## Best Practices & Guidelines

1. **Explicit Registration**: Always register models with `.use(...)` before calling class methods like `.Get()` or `.All()`.
2. **Explicit Persistence**: Modifications are not automatically saved unless using the `with storage:` context manager. Call `.save()` after changing properties.
3. **Use Descriptors**: Declare properties in `PROPERTIES` and access them directly as attributes (`account.email`).
4. **Shallow vs Deep Exports**: Use `export(depth=1)` for shallow reference exports and `export(depth=2)` to serialize related objects.
5. **Reserved Property Names**: Do not use `type`, `id`, `owner`, `partition`, `revision`, or `updates` as model property names.
6. **Thread Safety**: Storage backends and runtimes use `threading.RLock` and are safe for concurrent multithreaded access.
