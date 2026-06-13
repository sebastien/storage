          
        _|_|_|  _|_|_|_|_|    _|_|    _|_|_|      _|_|      _|_|_|  _|_|_|_|  
      _|            _|      _|    _|  _|    _|  _|    _|  _|        _|        
        _|_|        _|      _|    _|  _|_|_|    _|_|_|_|  _|  _|_|  _|_|_|    
            _|      _|      _|    _|  _|    _|  _|    _|  _|    _|  _|        
      _|_|_|        _|        _|_|    _|    _|  _|    _|    _|_|_|  _|_|_|_|  
                                                                              
                                                                                                                                      

*Storage* is a Python module to store objects, blobs and metrics on a
local filesystem first, with the possibility of using specialised
backends if necessary.

It is designed to be simple, standalone and work for an individual-size
amount of data.

At its core is a composable backend abstraction — `StorageBackend` —
with in-memory, directory-per-object, DBM, SQLite, and journaling
implementations. On top sit three storage runtimes: `ObjectStorage`
for structured data with typed properties and relations, `RawStorage`
for binary blobs, and `MetricStorage` for append-only metrics. An
optional web layer (`storage.web`) exposes any storage class as a
REST API with real-time SSE push channels using the `extra`
microservice framework.

You can learn more about each component:

- **Core storage**: typed objects, blobs, metrics, and indexes — [reference](docs/ref-storage.md)
- **Web layer**: REST API, relations, custom methods, SSE channels — [reference](docs/ref-storage-web.md)
- **Blog example**: full application pattern with Accounts, Articles, Images — [`examples/blog.py`](examples/blog.py)

## In a nutshell

```python
from storage import Types, DirectoryBackend, Storable
from storage.objects import StoredObject, ObjectStorage
from storage.raw import StoredRaw, RawStorage
from storage.index import Indexing, Indexes

# 1. Define models
class Account(StoredObject):
	PROPERTIES = dict(
		email=Types.EMAIL,
		name=Types.STRING,
		roles=Types.LIST(Types.STRING),
	)
	INDEX_BY = dict(email=Indexing.Normalize)

class Article(StoredObject):
	PROPERTIES = dict(
		title=Types.STRING,
		status=Types.ENUM("draft", "published"),
	)
	RELATIONS = dict(author=Account)
	INDEX_BY = dict(
		status=Indexing.Normalize,
		keywords=lambda n, obj: Indexing.Keywords((obj.title,)),
	)

class File(StoredRaw):
	pass

# 2. Create storage
objects = ObjectStorage(DirectoryBackend("Data/")).use(Account, Article)
raw = RawStorage(DirectoryBackend("Data/")).use(File)
indexes = Indexes(DirectoryBackend, "Data/").use(Account, Article)

# 3. Save and query
account = Account(properties={"email": "alice@example.com", "name": "Alice"})
account.save()

article = Article(properties={"title": "Hello", "status": "draft"})
article.author = account
article.save()

# 4. Query by index
for art in indexes.Article.by.status("draft"):
	print(art.title)

# 5. Export
data = article.export(depth=2)  # deep: expanded relations
```

### Installation

```bash
# Add to PYTHONPATH and use directly (no build step)
export PYTHONPATH="$PYTHONPATH:src/py"

# Or link into site-packages
make prepare
```

No external PyPI dependencies are required. The web layer optionally
uses the [`extra`](deps/extra/) microservice framework (bundled).

### API

- `Storable`: abstract base class for all persistable objects.
- `Identifier`: sortable ID generation (`Stamp`, `ID`, `UUID`, `Timestamp`, `ParseNodeID`, `UpdateNodeID`).
- `Types`: type tag namespace (`STRING`, `EMAIL`, `LIST`, `MAP`, `ENUM`, `REFERENCE`, …).
- `StoredObject(Storable)`: structured object with typed properties, relations, revision tracking, and export/import.
- `StoredRaw(Storable)`: binary blob with separate metadata and streaming data access.
- `StoredMetric(Storable)`: append-only metric value with timestamp and metadata.
- `ObjectStorage`: runtime for `StoredObject` instances with weak-reference caching.
- `RawStorage`: runtime for `StoredRaw` instances with data/metadata key convention.
- `MetricStorage`: runtime for `StoredMetric` instances with per-file append storage.
- `Index`: single index mapping keys to object references.
- `IndexStorage`: bidirectional index (forward + backward) persistence.
- `Indexes`: registry managing multiple indexes with shortcut access (`indexes.ClassName.by.field`).
- `Indexing`: extractor functions (`Value`, `Normalize`, `NoAccents`, `Keyword`, `Keywords`, `UpdateTime`, `Paths`).
- `StorageBackend`: abstract key-value backend with capability flags.
- `MemoryBackend`: ephemeral dict-backed backend.
- `DirectoryBackend`: file-per-object filesystem backend.
- `DBMBackend`: `dbm.ndbm`-backed persistent key-value store.
- `SQLiteBackend`: SQLite-backed store with ordering and raw blob support.
- `JournalBackend`: transaction journal wrapper (replay, compaction, snapshots, pub/sub).
- `MultiBackend`: composite backend (writes to all, reads from first capable).
- `StorageServer`: REST API server with auto-generated CRUD, pagination, relations, and custom methods.
- `http`: decorator to expose classes and methods over HTTP.
- `Property`, `Relation`: value descriptors for transparent `.property` / `.relation` access.

### Modules

- [`docs/ref-storage.md`](docs/ref-storage.md): complete reference for object storage, raw storage, metrics, types, indexes, and backends.
- [`docs/ref-storage-web.md`](docs/ref-storage-web.md): REST API routing, `@http` decorator, SSE channels, commands endpoint, and JavaScript bridge.

### Notable examples

- [`examples/blog.py`](examples/blog.py): full application pattern with `Account`, `Article`, `Comment`, `File`, `Image`, `Video` models including typed properties, relations, indexing, and a unified `Interface` class composing `ObjectStorage`, `RawStorage`, `Indexes`, and `StorageServer`.

## Features

- *Local-first design*: default `DirectoryBackend` needs zero infrastructure.
- *Typed properties*: declare `PROPERTIES` with `Types.*` for validation and documentation.
- *First-class relations*: one-to-many and many-to-many with lazy loading and pagination.
- *Pluggable backends*: swap storage strategy without changing application code.
- *Composable backends*: `MultiBackend` (multiplex) and `JournalBackend` (journaling) wrap other backends.
- *Bidirectional indexing*: forward + backward index storage for efficient lookups.
- *REST API generation*: `@http` decorator auto-exposes classes as CRUD endpoints with paginated lists, relation mutations, and custom methods.
- *Real-time push*: SSE channels stream object changes via `JournalBackend`.
- *No external dependencies*: pure Python stdlib (optional `extra` for web layer).
- *Thread-safe*: `RLock`-protected backends and storage runtimes.
- *Weak-reference caching*: automatic object identity within a process without memory leaks.
- *Export/Import protocol*: depth-controlled serialization to/from primitive dicts.
- *Revision tracking*: per-property update timestamps for change detection and conflict resolution.
- *Sortable IDs*: timestamp-encoded, distributed ID generation with optional type prefixes.
