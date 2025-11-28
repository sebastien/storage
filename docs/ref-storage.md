# Storage Module Reference for Agents

## Overview

The storage module provides a composable, persistent object storage system for Python applications. It supports structured objects (`StoredObject`), raw binary data (`StoredRaw`), type-safe properties, relations, and indexing across multiple backend implementations (Memory, Directory, DBM).

## Core Concepts

### 1. StoredObject (Structured Data)

**Purpose**: Store structured data with typed properties and relations between objects.

**Key Features**:
- Type-safe properties using `PROPERTIES` declaration
- Relations to other `StoredObject` instances via `RELATIONS`
- Automatic OID (Object ID) generation
- Built-in caching and weak-reference management
- Indexing support via `INDEX_BY`
- Export/Import to/from primitive dictionaries

**Basic Usage**:

```python
from storage import Types, DirectoryBackend, StoredObject
from storage.objects import ObjectStorage

# Define a model
class Account(StoredObject):
	PROPERTIES = dict(
		email=Types.EMAIL,
		name=Types.STRING,
		password=Types.STRING,
		roles=Types.LIST(Types.STRING),
		avatar=Types.STRING,
	)
	
	INDEX_BY = dict(
		email=Indexing.Normalize,
		name=Indexing.Normalize,
	)

# Create storage and register classes
storage = ObjectStorage(DirectoryBackend("Data/")).use(Account)

# Create and save objects
account = Account(properties={"email": "user@example.com", "name": "John"})
account.save()

# Retrieve by OID
retrieved = Account.Get(account.oid)

# Query all
for acc in Account.All():
	print(acc.email)
```

**Property Access**:

```python
# Direct property access (using descriptors)
account.email = "new@example.com"
name = account.name

# Or via methods
account.setProperty("email", "new@example.com")
value = account.getProperty("name")
```

**Class Methods**:
- `ClassName.Get(oid)` - Retrieve object by OID
- `ClassName.All()` - Iterate all objects of this type
- `ClassName.Count()` - Count objects
- `ClassName.List(count, start, end)` - List objects with pagination
- `ClassName.Has(oid)` - Check if object exists
- `ClassName.Ensure(oid)` - Get or create object
- `ClassName.Import(properties)` - Create from dict/export

**Instance Methods**:
- `.save()` - Persist to storage
- `.remove()` - Delete from storage
- `.export(**options)` - Convert to primitive dict (depth control)
- `.update(dict)` - Update multiple properties
- `.getUpdateTime(key)` - Get timestamp of last update

### 2. StoredRaw (Binary/File Data)

**Purpose**: Store raw binary data (files, images, videos) with associated metadata.

**Key Features**:
- Separate data and metadata storage
- Data streaming support
- File path access for backends that support it
- Lazy data loading
- Metadata dictionary

**Basic Usage**:

```python
from storage.raw import StoredRaw, RawStorage

# Define a raw data class
class Image(StoredRaw):
	"""Stores image files with metadata"""
	
	def getFormat(self):
		return self.meta("format")
	
	def getWidth(self):
		return self.meta("width")
	
	def getHeight(self):
		return self.meta("height")

# Create storage
raw_storage = RawStorage(DirectoryBackend("Data/")).use(Image)

# Store image data
with open("photo.jpg", "rb") as f:
	data = f.read()

image = Image(data=data, format="jpeg", width=1920, height=1080)
image.save()

# Access metadata
width = image.meta("width")
format = image.meta("format")

# Access data
for chunk in image.data():
	process(chunk)

# Get file path (if backend supports it)
path = image.path()
```

**Class Methods**:
- `ClassName.Get(oid)` - Retrieve by OID
- `ClassName.All()` - Iterate all
- `ClassName.Count()` - Count objects
- `ClassName.Has(oid)` - Check existence

**Instance Methods**:
- `.meta(name, value)` - Get/set metadata
- `.setMeta(**kwargs)` - Set multiple metadata fields
- `.data(size)` - Stream data chunks
- `.loadData()` - Load all data (use cautiously for large files)
- `.path()` - Get filesystem path (backend-dependent)
- `.save()` - Persist changes

### 3. Relations

**Purpose**: Link `StoredObject` instances to each other.

**Declaration**:

```python
class Comment(StoredObject):
	PROPERTIES = dict(
		message=Types.STRING,
		date=Types.DATE,
	)
	
	RELATIONS = dict(
		author=Account,  # Single relation
		replies=[Comment],  # Many relation (list/tuple notation)
	)
```

**Usage**:

```python
comment = Comment()
comment.author = account  # Set single relation
comment.replies.add(reply_comment)  # Add to many relation

# Access
author = comment.author.one()  # Get single related object
for reply in comment.replies:  # Iterate many relation
	print(reply.message)

# Check membership
if comment.replies.has(some_comment):
	pass

# Remove
comment.replies.remove(some_comment)
comment.replies.clear()
```

**Relation Methods**:
- `.add(obj)` / `.append(obj)` - Add object to relation
- `.remove(obj)` - Remove from relation
- `.clear()` - Remove all
- `.get(start, limit, resolve)` - Get with pagination
- `.one(index)` - Get single item
- `.has(obj_or_oid)` - Check membership
- `.list()` / `.all()` - Get all as iterator
- `len(relation)` - Count items

### 4. Types

**Available Types**:

```python
Types.BOOL
Types.INTEGER, Types.POSITIVE, Types.FLOAT, Types.NUMBER
Types.STRING, Types.LINE, Types.EMAIL, Types.PASSWORD, Types.URL
Types.HTML, Types.MARKDOWN, Types.RICHTEXT
Types.DATE, Types.TIME, Types.DATETIME
Types.BINARY
Types.ANY

# Composite types
Types.LIST(Types.STRING)
Types.TUPLE(Types.INTEGER, Types.STRING)
Types.ONE_OF(Types.STRING, Types.INTEGER)
Types.MAP(name=Types.STRING, age=Types.INTEGER)
Types.ENUM("draft", "published", "archived")
Types.REFERENCE(Account)  # Reference to another StoredObject
Types.RANGE(0, 100, Types.INTEGER)
```

### 5. Indexing

**Purpose**: Enable efficient lookups by property values.

**Declaration**:

```python
from storage.index import Indexing, Indexes

class Article(StoredObject):
	PROPERTIES = {
		"title": Types.STRING,
		"author": Types.STRING,
		"content": Types.STRING,
		"date": Types.STRING,
		"status": Types.STRING,
	}
	
	INDEX_BY = dict(
		# Simple normalization
		date=Indexing.Normalize,
		
		# Extract keywords from multiple fields
		keywords=lambda name, obj: Indexing.Keywords(
			(obj.title, obj.author, obj.content)
		),
	)
```

**Indexing Functions**:
- `Indexing.Value` - Pass-through
- `Indexing.Normalize` - Lowercase, strip, normalize spaces
- `Indexing.NoAccents` - Remove accents
- `Indexing.Keyword` - Single keyword normalization
- `Indexing.Keywords(values, minLength=3)` - Extract multiple keywords
- `Indexing.UpdateTime` - Index by update timestamp
- `Indexing.Paths(separator)` - Index hierarchical paths

**Setup and Usage**:

```python
from storage.backends.dbm import DBMBackend

# Create indexes manager
indexes = Indexes(DBMBackend, "Data/").use(Article, Account)

# Access indexes via shortcuts
articles_by_date = indexes.Article.by.date

# Query
for article in articles_by_date("2025-01-15"):
	print(article.title)

# Get one
article = articles_by_date.one("2025-01-15")

# Check existence
if articles_by_date.has("2025-01-15"):
	pass

# Rebuild indexes
indexes.rebuild(sync=True)
```

**Index Methods**:
- `.get(key, restore=True)` - Iterator of objects with key
- `.one(key, index=0)` - Get single object
- `.has(key)` - Check if key exists
- `.count(key)` - Count objects for key
- `.keys()` - List all index keys
- `.list(start, end, count, order)` - Paginated list

### 6. Backends

**Available Backends**:

```python
from storage.backends.memory import MemoryBackend
from storage.backends.fs import DirectoryBackend
from storage.backends.dbm import DBMBackend

# Memory (ephemeral, for testing)
backend = MemoryBackend()

# Directory (file-per-object)
backend = DirectoryBackend("path/to/data")

# DBM (key-value database)
backend = DBMBackend("path/to/db")
```

**Backend Operations**:
- `.add(key, value)` - Create entry
- `.update(key, value)` - Update entry
- `.get(key)` - Retrieve value
- `.has(key)` - Check existence
- `.remove(key)` - Delete entry
- `.keys(prefix)` - List keys
- `.sync()` - Flush to disk
- `.clear()` - Remove all data

### 7. Complete Application Pattern

**Recommended Structure**:

```python
from storage import Types, DirectoryBackend, DBMBackend
from storage.objects import StoredObject, ObjectStorage
from storage.raw import StoredRaw, RawStorage
from storage.index import Indexing, Indexes

# 1. Define models
class Account(StoredObject):
	PROPERTIES = dict(email=Types.EMAIL, name=Types.STRING)
	INDEX_BY = dict(email=Indexing.Normalize)

class File(StoredRaw):
	"""Stores files"""

class Article(StoredObject):
	PROPERTIES = dict(
		title=Types.STRING,
		content=Types.STRING,
		status=Types.ENUM("draft", "published"),
	)
	RELATIONS = dict(
		author=Account,
		attachments=[File],
	)
	INDEX_BY = dict(
		status=Indexing.Normalize,
		keywords=lambda n, obj: Indexing.Keywords((obj.title, obj.content)),
	)

# 2. Create unified interface
class DataInterface:
	def __init__(self, path="Data/"):
		# Object storage
		self.objects = ObjectStorage(DirectoryBackend(path)).use(
			Account, Article
		)
		
		# Raw storage
		self.raw = RawStorage(DirectoryBackend(path)).use(File)
		
		# Indexes
		self.indexes = Indexes(DBMBackend, path).use(
			Account, Article
		)
	
	def sync(self):
		"""Persist all changes"""
		self.objects.sync()
		self.raw.sync()
		return True
	
	def reindex(self):
		"""Rebuild all indexes"""
		return self.indexes.rebuild(sync=True)

# 3. Use the interface
data = DataInterface("Data/")

# Create account
account = Account(properties={"email": "user@example.com", "name": "John"})
account.save()

# Create article
article = Article(properties={
	"title": "Hello World",
	"content": "This is my first article",
	"status": "draft",
})
article.author = account
article.save()

# Query by index
for art in data.indexes.Article.by.status("published"):
	print(art.title)

# Search keywords
for art in data.indexes.Article.by.keywords("hello"):
	print(art.title)

# Sync to disk
data.sync()
```

## Key Patterns and Best Practices

### Property vs Metadata
- **StoredObject**: Use typed `PROPERTIES` for structured data
- **StoredRaw**: Use `.meta()` dictionary for flexible metadata

### Relations vs References
- **Relations** (RELATIONS): Managed collections, lazy loading
- **Type.REFERENCE**: Just a type hint, stores as primitive

### Saving Changes
```python
# StoredObject - call save() after modifications
obj.name = "New Name"
obj.save()

# Or use context manager (auto-saves on exit)
with storage:
	obj = Account(properties={"email": "test@test.com"})
# obj is automatically saved
```

### Exporting Data
```python
# Shallow export (oid + type only)
obj.export(depth=0)

# Full export (all properties + relations as references)
obj.export(depth=1)

# Deep export (properties + expanded relations)
obj.export(depth=2)
```

### Iteration Performance
```python
# Memory efficient - uses iterator
for account in Account.All():
	process(account)

# Pagination
for account in Account.List(count=10, start=0):
	process(account)
```

### Thread Safety
- Storage uses `threading.RLock` for thread-safe operations
- Safe to use from multiple threads
- Each storage maintains internal cache with weak references

### Update Tracking
```python
# Every object tracks update times per property
timestamp = obj.getUpdateTime("email")  # When email was last updated
timestamp = obj.getUpdateTime("oid")    # When object was last modified
```

## Common Gotchas

1. **Must register with storage**: `ClassName.STORAGE` must be set before using class methods like `Get()`, `All()`

2. **Save after modifications**: Changes are not auto-persisted, call `.save()`

3. **Relations store references**: When exporting, relations export as `{oid, type}` by default (shallow)

4. **Indexes need rebuilding**: After bulk changes, call `indexes.rebuild()`

5. **StoredRaw data streaming**: Use `.data()` for large files, not `.loadData()`

6. **Collection names**: Objects stored as `ClassName.oid`, customize with `COLLECTION` attribute

7. **Reserved properties**: Cannot use `type`, `oid`, `updates` as property names

## Code Style (Per AGENTS.md)

- Use **TABS** (size 4) for indentation
- Type hints: Use `ClassVar`, `Optional`, `List`, `Type`, etc.
- Naming: PascalCase for classes, camelCase for functions/variables
- Class variables: Use `ClassVar` type hint
- Error handling: Raise exceptions with descriptive messages
