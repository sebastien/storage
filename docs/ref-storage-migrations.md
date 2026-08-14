# Storage Migrations Reference (`storage.migrations`)

Part of the [Storage Module Reference](ref-storage.md).

## Overview

The `storage.migrations` module provides an ordered, backend-backed mechanism to evolve stored data and object schemas over time.

Migrations are plain Python files stored in a directory, discovered automatically, sorted in numeric order, validated for content immutability via SHA-256 checksums, and recorded in backend metadata upon completion.

The migration runtime supports:
- **Declarative schema change tracking** via the `@migration` decorator.
- **Resumable data migrations** with automatic checkpointing and progress tracking.
- **Batch transforms, field setters, ownership reassignment, and public ID backfills** via `MigrationRunner`.
- **Automatic execution** during `ObjectStorage` startup via `SchemaValidator`.

---

## How Migrations Work

When `MigrationOperator.apply()` runs:

1. **Resolve directory**: Locates the migration files (defaults to `jobs/migrations` or `$STORAGE_MIGRATIONS_PATH`).
2. **Discover files**: Scans for files matching the pattern `<id>-<name>.py`.
3. **Sort numerically**: Orders migrations by numeric ID (e.g. `1`, `02`, `100`).
4. **Load applied records**: Reads applied migration records from backend metadata (`migrations.applied`).
5. **Verify checksums**: Computes SHA-256 of all discovered files. If any previously applied migration has changed, execution halts immediately with `RuntimeError`.
6. **Apply pending migrations**:
   - If the migration's `apply()` function is decorated with `@migration`, a `MigrationContext` is passed with checkpointing support.
   - Otherwise, the raw `storage` facade or backend is passed.
7. **Record completion**: Writes successful migration metadata (`id`, `name`, `filename`, `path`, `checksum`, `appliedAt`) to `migrations.applied` and synchronizes the backend.

```
jobs/migrations/
├── 1-initial_schema.py
├── 2-backfill_slugs.py
└── 3-assign_public_ids.py
```

---

## File Location & Naming Conventions

### Default Location

By default, migrations are discovered in:
```text
jobs/migrations
```

You can override the discovery path using the environment variable:
```bash
export STORAGE_MIGRATIONS_PATH=path/to/custom/migrations
```
Or pass the path directly when creating `MigrationOperator(storage, path="...")`.

### Filename Pattern

Migration filenames must conform to:
```text
<number>-<name>.py
```

* `<number>`: integer sequence prefix (sorted numerically: `1-*.py` runs before `02-*.py` and `100-*.py`).
* `<name>`: descriptive name containing letters, digits, and underscores.

Examples:
- `1-create_users.py`
- `2-add_profile_fields.py`
- `03-assign_public_ids.py`
- `10-normalize_tags.py`

---

## Writing Migrations

Migrations can be written either using the high-level declarative `@migration` context runner (recommended for `ObjectStorage`) or as a simple low-level function.

### 1. Declarative Migrations with `@migration` (Recommended)

Decorating `apply(m)` with `@migration(*schemaChanges)` provides a `MigrationContext` instance `m` with resumable execution, step tracking, and schema declaration.

```python
from storage import Types, migration
from app.models import Article

@migration(
	migration.add(Article, "slug", Types.STRING),
	migration.add(Article, "views", Types.INTEGER),
)
def apply(m):
	# 1. Backfill slug from title for all Article records
	def generateSlug(article):
		if not article.slug and article.title:
			article.slug = article.title.lower().replace(" ", "-")

	m.each(Article).run(generateSlug, operation="slugs")

	# 2. Set default values
	m.each(Article).set(views=0)
```

#### Schema Change Declarations

Declare schema changes to notify `SchemaValidator` of intended model changes:

* `migration.add(Model, "field", Types.TYPE)`: Declares a new property.
* `migration.remove(Model, "field")`: Declares property removal.
* `migration.rename(Model, "old_name", "new_name")`: Declares property rename.
* `migration.split(Model, "old_name", new1=Types.TYPE, new2=Types.TYPE)`: Declares splitting a property into multiple fields.
* `migration.ownership(Model, OwnerModel, previous=None)`: Declares a change in model ownership.

### 2. Context Operations (`MigrationContext`)

`MigrationContext` provides helpers to iterate records safely:

```python
@migration()
def apply(m):
	# Iterate over all records of one or more classes
	m.each(User, Account).run(lambda obj: obj.touch(), operation="touch")

	# Set static or computed properties
	m.each(User).set(
		status="active",
		displayName=lambda u: f"{u.firstName} {u.lastName}".strip()
	)

	# Assign owner to unowned records
	tenant = m.only(Tenant, ifEmpty=(User,))
	if tenant:
		m.each(User).owner(tenant)

	# Backfill partition-scoped public IDs
	m.each(Ticket).publicIDs()

	# Run a one-time final step after iteration
	m.after(lambda storage: storage.sync(), step="flush")
```

#### `m.each(*classes, label=None)` -> `MigrationRunner`

Returns a `MigrationRunner` that processes records in ascending key order.

Available runner methods:
* `.run(transform, operation="run")`: Calls `transform(obj)` for each record. If `obj.export()` changes and `transform` does not return `False`, `obj.save()` is called automatically. Checkpoints progress per record.
* `.set(**fields)`: Sets fields on all objects. Values can be static literals or `callable(obj) -> value`.
* `.owner(owner)`: Reassigns unowned objects to `owner` using `storage.changeOwner(obj, owner)`.
* `.publicIDs()`: Triggers `.save()` on objects of classes that opt into `PUBLIC_ID = PublicID.Partition` to atomically allocate missing numbers.

#### `m.only(Model, ifEmpty=())`

Fetches the single instance of a model (e.g. root account, default tenant).
* Raises `RuntimeError` if more than one instance exists.
* If none exist, checks `ifEmpty` classes: if any `ifEmpty` class has records, raises an error; otherwise returns `None`.

#### `m.after(callback, step=None)`

Executes a standalone callback once all prior steps complete. Automatically recorded in the migration's completed steps.

### 3. Low-Level Migrations (`apply(storage)`)

For raw storage, KV stores, or direct backend manipulation:

```python
def apply(storage):
	# Direct backend or storage operations
	storage.backend.set("system.version", {"version": 3})
	if hasattr(storage, "sync"):
		storage.sync()
```

---

## Checkpointing and Resumability

Long-running migrations that get interrupted (e.g. timeout, process crash) do not need to restart from the beginning.

1. **Step Progress**: `MigrationContext` persists the current operation name and the last processed storage key under `migrations.progress` in backend metadata.
2. **Resumption**: When the migration is retried, `runner.run()` fast-forwards directly to `lastKey` and continues from the next record.
3. **Completion**: Once all steps finish successfully, progress metadata is cleared and the migration is permanently recorded in `migrations.applied`.

---

## Metadata Structure

### Applied Migrations (`migrations.applied`)

Stored under metadata key `"migrations.applied"`:

```json
{
	"1-initial": {
		"id": "1",
		"name": "initial",
		"filename": "1-initial.py",
		"path": "jobs/migrations/1-initial.py",
		"checksum": "a3f5c7...",
		"appliedAt": 1710000000
	},
	"2-backfill": {
		"id": "2",
		"name": "backfill",
		"filename": "2-backfill.py",
		"path": "jobs/migrations/2-backfill.py",
		"checksum": "b8e1d2...",
		"appliedAt": 1710000050
	}
}
```

### In-Progress Checkpoints (`migrations.progress`)

Stored under metadata key `"migrations.progress"`:

```json
{
	"2-backfill": {
		"step": "Article.slugs",
		"lastKey": "Article.12345",
		"updated": 42,
		"done": ["Article.init"]
	}
}
```

---

## Programmatic API (`MigrationOperator`)

```python
from storage import DirectoryBackend, MigrationOperator
from storage.objects import ObjectStorage

storage = ObjectStorage(DirectoryBackend("Data/")).use(...)
operator = MigrationOperator(storage, path="jobs/migrations")
```

### Methods

| Method | Returns | Description |
| :--- | :--- | :--- |
| `operator.prepare()` | `self` | Discovers files, reads applied metadata, and verifies checksums. |
| `operator.list()` | `list[Migration]` | Returns all discovered migrations sorted by ID. |
| `operator.applied()` | `dict` | Returns applied migration records dictionary. |
| `operator.pending()` | `list[Migration]` | Returns only unapplied `Migration` objects. |
| `operator.apply()` | `dict` | Applies all pending migrations in order and returns applied records. |
| `operator.getSchemaChanges(migration)` | `list[dict]` | Returns declared `@migration` schema changes for a migration. |

---

## Automatic Integration with `SchemaValidator`

When initializing `ObjectStorage(backend, validateSchema=True)` (default):

1. `SchemaValidator` inspects the declared Python models against stored schema metadata in `schema.objects`.
2. If non-additive changes exist (e.g. removed property, altered property type, modified relation), `SchemaValidator` checks if pending migrations supply matching `@migration` changes.
3. If covered, `SchemaValidator` automatically runs `MigrationOperator(storage).apply()` and updates `schema.objects` metadata.
4. If uncovered breaking changes exist, `SchemaValidator` raises `SchemaValidationError` with a detailed diff.

---

## Best Practices

1. **Treat Applied Migrations as Immutable**: Never modify an applied migration file. Checksums will fail on next startup.
2. **Use `@migration` for Structured Objects**: Enables automatic schema synchronization and step checkpointing.
3. **Make Migrations Idempotent**: Design transforms so that re-running them produces identical results.
4. **Use Atomic Backfills**: Use `m.each(Model).publicIDs()` to safely backfill partition public IDs.
5. **Separate Large Migrations**: Split large schema changes into sequential migration steps.
