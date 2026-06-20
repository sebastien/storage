# Migration Reference (`storage.migrations`)

## Overview

The migration system provides a small, backend-backed way to evolve stored data over time.

Migrations are plain Python files discovered from a directory, applied in numeric order, and recorded in backend metadata after success.

The public API is centered on `MigrationOperator`.

## How Migrations Work

At a high level, a migration run does the following:

1. Resolve the migrations path.
2. Discover migration files matching the expected filename pattern.
3. Sort them by numeric prefix.
4. Load the list of already applied migrations from backend metadata.
5. Reject any already-applied migration whose file checksum changed.
6. Apply pending migrations in order.
7. Record each successful migration in backend metadata.

Applied migration metadata is stored under the backend metadata key:

```python
"migrations.applied"
```

This metadata is stored outside the normal user keyspace, so it does not appear in `keys()`, `count()`, or `list()` results.

## File Location

By default, migrations are discovered in:

```text
jobs/migrations
```

You can override this with the environment variable:

```text
STORAGE_MIGRATIONS_PATH
```

Example:

```bash
export STORAGE_MIGRATIONS_PATH=jobs/migrations
```

## Migration Filenames

Migration filenames must match this shape:

```text
<number>-<name>.py
```

Examples:

```text
1-initial.py
002-add_indexes.py
100-backfill_owner_ids.py
```

Rules:

- The numeric prefix may be any width.
- Files are sorted numerically, not lexicographically.
- The suffix name may use letters, numbers, and underscores.

## Implementing a Migration

Each migration file must define:

```python
def apply(storage):
	...
```

The `storage` argument is the exact storage facade or backend passed to `MigrationOperator`.

Example:

```python
def apply(storage):
	storage.backend.set("settings.version", {"value": 2})
```

For object storage migrations, you will typically pass an `ObjectStorage` instance and use registered classes.

Example:

```python
from app.models import User


def apply(storage):
	for user in User.All():
		if not user.getProperty("slug"):
			user.setProperty("slug", user.name.lower().replace(" ", "-"))
			user.save()
	storage.sync()
```

For raw or KV-oriented migrations, use the corresponding runtime or backend API.

## Running Migrations

### Basic Usage

```python
from storage import DirectoryBackend, MigrationOperator
from storage.objects import ObjectStorage

storage = ObjectStorage(DirectoryBackend("Data/")).use(...)

MigrationOperator(storage).apply()
```

### Inspect Before Applying

```python
from storage import MigrationOperator

operator = MigrationOperator(storage).prepare()

for migration in operator.list():
	print(migration.filename)

for migration in operator.pending():
	print("pending:", migration.filename)
```

### Use a Custom Path

```python
from storage import MigrationOperator

MigrationOperator(storage, path="jobs/migrations").apply()
```

## `MigrationOperator`

### Construction

```python
MigrationOperator(storage, path=None)
```

Parameters:

- `storage`: storage facade or backend to operate on.
- `path`: optional migration directory path. If omitted, `STORAGE_MIGRATIONS_PATH` is used, then `jobs/migrations`.

### Methods

#### `prepare()`

Discovers migrations, loads applied metadata, and validates checksum drift.

Returns `self`.

```python
operator = MigrationOperator(storage).prepare()
```

#### `list()`

Returns all discovered migrations as `Migration` objects.

```python
migrations = MigrationOperator(storage).list()
```

#### `applied()`

Returns the applied migration records loaded from backend metadata.

```python
records = MigrationOperator(storage).applied()
```

#### `pending()`

Returns only the migrations that have not yet been applied.

```python
pending = MigrationOperator(storage).pending()
```

#### `apply()`

Applies all pending migrations in order.

```python
records = MigrationOperator(storage).apply()
```

## `Migration`

Discovered migrations are represented by the `Migration` dataclass.

Fields:

- `id`: numeric prefix as a string
- `name`: migration suffix name
- `path`: full file path
- `filename`: basename
- `checksum`: SHA-256 checksum of file contents
- `key`: computed `<id>-<name>` identifier

Example:

```python
for migration in MigrationOperator(storage).list():
	print(migration.id, migration.name, migration.checksum)
```

## Applied Metadata Format

Applied migration records are stored as a dictionary keyed by migration key.

Example:

```python
{
	"1-initial": {
		"id": "1",
		"name": "initial",
		"filename": "1-initial.py",
		"path": "jobs/migrations/1-initial.py",
		"checksum": "...",
		"appliedAt": 1710000000,
	}
}
```

## Checksum Validation

If a migration has already been applied and its file contents later change, `prepare()` and `apply()` raise an error.

This is intentional. Applied migrations are treated as immutable history.

Example failure mode:

```python
MigrationOperator(storage).prepare()
# RuntimeError: Applied migration checksum changed: 1-initial.py
```

## Failure Behavior

If a migration raises an exception:

- execution stops immediately
- the failing migration is not recorded as applied
- previously successful migrations remain recorded

This means migrations should be written to be safe to retry.

## Writing Good Migrations

Recommended practices:

- Keep migrations small and single-purpose.
- Treat applied migration files as immutable.
- Prefer explicit, idempotent data updates when possible.
- Call `save()` on changed objects.
- Call `storage.sync()` when your migration performs many writes and you want an explicit flush point.
- Use descriptive filenames that explain intent.

Example:

```text
12-backfill_account_slugs.py
13-normalize_tag_names.py
14-rebuild_message_indexes.py
```

## Example Workflow

Create a migration file:

```python
# jobs/migrations/1-add_default_status.py

from app.models import Article


def apply(storage):
	for article in Article.All():
		if not article.getProperty("status"):
			article.setProperty("status", "draft")
			article.save()
	storage.sync()
```

Run it:

```python
from storage import DirectoryBackend, MigrationOperator
from storage.objects import ObjectStorage
from app.models import Article

storage = ObjectStorage(DirectoryBackend("Data/")).use(Article)
MigrationOperator(storage).apply()
```

Inspect state:

```python
operator = MigrationOperator(storage).prepare()
print(operator.applied())
print([_.filename for _ in operator.pending()])
```

## Summary

Use migrations when stored data needs an ordered, repeatable evolution path.

- Put files in `jobs/migrations`.
- Implement `apply(storage)` in each file.
- Run them with `MigrationOperator(storage).apply()`.
- Do not edit applied migration files after they have run.
