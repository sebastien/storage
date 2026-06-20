"""Storage migration discovery and execution."""

from __future__ import annotations

import hashlib
import importlib.util
import inspect
import os
import re
from dataclasses import dataclass
from typing import Any, Callable, Iterable, Optional

from .backends import StorageBackend
from .core import getTimestamp

DEFAULT_MIGRATIONS_PATH = "jobs/migrations"
MIGRATIONS_ENV_VAR = "STORAGE_MIGRATIONS_PATH"
MIGRATIONS_METADATA_KEY = "migrations.applied"
MIGRATIONS_PROGRESS_METADATA_KEY = "migrations.progress"
MIGRATION_RE = re.compile(r"^(?P<id>\d+)-(?P<name>[A-Za-z0-9_]+)\.py$")


@dataclass(frozen=True)
class Migration:
	id: str
	name: str
	path: str
	filename: str
	checksum: str

	@property
	def key(self) -> str:
		return f"{self.id}-{self.name}"


def getMigrationsPath(path: str | None = None) -> str:
	return path or os.environ.get(MIGRATIONS_ENV_VAR, DEFAULT_MIGRATIONS_PATH)


def _backend(storageOrBackend):
	return storageOrBackend.backend if hasattr(storageOrBackend, "backend") else storageOrBackend


def _migrationChecksum(path: str) -> str:
	with open(path, "rb") as f:
		return hashlib.sha256(f.read()).hexdigest()


def _loadMigrationModule(migration: Migration):
	spec = importlib.util.spec_from_file_location(
		f"storage_migration_{migration.id}_{migration.name}", migration.path
	)
	if spec is None or spec.loader is None:
		raise RuntimeError(f"Could not load migration module: {migration.path}")
	module = importlib.util.module_from_spec(spec)
	spec.loader.exec_module(module)
	return module


def _call(function: Callable[..., Any], *args):
	parameters = inspect.signature(function).parameters
	if len(parameters) == 0:
		return function()
	return function(*args)


class MigrationRunner:
	def __init__(self, context: "MigrationContext", classes: tuple[type, ...], label: Optional[str] = None):
		self.context = context
		self.classes = classes
		self.label = label

	def _step(self, storedObjectClass: type, operation: str) -> str:
		base = self.label or storedObjectClass.__name__
		return f"{base}.{operation}"

	def run(self, transform: Callable[[Any], Any], operation: str = "run") -> "MigrationRunner":
		for storedObjectClass in self.classes:
			step = self._step(storedObjectClass, operation)
			if self.context.isDone(step):
				continue
			self.context.start(step)
			resume = self.context.resumeKey(step)
			skipping = bool(resume)
			for key in self.context.storage.keys(
				storedObjectClass, order=StorageBackend.ORDER_ASCENDING
			):
				if skipping:
					if key == resume:
						skipping = False
					continue
				storedObject = self.context.storage.get(key)
				if storedObject is None:
					self.context.checkpoint(step, key)
					continue
				before = storedObject.export()
				result = transform(storedObject)
				if result is not False and storedObject.export() != before:
					storedObject.save()
				self.context.checkpoint(step, key)
			self.context.finish(step)
		return self

	def set(self, **fields: Any) -> "MigrationRunner":
		def transform(storedObject):
			for name, value in list(fields.items()):
				resolved = value(storedObject) if callable(value) else value
				if getattr(storedObject, name) != resolved:
					setattr(storedObject, name, resolved)

		return self.run(transform, operation="set")

	def owner(self, owner) -> "MigrationRunner":
		if owner is None:
			return self

		def transform(storedObject):
			if storedObject.hasOwner():
				return False
			self.context.storage.changeOwner(storedObject, owner)
			return False

		return self.run(transform, operation="owner")


class MigrationContext:
	def __init__(self, storage, backend, migration: Migration):
		self.storage = storage
		self.backend = backend
		self.migration = migration

	def _allProgress(self) -> dict[str, dict[str, Any]]:
		return self.backend.getMetadata(MIGRATIONS_PROGRESS_METADATA_KEY, {}) or {}

	def _progress(self) -> dict[str, Any]:
		progress = self._allProgress()
		return dict(progress.get(self.migration.key, {}))

	def _storeProgress(self, value: Optional[dict[str, Any]]) -> dict[str, Any]:
		progress = self._allProgress()
		if value:
			progress[self.migration.key] = value
		else:
			progress.pop(self.migration.key, None)
		self.backend.setMetadata(MIGRATIONS_PROGRESS_METADATA_KEY, progress)
		if hasattr(self.backend, "sync"):
			self.backend.sync()
		return progress

	def isDone(self, step: str) -> bool:
		return step in (self._progress().get("done") or [])

	def start(self, step: str) -> dict[str, Any]:
		progress = self._progress()
		if progress.get("step") != step:
			progress["step"] = step
			progress["lastKey"] = None
		self._storeProgress(progress)
		return progress

	def resumeKey(self, step: str) -> Optional[str]:
		progress = self._progress()
		return progress.get("lastKey") if progress.get("step") == step else None

	def checkpoint(self, step: str, key: str) -> dict[str, Any]:
		progress = self._progress()
		progress["step"] = step
		progress["lastKey"] = key
		progress["updated"] = int(progress.get("updated") or 0) + 1
		self._storeProgress(progress)
		return progress

	def finish(self, step: str) -> dict[str, Any]:
		progress = self._progress()
		done = list(progress.get("done") or [])
		if step not in done:
			done.append(step)
		progress["done"] = done
		progress["step"] = None
		progress["lastKey"] = None
		self._storeProgress(progress)
		return progress

	def clear(self) -> None:
		self._storeProgress(None)

	def each(self, *classes: type, label: Optional[str] = None) -> MigrationRunner:
		return MigrationRunner(self, classes, label=label)

	def only(self, storedObjectClass: type, ifEmpty: Iterable[type] = ()):
		items = storedObjectClass.All(order=StorageBackend.ORDER_ASCENDING)
		first = next(items, None)
		if first is None:
			for requiredClass in ifEmpty:
				if requiredClass.Count() > 0:
					raise RuntimeError(
						f"Migration {self.migration.id} requires exactly one {storedObjectClass.__name__} before migrating existing data"
					)
			return None
		if next(items, None) is not None:
			raise RuntimeError(
				f"Migration {self.migration.id} requires exactly one {storedObjectClass.__name__} before migrating existing data"
			)
		return first

	def after(self, callback: Callable[..., Any], step: Optional[str] = None):
		name = step or getattr(callback, "__name__", "after")
		stepName = f"after.{name}"
		if self.isDone(stepName):
			return None
		self.start(stepName)
		result = _call(callback, self.storage)
		self.finish(stepName)
		return result


class _MigrationBuilder:
	def __call__(self, *changes: dict[str, Any]):
		def decorator(function):
			existing = list(getattr(function, "SCHEMA_CHANGES", []))
			function.SCHEMA_CHANGES = list(changes) + existing
			function.USES_MIGRATION_CONTEXT = True
			return function

		return decorator

	def add(self, storedObjectClass, name: str, type: Any) -> dict[str, Any]:
		return {"op": "addProperty", "class": storedObjectClass, "name": name, "type": type}

	def remove(self, storedObjectClass, name: str) -> dict[str, Any]:
		return {"op": "removeProperty", "class": storedObjectClass, "name": name}

	def rename(self, storedObjectClass, old: str, new: str) -> dict[str, Any]:
		return {"op": "renameProperty", "class": storedObjectClass, "from": old, "to": new}

	def split(self, storedObjectClass, old: str, **fields: Any) -> dict[str, Any]:
		return {"op": "splitProperty", "class": storedObjectClass, "from": old, "to": fields}

	def ownership(self, storedObjectClass, ownerClass, previous=None) -> dict[str, Any]:
		return {
			"op": "changeOwnership",
			"class": storedObjectClass,
			"from": previous,
			"to": ownerClass,
		}


migration = _MigrationBuilder()


class MigrationOperator:
	def __init__(self, storage, path: str | None = None):
		self.storage = storage
		self.backend = _backend(storage) if storage is not None else None
		self.path = getMigrationsPath(path)
		self.migrations: list[Migration] = []
		self.records: dict = {}
		self._schemaChanges: dict[str, list[dict]] = {}
		self.prepared = False

	def prepare(self):
		self.migrations = self._discover()
		self.records = self._applied()
		for migrationRecord in self.migrations:
			recorded = self.records.get(migrationRecord.key)
			if recorded and recorded.get("checksum") != migrationRecord.checksum:
				raise RuntimeError(
					"Applied migration checksum changed: %s" % migrationRecord.filename
				)
		self.prepared = True
		return self

	def list(self) -> list[Migration]:
		if not self.prepared:
			self.prepare()
		return self.migrations

	def applied(self):
		if not self.prepared:
			self.prepare()
		return self.records

	def pending(self) -> list[Migration]:
		if not self.prepared:
			self.prepare()
		return [_ for _ in self.migrations if _.key not in self.records]

	def apply(self):
		if self.storage is None or self.backend is None:
			raise RuntimeError("MigrationOperator.apply requires a storage or backend")
		self.prepare()
		for migrationRecord in self.pending():
			module = _loadMigrationModule(migrationRecord)
			apply = getattr(module, "apply", None)
			if not callable(apply):
				raise RuntimeError(
					"Migration does not define apply(storage): %s" % migrationRecord.path
				)
			if getattr(apply, "USES_MIGRATION_CONTEXT", False):
				context = MigrationContext(self.storage, self.backend, migrationRecord)
				apply(context)
				context.clear()
			else:
				apply(self.storage)
			self.records[migrationRecord.key] = {
				"id": migrationRecord.id,
				"name": migrationRecord.name,
				"filename": migrationRecord.filename,
				"path": migrationRecord.path,
				"checksum": migrationRecord.checksum,
				"appliedAt": getTimestamp(),
			}
			self.backend.setMetadata(MIGRATIONS_METADATA_KEY, self.records)
			if hasattr(self.backend, "sync"):
				self.backend.sync()
		return self.records

	def getSchemaChanges(self, migrationRecord: Migration) -> list[dict]:
		if migrationRecord.key not in self._schemaChanges:
			from .schema import getMigrationSchemaChanges

			module = _loadMigrationModule(migrationRecord)
			self._schemaChanges[migrationRecord.key] = getMigrationSchemaChanges(module)
		return list(self._schemaChanges[migrationRecord.key])

	def _discover(self) -> list[Migration]:
		if not os.path.isdir(self.path):
			return []
		result = []
		for name in os.listdir(self.path):
			match = MIGRATION_RE.match(name)
			if not match:
				continue
			migrationPath = os.path.join(self.path, name)
			if not os.path.isfile(migrationPath):
				continue
			result.append(
				Migration(
					id=match.group("id"),
					name=match.group("name"),
					path=migrationPath,
					filename=name,
					checksum=_migrationChecksum(migrationPath),
				)
			)
		result.sort(key=lambda _: (int(_.id), _.name, _.filename))
		return result

	def _applied(self):
		if self.backend is None:
			return {}
		return self.backend.getMetadata(MIGRATIONS_METADATA_KEY, {}) or {}


__all__ = [
	"DEFAULT_MIGRATIONS_PATH",
	"MIGRATIONS_ENV_VAR",
	"MIGRATIONS_METADATA_KEY",
	"MIGRATIONS_PROGRESS_METADATA_KEY",
	"Migration",
	"MigrationContext",
	"MigrationOperator",
	"MigrationRunner",
	"getMigrationsPath",
	"migration",
]


# EOF
