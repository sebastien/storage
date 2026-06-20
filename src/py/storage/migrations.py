"""Storage migration discovery and execution."""

import hashlib
import importlib.util
import os
import re
from dataclasses import dataclass

from .core import getTimestamp

DEFAULT_MIGRATIONS_PATH = "jobs/migrations"
MIGRATIONS_ENV_VAR = "STORAGE_MIGRATIONS_PATH"
MIGRATIONS_METADATA_KEY = "migrations.applied"
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
		for migration in self.migrations:
			recorded = self.records.get(migration.key)
			if recorded and recorded.get("checksum") != migration.checksum:
				raise RuntimeError(
					"Applied migration checksum changed: %s" % migration.filename
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
		for migration in self.pending():
			module = _loadMigrationModule(migration)
			apply = getattr(module, "apply", None)
			if not callable(apply):
				raise RuntimeError(
					"Migration does not define apply(storage): %s" % migration.path
				)
			apply(self.storage)
			self.records[migration.key] = {
				"id": migration.id,
				"name": migration.name,
				"filename": migration.filename,
				"path": migration.path,
				"checksum": migration.checksum,
				"appliedAt": getTimestamp(),
			}
			self.backend.setMetadata(MIGRATIONS_METADATA_KEY, self.records)
			if hasattr(self.backend, "sync"):
				self.backend.sync()
		return self.records

	def getSchemaChanges(self, migration: Migration) -> list[dict]:
		if migration.key not in self._schemaChanges:
			from .schema import getMigrationSchemaChanges

			module = _loadMigrationModule(migration)
			self._schemaChanges[migration.key] = getMigrationSchemaChanges(module)
		return list(self._schemaChanges[migration.key])

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
	"Migration",
	"MigrationOperator",
	"getMigrationsPath",
]
