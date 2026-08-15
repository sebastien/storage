from .base import StorageBackend

from io import IOBase
import os
import sqlite3
import threading


class SQLiteBackend(StorageBackend):
	"""A SQLite-backed key/value backend using Python's stdlib sqlite3 module."""

	HAS_RAW = True
	HAS_STREAM = True
	HAS_FILE = False
	HAS_ORDERING = True
	DEFAULT_STREAM_SIZE = 1024 * 100

	def __init__(self, path, autoSync=True, wal=True):
		super().__init__()
		self.path = path if path.endswith(".sqlite3") else f"{path}.sqlite3"
		self.autoSync = autoSync
		self.wal = wal
		self.connection = None
		self.lock = threading.RLock()
		self._open()

	def _open(self):
		with self.lock:
			if self.connection is not None:
				return False
			parent = os.path.dirname(os.path.abspath(self.path))
			if parent and not os.path.isdir(parent):
				raise RuntimeError("SQLite database parent does not exist: %s" % parent)
			try:
				self.connection = sqlite3.connect(self.path)
			except sqlite3.Error as e:
				raise RuntimeError("Cannot open SQLite database at path {}:{}".format(self.path, e))
			if self.wal:
				self.connection.execute("PRAGMA journal_mode=WAL")
				self.connection.execute("PRAGMA synchronous=NORMAL")
			self.connection.execute("PRAGMA foreign_keys=ON")
			self.connection.execute(
				"CREATE TABLE IF NOT EXISTS kv ("
				"key TEXT PRIMARY KEY, "
				"data TEXT NOT NULL, "
				"updated INTEGER NOT NULL DEFAULT (unixepoch())"
				")"
			)
			self.connection.execute(
				"CREATE TABLE IF NOT EXISTS raw ("
				"key TEXT PRIMARY KEY, "
				"data BLOB, "
				"updated INTEGER NOT NULL DEFAULT (unixepoch())"
				")"
			)
			self.connection.execute(
				"CREATE TABLE IF NOT EXISTS metadata ("
				"key TEXT PRIMARY KEY, "
				"data TEXT NOT NULL, "
				"updated INTEGER NOT NULL DEFAULT (unixepoch())"
				")"
			)
			self.connection.execute(
				"CREATE TABLE IF NOT EXISTS public_ids ("
				"scope TEXT PRIMARY KEY, "
				"next INTEGER NOT NULL"
				")"
			)
			self.connection.execute(
				"CREATE TABLE IF NOT EXISTS public_objects ("
				"scope TEXT NOT NULL, "
				"public_id INTEGER NOT NULL, "
				"object_key TEXT NOT NULL, "
				"PRIMARY KEY(scope, public_id)"
				")"
			)
			self.connection.commit()
			return True

	def _connection(self):
		if self.connection is None:
			self._open()
		return self.connection

	def _commit(self):
		if self.autoSync:
			self._connection().commit()

	def add(self, key, data):
		key, data = self._serialize(key, data)
		with self.lock:
			self._connection().execute(
				"INSERT INTO kv(key, data, updated) VALUES (?, ?, unixepoch()) "
				"ON CONFLICT(key) DO UPDATE SET data=excluded.data, updated=unixepoch()",
				(key, data),
			)
			self._commit()
		return self

	def update(self, key, data):
		return self.add(key, data)

	def remove(self, key):
		key = self._serialize(key=key)
		with self.lock:
			self._connection().execute("DELETE FROM kv WHERE key = ?", (key,))
			self._commit()
		return self

	def sync(self):
		with self.lock:
			self._connection().commit()
		return self

	def has(self, key):
		key = self._serialize(key=key)
		with self.lock:
			cursor = self._connection().execute("SELECT 1 FROM kv WHERE key = ?", (key,))
			return cursor.fetchone() is not None

	def get(self, key):
		key = self._serialize(key=key)
		with self.lock:
			cursor = self._connection().execute("SELECT data FROM kv WHERE key = ?", (key,))
			row = cursor.fetchone()
		return None if row is None else self._deserialize(data=row[0])

	def keys(self, collection=None, order=StorageBackend.ORDER_NONE):
		query = "SELECT key FROM kv"
		if order == StorageBackend.ORDER_ASCENDING:
			query += " ORDER BY key ASC"
		elif order == StorageBackend.ORDER_DESCENDING:
			query += " ORDER BY key DESC"
		with self.lock:
			rows = list(self._connection().execute(query))
		for row in rows:
			key = self._deserialize(key=row[0])
			if self._matchesPrefix(key, collection):
				yield key

	def clear(self):
		with self.lock:
			self._connection().execute("DELETE FROM kv")
			self._connection().execute("DELETE FROM raw")
			self._connection().execute("DELETE FROM metadata")
			self._connection().execute("DELETE FROM public_objects")
			self._commit()
		return self

	def list(self, key=None):
		assert key is None, "Not implemented"
		with self.lock:
			rows = list(self._connection().execute("SELECT data FROM kv"))
		for row in rows:
			yield self._deserialize(data=row[0])

	def count(self, key=None) -> int:
		if key is None:
			with self.lock:
				cursor = self._connection().execute("SELECT count(*) FROM kv")
				return cursor.fetchone()[0]
		else:
			return len(list(self.keys(key)))

	def hasRawData(self, key, ext=None):
		key = self._rawKey(key, ext)
		with self.lock:
			cursor = self._connection().execute("SELECT 1 FROM raw WHERE key = ?", (key,))
			return cursor.fetchone() is not None

	def saveRawData(self, key, data, ext=None):
		key = self._rawKey(key, ext)
		data = self._readRawData(data)
		with self.lock:
			self._connection().execute(
				"INSERT INTO raw(key, data, updated) VALUES (?, ?, unixepoch()) "
				"ON CONFLICT(key) DO UPDATE SET data=excluded.data, updated=unixepoch()",
				(key, sqlite3.Binary(data) if data is not None else None),
			)
			self._commit()
		return self

	def streamRawData(self, key, size=None, ext=None):
		key = self._rawKey(key, ext)
		size = size or self.DEFAULT_STREAM_SIZE
		with self.lock:
			cursor = self._connection().execute("SELECT length(data) FROM raw WHERE key = ?", (key,))
			row = cursor.fetchone()
			length = row[0] if row else None
		if length is None:
			yield None
			return
		start = 1
		while start <= length:
			with self.lock:
				cursor = self._connection().execute(
					"SELECT substr(data, ?, ?) FROM raw WHERE key = ?",
					(start, size, key),
				)
				row = cursor.fetchone()
			if not row or row[0] is None:
				break
			yield row[0]
			start += size

	def getRawDataPath(self, key, ext=None):
		raise NotImplementedError("SQLiteBackend stores raw data in SQLite BLOBs")

	def getMetadata(self, key=None, default=None):
		with self.lock:
			if key is None:
				rows = list(self._connection().execute("SELECT key, data FROM metadata"))
				return dict((row[0], self._deserialize(data=row[1])) for row in rows)
			cursor = self._connection().execute(
				"SELECT data FROM metadata WHERE key = ?",
				(key,),
			)
			row = cursor.fetchone()
		return default if row is None else self._deserialize(data=row[0])

	def setMetadata(self, key, value):
		data = self._serialize(data=value)
		with self.lock:
			self._connection().execute(
				"INSERT INTO metadata(key, data, updated) VALUES (?, ?, unixepoch()) "
				"ON CONFLICT(key) DO UPDATE SET data=excluded.data, updated=unixepoch()",
				(key, data),
			)
			self._commit()
		return value

	def removeMetadata(self, key):
		with self.lock:
			if key is None:
				self._connection().execute("DELETE FROM metadata")
			else:
				self._connection().execute("DELETE FROM metadata WHERE key = ?", (key,))
			self._commit()
		return self

	def storePublicObject(self, key, scope, factory):
		"""Atomically allocate, persist, and register a public object ID."""
		with self.lock:
			connection = self._connection()
			transactionStarted = not connection.in_transaction
			try:
				if transactionStarted:
					connection.execute("BEGIN IMMEDIATE")
				else:
					connection.execute("SAVEPOINT public_id_allocation")
				connection.execute(
					"INSERT INTO public_ids(scope, next) VALUES (?, 2) "
					"ON CONFLICT(scope) DO UPDATE SET next=next + 1",
					(scope,),
				)
				publicID = connection.execute(
					"SELECT next - 1 FROM public_ids WHERE scope = ?", (scope,)
				).fetchone()[0]
				data = self._serialize(data=factory(publicID))
				serializedKey = self._serialize(key=key)
				connection.execute(
					"INSERT INTO kv(key, data, updated) VALUES (?, ?, unixepoch()) "
					"ON CONFLICT(key) DO UPDATE SET data=excluded.data, updated=unixepoch()",
					(serializedKey, data),
				)
				connection.execute(
					"INSERT INTO public_objects(scope, public_id, object_key) VALUES (?, ?, ?)",
					(scope, publicID, serializedKey),
				)
				if transactionStarted:
					connection.commit()
				else:
					connection.execute("RELEASE SAVEPOINT public_id_allocation")
				return publicID
			except Exception:
				if transactionStarted:
					connection.rollback()
				else:
					connection.execute("ROLLBACK TO SAVEPOINT public_id_allocation")
					connection.execute("RELEASE SAVEPOINT public_id_allocation")
				raise

	def getPublicObjectKey(self, scope, publicID):
		with self.lock:
			row = self._connection().execute(
				"SELECT object_key FROM public_objects WHERE scope = ? AND public_id = ?",
				(scope, int(publicID)),
			).fetchone()
		return None if row is None else self._deserialize(key=row[0])

	def removeObject(self, key):
		"""Atomically remove an object and its public-ID mapping."""
		serializedKey = self._serialize(key=key)
		with self.lock:
			connection = self._connection()
			transactionStarted = not connection.in_transaction
			try:
				if transactionStarted:
					connection.execute("BEGIN IMMEDIATE")
				else:
					connection.execute("SAVEPOINT public_id_removal")
				connection.execute(
					"DELETE FROM public_objects WHERE object_key = ?", (serializedKey,)
				)
				connection.execute("DELETE FROM kv WHERE key = ?", (serializedKey,))
				if transactionStarted:
					connection.commit()
				else:
					connection.execute("RELEASE SAVEPOINT public_id_removal")
			except Exception:
				if transactionStarted:
					connection.rollback()
				else:
					connection.execute("ROLLBACK TO SAVEPOINT public_id_removal")
					connection.execute("RELEASE SAVEPOINT public_id_removal")
				raise
		return self

	def close(self) -> bool:
		with self.lock:
			if self.connection is not None:
				self.connection.commit()
				self.connection.close()
				self.connection = None
				return True
			else:
				return False

	def _matchesPrefix(self, key, prefix):
		return self.matchesPrefix(key, prefix)

	def _rawKey(self, key, ext=None):
		return key + (ext or "")

	def _readRawData(self, data):
		if data is None:
			return None
		elif isinstance(data, bytes):
			return data
		elif isinstance(data, str):
			return data.encode("utf-8")
		elif isinstance(data, IOBase):
			return data.read()
		elif hasattr(data, "read"):
			return data.read()
		else:
			return bytes(data)

	def __del__(self):
		self.close()


class KVSqliteBackend(StorageBackend):
	"""Compatibility backend for the legacy BLOB-based KV SQLite schema."""

	DEFAULT_TABLE = "kv"
	VALUE_FORMAT = "bytes"

	def __init__(self, path: str, *, table: str = DEFAULT_TABLE, wal: bool = True):
		super().__init__()
		self.path = path if path.endswith(".sqlite3") else f"{path}.sqlite3"
		self.table = table
		self.wal = wal
		self._connection: sqlite3.Connection | None = None
		self._lock = threading.RLock()
		self._open()

	def _open(self):
		with self._lock:
			if self._connection is not None:
				return
			parent = os.path.dirname(os.path.abspath(self.path))
			if parent and not os.path.isdir(parent):
				raise RuntimeError("SQLite parent directory does not exist: %s" % parent)
			self._connection = sqlite3.connect(self.path)
			if self.wal:
				self._connection.execute("PRAGMA journal_mode=WAL")
				self._connection.execute("PRAGMA synchronous=NORMAL")
			self._connection.execute(
				"CREATE TABLE IF NOT EXISTS %s (key TEXT PRIMARY KEY, value BLOB)" % self.table
			)
			self._connection.execute(
				"CREATE TABLE IF NOT EXISTS metadata (key TEXT PRIMARY KEY, value TEXT)"
			)
			self._connection.commit()

	def _conn(self) -> sqlite3.Connection:
		if self._connection is None:
			self._open()
		assert self._connection is not None
		return self._connection

	def set(self, key, data):
		with self._lock:
			self._conn().execute(
				"INSERT INTO %s(key, value) VALUES (?, ?) "
				"ON CONFLICT(key) DO UPDATE SET value=excluded.value" % self.table,
				(key, data),
			)
			self._conn().commit()

	def add(self, key, data):
		return self.set(key, data)

	def update(self, key, data):
		return self.set(key, data)

	def remove(self, key):
		with self._lock:
			self._conn().execute("DELETE FROM %s WHERE key = ?" % self.table, (key,))
			self._conn().commit()

	def delete(self, key):
		return self.remove(key)

	def has(self, key):
		with self._lock:
			cursor = self._conn().execute("SELECT 1 FROM %s WHERE key = ?" % self.table, (key,))
			return cursor.fetchone() is not None

	def get(self, key):
		with self._lock:
			cursor = self._conn().execute("SELECT value FROM %s WHERE key = ?" % self.table, (key,))
			row = cursor.fetchone()
			return row[0] if row else None

	def keys(self, collection=None, order=StorageBackend.ORDER_NONE):
		query = "SELECT key FROM %s" % self.table
		if order == StorageBackend.ORDER_ASCENDING:
			query += " ORDER BY key ASC"
		elif order == StorageBackend.ORDER_DESCENDING:
			query += " ORDER BY key DESC"
		with self._lock:
			rows = list(self._conn().execute(query))
		prefix = collection[0] if isinstance(collection, (tuple, list)) and collection else collection
		for row in rows:
			if prefix is None or self.matchesPrefix(row[0], prefix):
				yield row[0]

	def count(self, key=None) -> int:
		if key is None:
			with self._lock:
				return self._conn().execute("SELECT count(*) FROM %s" % self.table).fetchone()[0]
		return len(tuple(self.keys(key)))

	def clear(self):
		with self._lock:
			self._conn().execute("DELETE FROM %s" % self.table)
			self._conn().execute("DELETE FROM metadata")
			self._conn().commit()

	def getMetadata(self, key=None, default=None):
		with self._lock:
			if key is None:
				rows = list(self._conn().execute("SELECT key, value FROM metadata"))
				return {row[0]: self._deserialize(data=row[1]) for row in rows}
			row = self._conn().execute("SELECT value FROM metadata WHERE key = ?", (key,)).fetchone()
			return default if row is None else self._deserialize(data=row[0])

	def setMetadata(self, key, value):
		with self._lock:
			self._conn().execute(
				"INSERT INTO metadata(key, value) VALUES (?, ?) "
				"ON CONFLICT(key) DO UPDATE SET value=excluded.value",
				(key, self._serialize(data=value)),
			)
			self._conn().commit()
		return value

	def removeMetadata(self, key):
		with self._lock:
			self._conn().execute("DELETE FROM metadata" if key is None else "DELETE FROM metadata WHERE key = ?", () if key is None else (key,))
			self._conn().commit()
		return self

	def close(self):
		with self._lock:
			if self._connection is not None:
				self._connection.commit()
				self._connection.close()
				self._connection = None


__all__ = [
	"SQLiteBackend",
	"KVSqliteBackend",
]


# EOF
