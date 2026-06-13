from . import StorageBackend

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
		if prefix is None:
			return True
		if isinstance(prefix, (tuple, list)):
			return any(self._matchesPrefix(key, _) for _ in prefix)
		return str(key).startswith(prefix)

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


# EOF
