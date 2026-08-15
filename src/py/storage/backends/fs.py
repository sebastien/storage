from .base import StorageBackend
from ..core import NOTHING, Operation
from io import IOBase
from typing import Iterator
import os
import shutil
import base64
import json

# -----------------------------------------------------------------------------
#
# DIRECTORY BACKEND
#
# -----------------------------------------------------------------------------


# TODO: This does not detect local changes to the file
# TODO: Should add a backend that caches,
class DirectoryBackend(StorageBackend):
	"""A backend that stores the values as files with the given `DATA_EXTENSION`
	the `keyToPath` and `pathToKey` functions take care of translating the
	keys to specific file system paths, allowing to write custom path-mapping
	schemes."""

	HAS_FILE: bool = True
	HAS_STREAM: bool = True
	HAS_RAW: bool = True
	DATA_EXTENSION: str = ".json"
	RAW_EXTENSION: str = ".blob"
	DEFAULT_STREAM_SIZE: int = 1024 * 100

	def __init__(
		self,
		root,
		pathToKey=None,
		keyToPath=None,
		writer=None,
		reader=None,
		extension=None,
	):
		super().__init__()
		if not root.endswith("/"):
			root += "/"
		self.root = root
		# FIXME: This should be redefined
		self.keyToPath = keyToPath or self._defaultKeyToPath
		self.pathToKey = pathToKey or self._defaultPathToKey
		self.writer = writer or self._defaultWriter
		self.reader = reader or self._defaultReader
		if extension is not None:
			self.DATA_EXTENSION = extension
		parent_dir = os.path.dirname(os.path.abspath(self.root))
		os.makedirs(parent_dir, exist_ok=True)
		os.makedirs(self.root, exist_ok=True)

	# =========================================================================
	# BACKEND METHODS
	# =========================================================================

	def keys(self, prefix=None, order=StorageBackend.ORDER_NONE):
		"""Iterates through all (or the given subset) of keys in this storage."""
		if (
			order == StorageBackend.ORDER_ASCENDING
			or order == StorageBackend.ORDER_DESCENDING
		):
			for k in sorted(
				self.keys(prefix), reverse=order == StorageBackend.ORDER_DESCENDING
			):
				yield k
		else:
			assert not prefix or type(prefix) in (str, str) or len(prefix) == 1, (
				"Multiple prefixes not supported yet: {0}".format(prefix)
			)
			if prefix and type(prefix) in (tuple, list):
				prefix = prefix[0]
			ext_len = len(self.DATA_EXTENSION)
			if not prefix:
				prefix_path = self.root
			else:
				prefix_path = self.path(prefix or "")
				if ext_len:
					prefix_path = prefix_path[:-ext_len]
			for root, dirnames, filenames in os.walk(self.root):
				for f in filenames:
					if not f.endswith(self.DATA_EXTENSION):
						continue
					path = root + os.sep + f
					key = self.pathToKey(self, path)
					if prefix and not key.startswith(prefix):
						continue
					yield key

	def count(self, prefix=None):
		"""Returns the numbers of keys that match the given prefix(es)"""
		return len(tuple(self.keys(prefix)))

	def clear(self):
		for key in list(self.keys()):
			self.remove(key)
		metadata_path = self._metadataPath()
		if os.path.exists(metadata_path):
			os.unlink(metadata_path)
		self._cleanupEmptyParents(metadata_path)

	def list(self, key=None):
		assert key is None, "Not implemented"
		for storageKey in self.keys():
			yield self.get(storageKey)

	def add(self, key, data):
		"""Adds the given data to the storage."""
		self.writer(self, Operation.ADD, self.path(key), self._serialize(data=data))

	def update(self, key, data):
		"""Updates the given data to the storage."""
		self.writer(self, Operation.UPDATE, self.path(key), self._serialize(data=data))

	def get(self, key):
		"""Gets the value associated with the given key in the storage."""
		data = self.reader(self, self.path(key=key))
		return self._deserialize(data=data) if data is not None else None

	def has(self, key):
		return os.path.exists(self.path(key))

	def remove(self, key):
		"""Removes the given value from the storage. This will remove the
		given file and remove the parent directory if it's empty."""
		# FIXME: This works for objects and raw, not so much for metrics
		path = self.keyToPath(self, key)
		if os.path.exists(path):
			os.unlink(path)
		self._cleanupEmptyParents(path)
		return self

	def sync(self):
		"""This backend sync at each operation, so if you want to
		buffer operation, use a cached backend."""

	def path(self, key, ext=None):
		return self.keyToPath(self, key, ext)

	def getFileName(self, key):
		path = self.path(key)
		return path if os.path.exists(path) else None

	def stream(self, key, size=None) -> Iterator[bytes]:
		# FIXME: Hope this does not leak
		with open(self.path(key), "rb") as f:
			while True:
				d = f.read(size or self.DEFAULT_STREAM_SIZE)
				if d:
					yield d
				else:
					break

	# FIXME: Not sure if this should be merges as get/set/stream/path
	def hasRawData(self, key, ext=RAW_EXTENSION):
		return os.path.exists(self.path(key, ext=ext))

	def saveRawData(self, key, data, ext=RAW_EXTENSION):
		self.writer(self, Operation.SAVE_RAW, self.path(key, ext=ext), data)

	def loadRawData(self, key, data, ext=RAW_EXTENSION):
		return self.reader(self, self.path(key=key, ext=ext))

	def streamRawData(self, key, size=None, ext=RAW_EXTENSION):
		# FIXME: Hope this does not leak
		path = self.path(key, ext=ext)
		if os.path.exists(path):
			with open(path, "rb") as f:
				while True:
					d = f.read(size or self.DEFAULT_STREAM_SIZE)
					if d:
						yield d
					else:
						break
		else:
			yield None

	def getRawDataPath(self, key, ext=RAW_EXTENSION):
		return self.path(key, ext=ext)

	def queryMetrics(self, name=None, timestamp=None):
		return []

	def listMetrics(self):
		"""Lists the metrics available in this backend"""
		return []

	def getMetadata(self, key=None, default=None):
		metadata = self._readMetadata()
		if key is None:
			return metadata
		return metadata.get(key, default)

	def setMetadata(self, key, value):
		metadata = self._readMetadata()
		metadata[key] = value
		self._writeMetadata(metadata)
		return value

	def removeMetadata(self, key):
		metadata = self._readMetadata()
		if key in metadata:
			del metadata[key]
			self._writeMetadata(metadata)
		return self

	def _serialize(self, key=NOTHING, data=NOTHING):
		"""Serializing the key means converting the key to a path."""
		if key is NOTHING:
			return StorageBackend._serialize(self, data=data)
		elif data is NOTHING:
			raise Exception("Serialize key should not be used, use `path()` instead.")
		else:
			raise Exception("Serialize key should not be used, use `path()` instead.")

	# =========================================================================
	# FILE I/O
	# =========================================================================

	def appendFile(self, path, data):
		handle = self._getWriteFileHandle(
			path, mode="ab" if isinstance(data, bytes) else "at"
		)
		handle.write(data)
		self._closeFileHandle(handle)
		return True

	def writeFile(self, path: str, data: str | bytes | IOBase | None) -> bool:
		# In case we're given None as data, we don't create the file
		if data is None:
			return True
		isBinary = isinstance(data, bytes)
		if isinstance(data, IOBase):
			isBinary = isinstance(data.read(0), bytes)
		handle = self._getWriteFileHandle(path, mode="wb" if isBinary else "wt")
		if isinstance(data, IOBase) or isinstance(data, IOBase):
			try:
				shutil.copyfileobj(data, handle)
				self._closeFileHandle(handle)
			except Exception:
				self._closeFileHandle(handle)
				os.unlink(path)
				raise
			return True
		else:
			try:
				handle.write(data)
				self._closeFileHandle(handle)
			except Exception:
				self._closeFileHandle(handle)
				os.unlink(path)
				raise
		return True

	def readFile(self, path: str) -> bytes | None:
		handle = self._getReadFileHandle(path, mode="rb")
		if handle:
			data = handle.read()
			self._closeFileHandle(handle)
			return data
		else:
			return None

	# =========================================================================
	# INTERNALS (FILE MANIPULATION)
	# =========================================================================

	def _defaultKeyToPath(self, backend, key, ext=None):
		"""Converts the given key to the given path."""
		return self.root + key.replace(".", "/") + (ext or self.DATA_EXTENSION)

	def _defaultPathToKey(self, backend, path, ext=None):
		ext = ext or self.DATA_EXTENSION
		res = os.path.relpath(path, self.root)
		if ext:
			res = res[: -len(ext)]
		else:
			res = res
		return res.replace(os.sep, ".")

	def _defaultWriter(self, backend, operation, path, data):
		"""Writes the given operation on the storable with the given key and data"""
		return self.writeFile(path, data)

	def _defaultReader(self, backend, path):
		"""Returns the value that is stored in the given backend at the given
		key."""
		return self.readFile(path)

	def _metadataPath(self):
		return os.path.join(self.root, ".storage", "metadata.json")

	def _readMetadata(self):
		path = self._metadataPath()
		if not os.path.exists(path):
			return {}
		with open(path, "rt") as f:
			return json.load(f)

	def _writeMetadata(self, metadata):
		path = self._metadataPath()
		parent = os.path.dirname(path)
		if parent and not os.path.exists(parent):
			os.makedirs(parent)
		with open(path, "wt") as f:
			json.dump(metadata, f)

	def _getWriteFileHandle(self, path, mode="ab"):
		parent = os.path.dirname(path)
		if parent and not os.path.exists(parent):
			os.makedirs(parent)
		return open(path, mode)

	def _getReadFileHandle(self, path, mode="rb"):
		if os.path.exists(path):
			return open(path, mode)
		else:
			return None

	def _closeFileHandle(self, handle):
		handle.close()

	def _cleanupEmptyParents(self, path):
		root = self.root.rstrip(os.sep)
		parent = os.path.dirname(path)
		while parent and parent != root and os.path.exists(parent):
			if os.listdir(parent):
				break
			os.rmdir(parent)
			parent = os.path.dirname(parent)


class KVFileBackend(DirectoryBackend):
	"""Compatibility adapter for the general directory backend."""

	CODEC_PREFIX = "storage.codec:"

	def __init__(self, root: str, *, ext: str = ".kv"):
		super().__init__(
			root,
			keyToPath=self._keyToPath,
			pathToKey=self._pathToKey,
			extension=ext,
		)

	@staticmethod
	def _keyToPath(backend, key, ext=None):
		name = base64.urlsafe_b64encode(key.encode("utf-8")).decode("ascii")
		return os.path.join(backend.root, name + (ext or backend.DATA_EXTENSION))

	@staticmethod
	def _pathToKey(backend, path):
		name = os.path.basename(path)
		if backend.DATA_EXTENSION and name.endswith(backend.DATA_EXTENSION):
			name = name[: -len(backend.DATA_EXTENSION)]
		return base64.urlsafe_b64decode(name).decode("utf-8")

	def get(self, key):
		"""Reads either the new bridged envelope or legacy raw codec bytes."""
		path = self.path(key)
		if not os.path.exists(path):
			return None
		with open(path, "rb") as source:
			raw = source.read()
		try:
			value = self._deserialize(data=raw)
		except Exception:
			return raw
		return value if isinstance(value, str) and value.startswith(self.CODEC_PREFIX) else raw


__all__ = [
	"DirectoryBackend",
	"KVFileBackend",
]


# EOF
