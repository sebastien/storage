from . import StorageBackend
from ..core import NOTHING, Operation
from io import IOBase
from typing import Iterator
import os
import shutil

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
		if extension != None:
			self.DATA_EXTENSION = extension
		parent_dir = os.path.dirname(os.path.abspath(self.root))
		assert os.path.isdir(parent_dir), (
			"DirectoryBacked root parent does not exists: %s" % (parent_dir)
		)
		if not os.path.isdir(self.root):
			os.mkdir(self.root)

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
		parent = os.path.dirname(path)
		if parent != self.root:
			if os.path.exists(parent):
				if not os.listdir(parent):
					os.rmdir(parent)
		return self

	def sync(self):
		"""This backend sync at each operation, so if you want to
		buffer operation, use a cached backend."""

	def path(self, key, ext=None):
		return self.keyToPath(self, key, ext)

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
		handle = self._getWriteFileHandle(path, mode="ab")
		handle.write(data)
		self._closeFileHandle(handle)
		return True

	def writeFile(self, path: str, data: str | bytes | IOBase | None) -> bool:
		# In case we're given None as data, we don't create the file
		if data is None:
			return True
		handle = self._getWriteFileHandle(
			path, mode="wb" if isinstance(data, bytes) else "wt"
		)
		if isinstance(data, IOBase) or isinstance(data, IOBase):
			try:
				shutil.copyfileobj(data, handle)
				self._closeFileHandle(handle)
			except Exception as e:
				self._closeFileHandle(handle)
				os.unlink(path)
				raise e
			return True
		else:
			try:
				handle.write(data)
				self._closeFileHandle(handle)
			except Exception as e:
				self._closeFileHandle(handle)
				os.unlink(path)
				raise e
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
		res = path.replace("/", ".")
		ext = ext or self.DATA_EXTENSION
		if ext:
			return res[len(self.root) : -len(ext)]
		else:
			return res[len(self.root) :]

	def _defaultWriter(self, backend, operation, path, data):
		"""Writes the given operation on the storable with the given key and data"""
		return self.writeFile(path, data)

	def _defaultReader(self, backend, path):
		"""Returns the value that is stored in the given backend at the given
		key."""
		return self.readFile(path)

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
