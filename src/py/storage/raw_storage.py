"""Raw storage runtime implementation."""

import base64
import threading
import weakref

from .core import Storable, asPrimitive, getCanonicalName
from .raw_model import StoredRaw, _coerceCacheKey


class RawStorage:
	DATA_SUFFIX = ".data"
	META_SUFFIX = ".meta"

	def __init__(self, backend):
		"""Creates a new raw storage with the given backend."""
		self.backend = backend
		self._declaredClasses = {}
		self.lock = threading.RLock()
		self._cache = weakref.WeakValueDictionary()
		self._classPrefix = weakref.WeakKeyDictionary()

	def register(self, storedRaw, restored=False):
		"""Registers a raw value before it is persisted."""
		assert isinstance(storedRaw, StoredRaw), "Only stored raw can be registered"
		self.lock.acquire()
		key = storedRaw.id
		self._cache[key] = storedRaw
		self.lock.release()
		return self

	def use(self, *classes):
		"""Makes this storage register itself with the given classes."""
		for c in classes:
			self._declaredClasses[getCanonicalName(c)] = c
			assert c.STORAGE is None, "Storable already has a storage"
			c.STORAGE = self
			Storable.DeclareClass(c)
		return self

	def release(self):
		for k, c in list(self._declaredClasses.items()):
			c.STORAGE = None
		self._declaredClasses = {}

	def getStorageKeys(self, storedRawOrKey):
		if isinstance(storedRawOrKey, StoredRaw):
			key = storedRawOrKey.id
			prefix = self._classPrefix.get(storedRawOrKey.__class__)
			if not prefix:
				prefix = storedRawOrKey.__class__.__name__.split(".")[-1]
				self._classPrefix[storedRawOrKey.__class__] = prefix
			key = str(prefix) + "." + str(storedRawOrKey.id)
		else:
			key = storedRawOrKey
		return key + self.META_SUFFIX, key + self.DATA_SUFFIX

	def add(self, storedRaw, update=False):
		key_meta, key_data = self.getStorageKeys(storedRaw)
		assert storedRaw.hasStorage()
		assert storedRaw.STORAGE == self, "StoredRaw stored in a different storage"
		assert storedRaw.id in self._cache, "StoredRaw should be already in cache"
		if not update:
			self.backend.add(key_meta, self.serializeMeta(storedRaw.export()))
		else:
			self.backend.update(key_meta, self.serializeMeta(storedRaw.export()))
		if storedRaw.hasDataChanged():
			self.backend.saveRawData(key_data, storedRaw._data)
			storedRaw.setDataSaved()

	def update(self, storedRaw):
		return self.add(storedRaw, update=True)

	def restore(self, meta=None, data=None):
		if isinstance(meta, StoredRaw):
			meta.setStorage(self)
			return meta
		if meta["id"] in self._cache:
			res = self._cache[meta["id"]]
			res.meta(meta)
		else:
			raw_class = self._declaredClasses.get(meta.get("type")) or RawStorage
			data = meta.get("data")
			if data:
				data = base64.b64decode(data)
			res = raw_class(
				data,
				restored=True,
				**dict(((k, v) for k, v in list(meta.items()) if k != "data")),
			)
		if isinstance(res, StoredRaw):
			res.setStorage(self)
		return res

	def get(self, keyOrStoredRaw):
		cache_key = _coerceCacheKey(keyOrStoredRaw)
		if cache_key in self._cache:
			return self._cache[cache_key]
		key_meta, key_data = self.getStorageKeys(keyOrStoredRaw)
		if self.backend.has(key_data) or self.backend.has(key_meta):
			meta = self.deserializeMeta(self.backend.get(key_meta))
			raw_object = self.restore(meta=meta)
			raw_object._hasDataChanged = False
			return raw_object
		return None

	def has(self, keyOrStoredRaw):
		cache_key = _coerceCacheKey(keyOrStoredRaw)
		if cache_key in self._cache:
			return self._cache[cache_key]
		key_meta, key_data = self.getStorageKeys(keyOrStoredRaw)
		return self.backend.has(key_data) or self.backend.has(key_meta)

	def remove(self, keyOrStoredRaw):
		key_meta, key_data = self.getStorageKeys(keyOrStoredRaw)
		self.backend.remove(key_meta)
		self.backend.remove(key_data)
		cache_key = _coerceCacheKey(keyOrStoredRaw)
		if cache_key in self._cache:
			del self._cache[cache_key]
		return self

	def sync(self):
		self.backend.sync()

	def query(self, keyOrStoredRaw=None, timestamp=None):
		assert timestamp is None, "Timestamp not supported yet"
		raise NotImplementedError

	def keys(self, types=None):
		prefix = self._getStoragePrefix(types)
		for key in self.backend.keys(prefix):
			yield key

	def list(self, count=-1, start=0, end=None, types=None):
		end = end if end >= 0 else (start + count if count > 0 else None)
		i = 0
		if types and type(types) not in (list, tuple):
			types = (types,)
		data_suffix_len = len(self.DATA_SUFFIX)
		meta_suffix_len = len(self.META_SUFFIX)
		previous_key = None
		for key in self.keys(types):
			if count != 0 and (i >= start and (i < end or end is None)):
				if key.endswith(self.DATA_SUFFIX):
					key = key[:-data_suffix_len]
				elif key.endswith(self.META_SUFFIX):
					key = key[:-meta_suffix_len]
				else:
					continue
				s = self.get(key)
				if not s:
					continue
				if not types or s.__class__ in types:
					if s != previous_key:
						yield s
					previous_key = s
			i += 1

	def count(self, types=None):
		return len(list(self.list(types=types)))

	def path(self, storedRaw):
		"""Gets the physical path of the raw data."""
		key_meta, key_data = self.getStorageKeys(storedRaw)
		return self.backend.getRawDataPath(key_data)

	def streamData(self, storedRaw, size=None):
		"""Streams raw data from the storage backend."""
		key_meta, key_data = self.getStorageKeys(storedRaw)
		for chunk in self.backend.streamRawData(key_data, size=None):
			yield chunk

	def serializeMeta(self, meta):
		return asPrimitive(meta)

	def deserializeMeta(self, meta):
		return meta

	def _getStoragePrefix(self, types=None):
		"""Returns prefixes used to store the requested raw classes."""
		prefix = None
		if types:
			if type(types) not in (tuple, list):
				types = (types,)
			prefix = [_.StoragePrefix() for _ in types]
		return prefix


__all__ = ["RawStorage"]
