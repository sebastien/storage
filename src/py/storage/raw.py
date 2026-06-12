"""Raw binary storage model and runtime.

This module groups two closely related concerns:

- `StoredRaw` for raw payloads plus metadata
- `RawStorage` for persisting metadata and data streams
"""

import base64
import io
import threading
import weakref

from .core import (
	Identifier,
	NOTHING,
	Storable,
	asPrimitive,
	getCanonicalName,
	getTimestamp,
)

# FIXME: Should be much closer to stored object, with a similar API. meta
#        should never allow to store RESERVED PROPERTIES.

# TODO: There should be a backend that stores data so that the same file
#       uploaded multiple times would not be stored in two separate file. It
#       should be something that would have an data store and then manage symlinks.
# TODO: Add sync queue, review caching mechanism
# TODO: Add revision/update meta-data
# TODO: Add properties (for stored object compatibility)

# FIXME: This module is still a bit sketchy... make sure:
# 1 - Storage accepts file as well as string
# 2 - Makes sure that raw data is only accessible through file, because files
#     can be very large

__pychecker__ = "unusednames=options"

# -----------------------------------------------------------------------------
#
# STORAGE UTILITIES
#
# -----------------------------------------------------------------------------


def _coerceCacheKey(rawOrKey):
	return rawOrKey.id if isinstance(rawOrKey, StoredRaw) else rawOrKey


# -----------------------------------------------------------------------------
#
# STORED RAW MODEL
#
# -----------------------------------------------------------------------------

# TODO: Raw should support variants (with different formats), each variant
# should have one specific meta that overoides the whole thing
# Raw
# - meta
# - variants:
#   - data, meta
#   - data, meta
#   - data, meta
# The default data is the first variant


class StoredRaw(Storable):
	ID_GENERATOR = Identifier.Stamp
	ID_PREFIX = None
	RESERVED = ("type", "id", "revision", "updates")
	COLLECTION = None
	STORAGE = None

	@classmethod
	def _ensureStorage(cls):
		assert cls.STORAGE, (
			"Class has not been registered in an RawStorage yet: %s" % (cls)
		)
		return cls.STORAGE

	@classmethod
	def GenerateID(cls):
		id = cls.ID_GENERATOR()
		return f"{cls.ID_PREFIX}-{id}" if cls.ID_PREFIX else id

	@classmethod
	def Import(cls, meta, data=None, updateProperties=False):
		if isinstance(meta, StoredRaw):
			assert data is None, (
				"A StoredRaw was given as first argument, but data was given as well"
			)
			assert isinstance(meta, cls), "Expected class %s, got %s" % (
				cls,
				meta.__class__,
			)
			return meta
		else:
			id = meta.get("id")
			# We makre sure to extract the data and remove it from meta if provided
			# (which might happend in the stored raw is exported with its data)
			data = meta.get("data") if data is None else None
			if "data" in meta:
				data = base64.b64decode(data)
				meta = dict(((k, v) for k, v in list(meta.items()) if k != "data"))
			# If there is an object ID (and we're supposed to get it)
			if id:
				# We look in the storage for this specific object
				obj = cls.Get(id)
				# If it exists, we update its properties
				if obj:
					# FIXME: I don't see the use case for an `updateProperties`, but am
					# leaving here as an option. The default behaviour is that we should
					# only update the properties if the object does not exist in the
					# storage. If it does, then we assume the storage's version is the
					# most up to date.if updateProperties:
					for key, value in list(meta.items()):
						# NOTE: We manage reserved properites at this level
						if key == "type" or key == "id":
							continue
						elif key in ("revision", "updates"):
							obj._revision = value
						else:
							obj.meta(key, value)
					return obj
				# Otherwise we create a new one
				else:
					return cls(data, **(meta or {}))
			else:
				return cls(data, **(meta or {}))

	@classmethod
	def Has(cls, id):
		if id is None:
			return False
		return cls._ensureStorage().has(cls.StorageKey(id))

	@classmethod
	def Get(cls, id):
		if id is None:
			return None
		return cls._ensureStorage().get(cls.StorageKey(id))

	@classmethod
	def Ensure(cls, id):
		res = cls.Get(id)
		if res is None:
			res = cls(id=id)
		return res

	@classmethod
	def Count(cls):
		return cls._ensureStorage().count(cls)

	@classmethod
	def List(cls, count=-1, start=0, end=None):
		return cls._ensureStorage().list(count, start, end, types=cls)

	@classmethod
	def All(cls, count=-1, start=0, end=None):
		return cls.List()

	@classmethod
	def StorageKey(cls, id):
		"""Returns the storage key associated with the given id of this class."""
		if isinstance(id, StoredRaw):
			id = id.id
		if cls.COLLECTION:
			return str(cls.COLLECTION) + "." + str(id)
		else:
			cls.COLLECTION = cls.__name__.split(".")[-1]
			return cls.StorageKey(id)

	@classmethod
	def StoragePrefix(cls):
		"""Returns the storage prefix for storage keys of objects of this
		class."""
		if not cls.COLLECTION:
			cls.COLLECTION = cls.__name__.split(".")[-1]
		return cls.COLLECTION

	def __init__(self, data=None, restored=False, **meta):
		if "id" in meta:
			self.id = meta["id"]
		else:
			self.id = self.GenerateID()
		self._meta = {}
		self._hasDataChanged = True
		self._data = data
		self._revision = {}
		if data is None:
			self._hasDataChanged = False
		for k in meta:
			if k not in self.RESERVED:
				self._meta[k] = meta[k]
		if meta:
			self._revision.update(meta.get("revision") or meta.get("updates") or {})
		if "id" not in self._revision:
			self._revision["id"] = getTimestamp()
		if self.STORAGE:
			self.STORAGE.register(self, restored=restored)

	def remove(self):
		self.STORAGE.remove(self)
		return self

	def setStorage(self, storage):
		assert self.STORAGE is None or self.STORAGE == storage, (
			"StoredRaw already have a different storage assigned"
		)
		self.STORAGE = storage

	def hasStorage(self):
		return self.STORAGE is not None

	def hasDataChanged(self):
		return self._hasDataChanged

	def setDataSaved(self):
		if hasattr(self._data, "close"):
			# We force closing file-like objects once the backend has persisted them.
			try:
				self._data.close()
			except Exception:
				pass
			self._data = None
		self._hasDataChanged = False

	def setData(self, data, timestamp=None):
		self._data = data
		self._hasDataChanged = True
		# FIXME: Not sure what to do with timestamp
		self._revision["data"] = self._revision["id"] = max(
			getTimestamp() if timestamp is None else timestamp,
			self._revision.get("data", -1),
		)
		return self

	def setMeta(self, meta=NOTHING, **options):
		if meta is not NOTHING:
			assert type(meta) is dict, "StoredRaw.setMeta only accepts dict"
			self._meta = meta
		for k in options:
			if k not in self.RESERVED:
				self._meta[k] = options[k]
		# FIXME: Not sure what to do with timestamp
		timestamp = None
		self._revision["meta"] = self._revision["id"] = max(
			getTimestamp() if timestamp is None else timestamp,
			self._revision.get("meta", -1),
		)
		return self

	def clearMeta(self):
		self.meta = {}
		return self

	def getUpdateTime(self, key="id"):
		"""Returns the time at with the given object (or key) was updated. The time
		is returned as a storage timestamp."""
		if self._revision:
			return self._revision.get(key, 0)
		else:
			return 0

	def update(self, propertiesAndRelations):
		"""Compatiblity method for Storable, is an alias to setMeta"""
		return self.setMeta(propertiesAndRelations)

	def meta(self, name=NOTHING, value=NOTHING, **options):
		"""Returns the meta data"""
		# if NAME is not nothing and is dict, we should replace
		if name is NOTHING:
			if options:
				for key in options:
					if key not in self.RESERVED:
						self._meta[key] = options[key]
				return self
			else:
				return self._meta
		else:
			if value is NOTHING:
				if isinstance(name, dict):
					for _ in name:
						if _ not in self.RESERVED:
							self._meta[_] = name[_]
				else:
					return self._meta.get(name)
			else:
				assert name not in self.RESERVED, "Reserved meta property: {0}".format(
					name
				)
				self._meta[name] = value
				return self

	def data(self, size=None):
		"""Iterates through the data with chunks of the given size."""
		if self._data:
			yield self._data
		elif self.STORAGE:
			for _ in self.STORAGE.streamData(self, size=None):
				yield _

	def loadData(self):
		# FIXME: This is highly inefficient, but useful for debugging.
		v = io.BytesIO()
		for i, d in enumerate(self.data()):
			if d is not None:
				v.write(d)
		return v.getvalue()

	def path(self):
		"""Returns the path of the data file."""
		if self.STORAGE:
			return self.STORAGE.path(self)
		else:
			raise Exception("No storage attached to stored raw: {0}".format(self))

	def length(self):
		if self._data:
			return len(self._data)
		else:
			return None

	def export(self, **options):
		depth = 1
		if "depth" in options:
			depth = options["depth"]
		# SEE: http://stackoverflow.com/questions/1379934/large-numbers-erroneously-rounded-in-javascript
		# We cannot allow IDs to be long numbers...
		res = dict(
			id=str(self.id),
			type=self.getTypeName(),
			revision=self._revision,
		)
		if depth > 0:
			res.update(self._meta)
		# NOTE: This is just for data export/synchronization. It's not recommanded
		# for big files (where rsync is probably better)
		if options.get("data"):
			res["data"] = base64.b64encode(self.loadData())
		return res

	def getTypeName(self):
		return getCanonicalName(self.__class__)

	def save(self):
		self._ensureStorage().update(self)

	def __repr__(self):
		return "<raw:%s %s:%s>" % (self.__class__.__name__, id(self), self.id)


# -----------------------------------------------------------------------------
#
# RAW STORAGE RUNTIME
#
# -----------------------------------------------------------------------------


class RawStorage:
	DATA_SUFFIX = ".data"
	META_SUFFIX = ".meta"

	def __init__(self, backend):
		"""Creates a new metric storage with the given backend"""
		self.backend = backend
		self._declaredClasses = {}
		self.lock = threading.RLock()
		self._cache = weakref.WeakValueDictionary()
		self._classPrefix = weakref.WeakKeyDictionary()

	def register(self, storedRaw, restored=False):
		"""Registers a new StoredRaw in this storage. This allows a get()
		to be successful, even before the object is actually stored in the db."""
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
		key_data = key + self.DATA_SUFFIX
		key_meta = key + self.META_SUFFIX
		return key_meta, key_data

	def add(self, storedRaw, update=False):
		key_meta, key_data = self.getStorageKeys(storedRaw)
		assert storedRaw.hasStorage()
		assert storedRaw.STORAGE == self, "StoredRaw stored in a different storage"
		assert storedRaw.id in self._cache, "StoredRaw should be already in cache"
		# NOTE: We MUST make sure that there is something saved for the key_meta,
		# event when there's no meta data, as otherwise the object won't appear
		# as saved.
		if not update:
			self.backend.add(key_meta, self.serializeMeta(storedRaw.export()))
		else:
			self.backend.update(key_meta, self.serializeMeta(storedRaw.export()))
		# We only store the data if it has changed
		if storedRaw.hasDataChanged():
			self.backend.saveRawData(key_data, storedRaw._data)
			storedRaw.setDataSaved()

	def update(self, storedRaw):
		# Right now, we don't need anything different than add
		return self.add(storedRaw, update=True)

	def restore(self, meta=None, data=None):
		if isinstance(meta, StoredRaw):
			# in most cases, unJSON will properly deserialize the JSON
			# into a StoredRaw, so we just add the storage
			meta.setStorage(self)
			return meta
		else:
			if meta["id"] in self._cache:
				res = self._cache[meta["id"]]
				# FIXME: This should be a merge, as we don't know for sure which
				# version is the most up-to-date
				res.meta(meta)
			else:
				# FIXME: This should probably use Import
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
		# We look in the cache first
		if cache_key in self._cache:
			return self._cache[cache_key]
		# Or we restore the raw object
		else:
			key_meta, key_data = self.getStorageKeys(keyOrStoredRaw)
			# print "GET IMAGE", keyOrStoredRaw
			# print key_meta, key_data
			# print "+==="
			if self.backend.has(key_data) or self.backend.has(key_meta):
				# We don't deserialize the data as it might be too big
				meta = self.deserializeMeta(self.backend.get(key_meta))
				raw_object = self.restore(meta=meta)
				# We ensure that the object is set to have no data change
				raw_object._hasDataChanged = False
				return raw_object
			else:
				return None

	def has(self, keyOrStoredRaw):
		cache_key = _coerceCacheKey(keyOrStoredRaw)
		# We look in the cache first
		if cache_key in self._cache:
			return self._cache[cache_key]
		else:
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
		"""Explicitely ask the back-end to synchronize. Depending on the
		back-end this might be a long or short, blocking or async
		operation."""
		self.backend.sync()

	def query(self, keyOrStoredRaw=None, timestamp=None):
		assert timestamp is None, "Timestamp not supported yet"
		raise NotImplementedError
		# for k in self.backend(keys):
		# 	key_meta, key_data = self.getStorageKeys(keyOrStoredRaw)
		# 	if self.backend.has(key_meta):
		# 		meta = self.deserializeMeta(self.backend.get(key_meta)),
		# 		dataless_raw = self.restore(meta=meta)
		# 		if dataless_raw.id == id:
		# 			yield dataless_raw

	def keys(self, types=None):
		prefix = self._getStoragePrefix(types)
		for key in self.backend.keys(prefix):
			yield key

	def list(self, count=-1, start=0, end=None, types=None):
		end = end if end >= 0 else (start + count if count > 0 else None)
		i = 0
		if types and type(types) not in (list, tuple):
			types = (types,)
		# FIXME: Should be updated according to raw storage
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
					# We make sure not to issue the same key twice, as in the
					# case where there is both a meta and a data file, there might
					# be two matches for the key
					if s != previous_key:
						yield s
					previous_key = s
			i += 1

	def count(self, types=None):
		return len(list(self.list(types=types)))

	def path(self, storedRaw):
		"""Gets the physical path (on the file system) of the data for
		this stored raw. This might generate an exception if the backend
		does not support it."""
		key_meta, key_data = self.getStorageKeys(storedRaw)
		return self.backend.getRawDataPath(key_data)

	def streamData(self, storedRaw, size=None):
		"""Streams the data from the storage -- this might generate an
		exception, as not all storage support streaming."""
		key_meta, key_data = self.getStorageKeys(storedRaw)
		for chunk in self.backend.streamRawData(key_data, size=None):
			yield chunk

	def serializeMeta(self, meta):
		return asPrimitive(meta)

	def deserializeMeta(self, meta):
		return meta

	# FIXME: Same as StoredObject._getStoragePrefix
	def _getStoragePrefix(self, types=None):
		"""Returns the list of prefixes for keys that are used to store objects
		of the given classes."""
		prefix = None
		if types:
			if type(types) not in (tuple, list):
				types = (types,)
			prefix = [_.StoragePrefix() for _ in types]
		return prefix


# -----------------------------------------------------------------------------
#
# PUBLIC API
#
# -----------------------------------------------------------------------------

__all__ = [
	"RawStorage",
	"StoredRaw",
]


# EOF - vim: tw=80 ts=4 sw=4 noet
