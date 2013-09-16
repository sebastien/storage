# -----------------------------------------------------------------------------
# Project   : FFCTN/Storage
# -----------------------------------------------------------------------------
# Author    : Sebastien Pierre                            <sebastien@ffctn.com>
# License   : BSD License
# -----------------------------------------------------------------------------
# Creation  : 07-Aug-2012
# Last mod  : 16-Sep-2013
# -----------------------------------------------------------------------------

import types, weakref, threading
from   storage import Storable, Identifier, getCanonicalName, getTimestamp, asJSON, unJSON, NOTHING

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

__doc__ = """\
Allows to store raw data and its meta-information
"""

# -----------------------------------------------------------------------------
#
# STORED RAW
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

	OID_GENERATOR = Identifier.Stamp
	RESERVED      = ("type", "timestamp", "oid")
	COLLECTION    = None
	STORAGE       = None

	@classmethod
	def GenerateOID( cls ):
		return cls.OID_GENERATOR()

	@classmethod
	def Import( cls, meta, data=None ):
		if isinstance(meta, StoredRaw):
			assert data is None, "A StoredRaw was given as first argument, but data was given as well"
			assert isinstance(meta, cls), "Expected class %s, got %s" % (cls, properties.__class__)
			return meta
		else:
			oid   = meta.get("oid")
			# If there is an object ID (and we're supposed to get it)
			if oid:
				# We look in the storage for this specific object
				obj = cls.Get(oid)
				# If it exists, we update its properties
				if obj:
					obj.meta(meta)
					return obj
				# Otherwise we create a new one
				else:
					return cls(data, **(meta or {}))
			else:
				return cls(data,  **(meta or {}))

	@classmethod
	def Has( cls, oid ):
		assert cls.STORAGE, "Class has not been registered in an RawStorage yet: %s" % (cls)
		if oid is None: return False
		return cls.STORAGE.has(cls.StorageKey(oid))

	@classmethod
	def Get( cls, oid ):
		assert cls.STORAGE, "Class has not been registered in an RawStorage yet: %s" % (cls)
		if oid is None: return None
		return cls.STORAGE.get(cls.StorageKey(oid))

	@classmethod
	def Ensure( cls, oid ):
		res = cls.Get(oid)
		if res is None:
			res  = cls(oid=oid)
		return res

	@classmethod
	def Count( cls ):
		assert cls.STORAGE, "Class has not been registered in an ObjectStorage yet: %s" % (cls)
		return cls.STORAGE.count(cls)

	@classmethod
	def List( cls, count=-1, start=0, end=None ):
		return cls.STORAGE.list(count, start, end, types=cls)

	@classmethod
	def All( cls, count=-1, start=0, end=None ):
		return cls.List()

	@classmethod
	def StorageKey( cls, oid ):
		"""Returns the storage key associated with the given oid of this class."""
		if isinstance(oid, StoredRaw): oid = oid.oid
		if cls.COLLECTION:
			return str(cls.COLLECTION) + "." +  str(oid)
		else:
			cls.COLLECTION = cls.__name__.split(".")[-1]
			return cls.StorageKey(oid)

	@classmethod
	def StoragePrefix( cls ):
		"""Returns the storage prefix for storage keys of objects of this
		class."""
		if not cls.COLLECTION:
			cls.COLLECTION = cls.__name__.split(".")[-1]
		return cls.COLLECTION

	def __init__( self, data=None, restored=False, **meta ):
		if meta.has_key("timestamp"): self.timestamp = meta["timestamp"]
		else: self.timestamp = getTimestamp()
		if meta.has_key("oid"): self.oid = meta["oid"]
		else: self.oid       = StoredRaw.GenerateOID()
		self._meta           = {}
		self._hasDataChanged = True
		self._data           = data
		if data is None: self._hasDataChanged = False
		for k in meta:
			if k not in self.RESERVED:
				self._meta[k] = meta[k]
		if self.STORAGE: self.STORAGE.register(self, restored=restored)

	def getID( self ):
		return self.oid

	def remove( self ):
		self.STORAGE.remove(self)
		return self

	def setStorage( self, storage ):
		assert self.STORAGE is None or self.STORAGE == storage, "StoredRaw already have a different storage assigned"
		self.STORAGE = storage

	def hasStorage( self ):
		return self.STORAGE is not None

	def hasDataChanged( self ):
		return self._hasDataChanged

	def setDataSaved( self ):
		if type(self.data) is types.FileType:
			# We force closing the file and put the data to None
			try: self.data.close()
			except: pass
			self.data = None
		self._hasDataChanged = False

	def setData( self, data ):
		self._data           = data
		self._hasDataChanged = True
		return self

	def setMeta( self, meta=NOTHING, **options ):
		if meta is not NOTHING:
			assert type(meta) is dict, "StoredRaw.setMeta only accepts dict"
			self._meta = meta
		for k in options:
			self._meta[k] = options[k]
		return self

	def clearMeta( self ):
		self.meta = {}
		return self

	def update( self, propertiesAndRelations ):
		"""Compatiblity method for Storable, is an alias to setMeta"""
		return self.setMeta(propertiesAndRelations)

	def meta( self, name=NOTHING, value=NOTHING, **options ):
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
				assert name not in self.RESERVED, "Reserved meta property: {0}".format(name)
				self._meta[name] = value
				return self

	def data( self, size=None ):
		"""Iterates through the data with chunks of the given size."""
		if self._data:
			yield self._data
		elif self.STORAGE:
			for _ in self.STORAGE.stream(self, size=None):
				yield _

	def path( self ):
		"""Returns the path of the data file."""
		if self.STORAGE:
			return self.STORAGE.path(self)
		else:
			return None

	def length( self ):
		return len(self._data)

	def export( self, **options ):
		depth = 1
		if "depth" in options: depth = options["depth"]
		res = dict(
			oid=self.oid,
			timestamp=self.timestamp,
			type=getCanonicalName(self.__class__)
		)
		if depth > 0: res.update(self._meta)
		return res

	def save( self ):
		assert self.STORAGE, "StoredRaw must have storage"
		self.STORAGE.update(self)

	def __repr__( self ):
		return "<raw:%s %s:%s>" % (self.__class__.__name__, id(self), self.oid)

# -----------------------------------------------------------------------------
#
# RAW STORAGE
#
# -----------------------------------------------------------------------------

class RawStorage:

	DATA_SUFFIX = ".data"
	META_SUFFIX = ".meta"

	def __init__( self, backend ):
		"""Creates a new metric storage with the given backend"""
		self.backend          = backend
		self._declaredClasses = {}
		self.lock             = threading.RLock()
		self._cache           = weakref.WeakValueDictionary()
		self._classPrefix     = weakref.WeakKeyDictionary()

	def register( self, storedRaw, restored=False ):
		"""Registers a new StoredRaw in this storage. This allows a get()
		to be successful, even before the object is actually stored in the db."""
		assert isinstance(storedRaw, StoredRaw), "Only stored raw can be registered"
		self.lock.acquire()
		key              = storedRaw.oid
		self._cache[key] = storedRaw
		self.lock.release()
		return self

	def use( self, *classes ):
		"""Makes this storage register itself with the given classes."""
		for c in classes:
			self._declaredClasses[getCanonicalName(c)] = c
			assert c.STORAGE is None, "Storable already has a storage"
			c.STORAGE = self
			Storable.DeclareClass(c)
		return self

	def getStorageKeys( self, storedRawOrKey ):
		if isinstance(storedRawOrKey, StoredRaw):
			key = storedRawOrKey.oid
			prefix = self._classPrefix.get(storedRawOrKey.__class__)
			if not prefix:
				prefix = storedRawOrKey.__class__.__name__.split(".")[-1]
				self._classPrefix[storedRawOrKey.__class__] = prefix
			key =  prefix + "." + storedRawOrKey.oid
		else:
			key = storedRawOrKey
		key_data = key + self.DATA_SUFFIX
		key_meta = key + self.META_SUFFIX
		return key_meta, key_data

	def add( self, storedRaw ):
		key_meta, key_data = self.getStorageKeys(storedRaw)
		assert storedRaw.hasStorage()
		assert storedRaw.STORAGE == self, "StoredRaw stored in a different storage"
		assert storedRaw.oid in self._cache, "StoredRaw should be already in cache"
		self.backend.add(key_meta, self.serializeMeta(storedRaw.export()))
		# We only store the data if it has changed
		if storedRaw.hasDataChanged():
			# FIXME: Backends should supprot file instead of string for data
			self.backend.add(key_data, self.serializeData(storedRaw._data))
			storedRaw.setDataSaved()

	def update( self, storedRaw ):
		# Right now, we don't need anything different than add
		self.add(storedRaw)

	def restore( self, meta=None, data=None ):
		if isinstance(meta, StoredRaw):
			# in most cases, unJSON will properly deserialize the JSON
			# into a StoredRaw, so we just add the storage
			meta.setStorage(self)
			return meta
		else:
			if meta["oid"] in self._cache:
				res = self._cache[meta["oid"]]
				res.meta(meta)
			else:
				raw_class = self._declaredClasses.get(meta.get("type")) or RawStorage
				# TODO: Should report any problem
				res = raw_class(
					data,
					restored=True,
					**meta
				)
			if isinstance(res, StoredRaw): res.setStorage(self)
		return res

	def get( self, keyOrStoredRaw ):
		cache_key = keyOrStoredRaw.oid if isinstance(keyOrStoredRaw, StoredRaw) else keyOrStoredRaw
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

	def has( self, keyOrStoredRaw ):
		cache_key = keyOrStoredRaw.oid if isinstance(keyOrStoredRaw, StoredRaw) else keyOrStoredRaw
		# We look in the cache first
		if cache_key in self._cache:
			return self._cache[cache_key]
		else:
			key_meta, key_data = self.getStorageKeys(keyOrStoredRaw)
			return self.backend.has(key_data) or self.backend.has(key_meta)

	def remove( self, keyOrStoredRaw ):
		key_meta, key_data = self.getStorageKeys(keyOrStoredRaw)
		self.backend.remove(key_meta)
		self.backend.remove(key_data)
		cache_key = keyOrStoredRaw.oid if isinstance(keyOrStoredRaw, StoredRaw) else keyOrStoredRaw
		if cache_key in self._cache: del self._cache[cache_key]
		return self

	def sync( self ):
		"""Explicitely ask the back-end to synchronize. Depending on the
		back-end this might be a long or short, blocking or async
		operation."""
		self.backend.sync()

	def query( self, keyOrStoredRaw=None, timestamp=None ):
		assert timestamp is None, "Timestamp not supported yet"
		raise NotImplementedError
		#for k in self.backend(keys):
		#	key_meta, key_data = self.getStorageKeys(keyOrStoredRaw)
		#	if self.backend.has(key_meta):
		#		meta = self.deserializeMeta(self.backend.get(key_meta)),
		#		dataless_raw = self.restore(meta=meta)
		#		if dataless_raw.oid == oid:
		#			yield dataless_raw

	def keys( self, types=None ):
		prefix = self._getStoragePrefix(types)
		for key in self.backend.keys(prefix):
			yield key

	def list( self, count=-1, start=0, end=None, types=None ):
		end = start + count if end is None else end
		i   = 0
		if types and type(types) not in (list, tuple):types=(types,)
		# FIXME: Should be updated according to raw storage
		suffix_len = len(self.DATA_SUFFIX)
		for key in self.keys(types):
			if count < 0 or (i >= start and (i < end or end < 0)):
				if not key.endswith(self.DATA_SUFFIX) and not key.endswith(self.META_SUFFIX):
					continue
				key = key[:-suffix_len]
				s = self.get(key)
				if not s:
					continue
				if not types or s.__class__ in types:
					yield s
			i += 1

	def count( self, types=None ):
		return len(list(self.list(types=types)))

	def path( self, storedRaw ):
		"""Gets the physical path (on the file system) of the data for
		this stored raw. This might generate an exception if the backend
		does not support it."""
		key_meta, key_data = self.getStorageKeys(storedRaw)
		return self.backend.path(key_data)

	def stream( self, storedRaw, size=None ):
		"""Streams the data from the storage -- this might generate an
		exception, as not all storage support streaming."""
		key_meta, key_data = self.getStorageKeys(storedRaw)
		for chunk in self.backend.stream(key_data, size=None):
			yield chunk

	def serializeData( self, data ):
		return data

	def deserializeData( self, data ):
		return data

	def serializeMeta( self, meta ):
		return asJSON(meta)

	def deserializeMeta( self, meta ):
		# NOTE: We don't want to restore the object yet there, we just want
		# the meta information
		return unJSON(meta, useRestore=False)

	# FIXME: Same as StoredObject._getStoragePrefix
	def _getStoragePrefix( self, types=None ):
		"""Returns the list of prefixes for keys that are used to store objects
		of the given classes."""
		prefix = None
		if types:
			if type(types) not in (tuple,list): types = (types,)
			prefix = [_.StoragePrefix() for _ in types]
		return prefix

# EOF - vim: tw=80 ts=4 sw=4 noet
