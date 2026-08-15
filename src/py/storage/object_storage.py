"""Object storage runtime implementation."""

import inspect
import threading
import traceback
import weakref
from typing import Any, Optional, Self

from .backends import StorageBackend
from .core import Storable, asPrimitive, getCanonicalName
from .objects import Ownership, PublicID, StoredObject
from .utils import atomic


class ObjectStorage:
	"""A simple encapsulation of a key-value database that makes sure that
	you'll always get the same physical object for the given key -- at least
	until you remove the object.

	One thing to notice here is that you'll have to call `sync` explicitely
	to make sure that your objects are persisted to the back-end database.
	Some backends might sync implicitely, but in general, you should call
	sync regularily.

	By default, object are kept in a WeakValueDictionary to prevent having
	too many objects in memory when memory becomes scarce.
	"""

	def __init__(
		self,
		backend: StorageBackend,
		validateSchema: bool = True,
		migrateSchema: bool = True,
	):
		self.backend = backend
		self.validateSchemaOnUse = validateSchema
		self.migrateSchemaOnUse = migrateSchema
		self.lock = threading.RLock()
		# FIXME: This is wrong, we should make sure the object is persisted
		# when it is removed from cache!
		self._cache = weakref.WeakValueDictionary()
		self._syncQueue = weakref.WeakValueDictionary()
		self._lastSync = 0
		self._declaredClasses = {}
		# Used to keep track of allocated objects
		self.allocated: list[StoredObject] = []

	def register(self, storedObject: StoredObject, restored: bool = False) -> Self:
		"""Registers this new StoredObject in this storage. This allows a get()
		to be successful, even before the object is actually stored in the db."""
		if not isinstance(storedObject, StoredObject):
			raise ValueError(
				f"Only stored objects can be registered, got {type(storedObject)}: {storedObject}"
			)
		self.lock.acquire()
		# Here we don't need to check, as we're already sure it's a stored
		# object
		key = storedObject.getStorageKey()
		if not restored:
			self._syncQueue[key] = storedObject
		self._cache[key] = storedObject
		# TODO: Should register a callback for changes and then call for a merge
		self.lock.release()
		return self

	def _restore(self, exportedStoredObject: dict[str, Any], key: str | None = None) -> StoredObject:
		# NOTE: We call restore only when the object was not already in cache
		# NOTE: Exported stored object  is expected to be a dict as give
		# by StoredObject.export
		assert type(exportedStoredObject) is dict, (
			"Expected a dictionary as exported by StoredObject.export(), got a %s"
			% (type(exportedStoredObject))
		)
		id = exportedStoredObject["id"]
		oclass = exportedStoredObject["type"]
		# FIXME: Should check if the exported stored object is in cache first!
		actual_class = self._declaredClasses.get(oclass)
		if actual_class:
			if isinstance(id, (tuple, list)) and actual_class.GetOwnership() and len(id) == 2:
				owner_id, local_id = id
				exportedStoredObject["id"] = local_id
				exportedStoredObject.setdefault("owner", owner_id)
				exportedStoredObject.setdefault("partition", owner_id)
				id = local_id
			if key and "partition" not in exportedStoredObject:
				parts = key.split(".", 2)
				if len(parts) == 3 and parts[1] != actual_class.PartitionBucket(None):
					exportedStoredObject["partition"] = parts[1]
					if actual_class.GetOwnership() and "owner" not in exportedStoredObject:
						exportedStoredObject["owner"] = parts[1]
			owner = exportedStoredObject.get("owner")
			partition = exportedStoredObject.get("partition")
			storage_key = actual_class.StorageKey(id, owner=owner, partition=partition)
			assert storage_key not in self._cache
			# We instanciate the object, which will then be available in the cache, as
			# the constructor calls Storage.register.
			new_object = actual_class(id, exportedStoredObject, restored=True)
			assert storage_key in self._cache
			return new_object
		else:
			raise Exception("Class not registered in ObjectStorage: %s" % (oclass))

	def _injectOwner(self, value: dict[str, Any], owner_id: str) -> None:
		"""Legacy no-op kept for callers that imported the private helper."""
		if isinstance(value, dict):
			for v in value.values():
				self._injectOwner(v, owner_id)
		elif isinstance(value, list):
			for item in value:
				self._injectOwner(item, owner_id)

	def add(self, storedObject: StoredObject, creation: bool = False):
		"""Sets the given value to the given key, storing it in cache. Note that
		this does not store all referenced objects."""
		self.lock.acquire()
		allocatedPublicID = False
		try:
			# if True:
			storedObject.validateOwnership()
			key = storedObject.getStorageKey()
			if storedObject.__class__.HasPublicID() and storedObject.publicId is None:
				scope = storedObject.__class__.PublicIDScope(
					partition=storedObject.partition
				)

				def exportPublicObject(publicID):
					nonlocal allocatedPublicID
					storedObject._setPublicID(publicID)
					allocatedPublicID = True
					return self.serializeObjectExport(storedObject.export())

				self.backend.storePublicObject(key, scope, exportPublicObject)
			else:
				exported_object = self.serializeObjectExport(storedObject.export())
				if creation:
					self.backend.add(key, exported_object)
				else:
					self.backend.update(key, exported_object)
			try:
				self._cache[key] = storedObject
			except TypeError:
				# NOTE: Not sure in which cache we would get a cache error.
				pass
			if isinstance(storedObject, StoredObject):
				storedObject.setStorage(self)
			self.lock.release()
		except Exception:
			# We make sure to always release the lock here
			self.lock.release()
			if allocatedPublicID:
				storedObject._publicID = None
			exception_format = repr(traceback.format_exc()).split("\\n")
			error_msg = "\n|".join(exception_format[:-1])
			raise Exception(error_msg)
		# We update the indexes
		if hasattr(storedObject, "INDEXES"):
			for index in storedObject.INDEXES or ():
				if creation:
					index.add(storedObject)
				else:
					index.update(storedObject)
				index.save()
		return storedObject

	def create(self, storedObject: StoredObject) -> StoredObject:
		"""Alias for `add`, but checks that the object does not already exists"""
		# assert not self.has(key), "ObjectStorage already has object with key: '%s'" % (key)
		return self.add(storedObject, creation=True)

	def update(self, storedObject: StoredObject) -> StoredObject:
		"""Alias for update, but checks that the object already exist"""
		# assert self.has(key), "ObjectStorage has no object with key: '%s'" % (key)
		return self.add(storedObject, creation=False)

	def get(self, key):
		"""Returns the instance attached to the given key in the storage.
		We use an intermediate cache as shove's cache may delete instances
		whenever it find it necessary."""
		with atomic(self.lock):
			return self._get(key)

	def _get(self, key):
		result = None
		# We look in the cache first
		if key in self._cache:
			result = self._cache[key]
			return result
		# Or we get it directly from shove
		else:
			value = self.backend.get(key)
			if value:
				value = self.deserializeObjectExport(value)
				value = self._restore(value, key=key)
				if value is not None:
					try:
						self._cache[key] = value
					except TypeError:
						pass
				if isinstance(value, StoredObject):
					value.setStorage(self)
					return value
				else:
					assert isinstance(value, StoredObject), (
						"Stored object expected, got: %s" % (value)
					)
			else:
				return None

	def has(self, key):
		"""Tells if the storage has such a key."""
		# We have to hit the cache first, as Shove has a cache that will remove
		# instances after some time, we might end up with two objects for the
		# same key if we don't hit the cache first
		if key in self._cache:
			return True
		elif self.backend.has(key):
			return True
		else:
			return False

	def count(self, storedObjectClasses=None):
		return self.backend.count(self._getStoragePrefix(storedObjectClasses))

	def keys(self, storedObjectClasses=None, prefix=None, order=0):
		# FIXME: Not sure if we should list the cache fist...
		# for key in self._cache.keys():
		# 	if not prefix or key.startswith(prefix):
		# 		yield key
		p = self._getStoragePrefix(storedObjectClasses)
		if prefix:
			if p:
				p = [_ + "." + prefix for _ in p]
			else:
				p = prefix
		for key in self.backend.keys(p, order=order):
			if self._matchesPrefix(key, p):
				yield key

	# FIXME: Should be updated according to raw storage
	def list(self, storedObjectClasses=None, count=-1, start=0, end=None, order=0):
		"""Lists (iterates) the stored objects belonging to the given class. Note that
		there is no guaranteed ordering in the keys, so this might return different
		results depending on how many keys there are."""
		end = end if end >= 0 else (start + count if count > 0 else None)
		i = 0
		for key in self.keys(storedObjectClasses, order=order):
			if count != 0:
				if i >= start and (i < end or end is None):
					if count > 0:
						count -= 1
					yield self.get(key)
			i += 1

	def changeOwner(self, storedObject: StoredObject, owner: Optional[StoredObject]):
		if storedObject.publicId is not None:
			raise ValueError("Cannot change owner of an object with a public ID")
		oldKey = storedObject.getStorageKey()
		try:
			storedObject._allowPartitionChange = True
			storedObject.owner = owner
			storedObject.partition = owner.id if isinstance(owner, StoredObject) else owner
			storedObject.save()
		finally:
			storedObject._allowPartitionChange = False
		newKey = storedObject.getStorageKey()
		if newKey != oldKey:
			self._cache.pop(oldKey, None)
			self._syncQueue.pop(oldKey, None)
			if self.backend.has(oldKey):
				self.backend.remove(oldKey)
		return storedObject

	def changePartition(self, storedObject: StoredObject, partition: Optional[Any]):
		if storedObject.publicId is not None:
			raise ValueError("Cannot change partition of an object with a public ID")
		oldKey = storedObject.getStorageKey()
		try:
			storedObject._allowPartitionChange = True
			storedObject.partition = partition
			storedObject.save()
		finally:
			storedObject._allowPartitionChange = False
		newKey = storedObject.getStorageKey()
		if newKey != oldKey:
			self._cache.pop(oldKey, None)
			self._syncQueue.pop(oldKey, None)
			if self.backend.has(oldKey):
				self.backend.remove(oldKey)
		return storedObject

	def isCached(self, key):
		"""Tells if the given key is found in cache."""
		return key in self._cache

	def uncache(self, key):
		"""Uncaches the given key. If it is a stored object, it will be saved
		before being uncached."""
		if key in self._cache:
			v = self._cache[key]
			if isinstance(v, StoredObject):
				v.save()
		if key in self._cache:
			del self._cache[key]

	def remove(self, key):
		"""Removes the given key from the storage and from the cache"""
		if isinstance(key, StoredObject):
			old_value = key
			key = old_value.getStorageKey()
		else:
			old_value = self.get(key)
		if old_value and isinstance(old_value, StoredObject):
			self._cascadeOwnedObjects(old_value)
		if key in self._cache:
			del self._cache[key]
		# We update the indexes
		if hasattr(old_value, "INDEXES"):
			for index in old_value.INDEXES or ():
				index.remove(old_value)
				index.save()
		self.backend.removeObject(key)
		if old_value and isinstance(old_value, StoredObject):
			old_value.onRemove()

	def sync(self):
		"""Synchronizes the modifications with the backend."""
		for key, storedObject in list(self._syncQueue.items()):
			if storedObject:
				self.backend.update(key, storedObject.export())
		self.backend.sync()

	def validateSchema(self, applyMigrations: Optional[bool] = None):
		from .schema import SchemaValidator

		validator = SchemaValidator(
			self,
			migrate=self.migrateSchemaOnUse if applyMigrations is None else applyMigrations,
		)
		return validator.validate()

	def use(self, *classes):
		"""Makes this storage register itself with the given classes."""
		for c in classes:
			if c.PUBLIC_ID is not None and not isinstance(c.PUBLIC_ID, PublicID):
				raise ValueError(
					f"PUBLIC_ID for {c.__name__} must be a PublicID strategy"
				)
			name = getCanonicalName(c)
			c.STORAGE = self
			if name not in self._declaredClasses:
				self._declaredClasses[name] = c
				Storable.DeclareClass(c)
		if self.validateSchemaOnUse:
			self.validateSchema()
		return self

	def release(self):
		for k, c in list(self._declaredClasses.items()):
			c.STORAGE = None
		self._declaredClasses = {}

	def _getStoragePrefix(self, storedObjectClasses=None):
		"""Returns the list of prefixes for keys that are used to store objects
		of the given classes."""
		prefix = None
		if storedObjectClasses:
			if issubclass(storedObjectClasses, StoredObject):
				storedObjectClasses = (storedObjectClasses,)
			prefix = [_.StoragePrefix() for _ in storedObjectClasses]
		return prefix

	def ownedBy(self, owner: StoredObject, storedObjectClasses=None):
		classes = self._ownedClasses(storedObjectClasses)
		for ownedClass in classes:
			# TODO: Fall back to a dedicated owner index if key layout ever stops
			# encoding ownership as `<collection>.<owner>.<local>`.
			for key in self.keys(ownedClass, prefix=ownedClass.PartitionBucket(owner) + "."):
				item = self.get(key)
				if item:
					yield item

	def _ownedClasses(self, storedObjectClasses=None):
		if storedObjectClasses:
			if inspect.isclass(storedObjectClasses) and issubclass(
				storedObjectClasses, StoredObject
			):
				classes = (storedObjectClasses,)
			else:
				classes = tuple(storedObjectClasses)
		else:
			classes = tuple(self._declaredClasses.values())
		return [
			c
			for c in classes
			if isinstance(c.GetOwnership(), Ownership)
		]

	def _cascadeOwnedObjects(self, owner: StoredObject):
		for ownedClass in self._ownedClasses():
			ownership = ownedClass.OWNERSHIP
			if not ownership or not ownership.cascade:
				continue
			if not isinstance(owner, ownership.ownerType):
				continue
			for item in list(self.ownedBy(owner, ownedClass)):
				self.remove(item)

	def _matchesPrefix(self, key, prefix) -> bool:
		if prefix is None:
			return True
		if isinstance(prefix, (tuple, list)):
			return any(self._matchesPrefix(key, _) for _ in prefix)
		return str(key).startswith(str(prefix))

	def export(self):
		"""Exports all the objects in this storage. You should only use that
		in development mode as it could bring down your machine as it will
		load all the objects and export them."""
		res = {}
		for key in list(self.keys()):
			res[key] = self.get(key)
		return res

	def serializeObjectExport(self, data):
		return asPrimitive(data)

	def deserializeObjectExport(self, data):
		return data

	def __enter__(self):
		self.allocated = []
		return self.allocated

	def __exit__(self, type, value, traceback):
		parent_locals = inspect.currentframe().f_back.f_locals
		# Upon exit, we name any atom that we find in the scope
		for k, v in (
			(k, v) for k, v in parent_locals.items() if isinstance(v, StoredObject)
		):
			if not v.storage:
				v.setStorage(self)
			v.save()
			self.allocated.append(v)
		self.allocated = []


__all__ = ["ObjectStorage"]
