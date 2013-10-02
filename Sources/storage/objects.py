# -----------------------------------------------------------------------------
# Project   : FFCTN/Storage
# -----------------------------------------------------------------------------
# Author    : Sebastien Pierre                            <sebastien@ffctn.com>
# License   : BSD License
# -----------------------------------------------------------------------------
# Creation  : 14-Jul-2008
# Last mod  : 26-Sep-2013
# -----------------------------------------------------------------------------

# TODO: Add import/create/update filters that will check and normalize the input data

# TODO: Review sync queue and caching, which don't seem to be 100% consitent
# TODO: Add sync queue, review caching mechanism
# TODO: Add revision/update meta-data
# TODO: Add properties (for stored object compatibility)

# FIXME: Use of locks in back-end is not ideall... should be heavily tested

# FIXME: Backend should never be accessed directly. remove storage.backend
# and STORAGE.backend references with more abstract functions.

# FIXME: Provide a MongoDB backend, and introduce the notion of collection
#        along with the key

# NOTE: This module is a generic module that is part of FFCTN's custom modules
# library, it is distributed under the BSD license.

# FIXME: How to update the objects when the db has changed locally

import time, threading, json, weakref, types, datetime, traceback
from   storage import Identifier, getCanonicalName, asPrimitive, asJSON, unJSON, Storable, restore

__pychecker__ = "unusednames=options"

# TODO: Do some garbage-collection in the cache or use weak-references

__doc__ = """

This module provides is a model for object persistence in composable storage
backend (memory, journal, file, directory and MongoDB). The main interface
ensures that you'll always get the same physical object for a specific key --
this comes to the price of maintaining a runtime cache of the objects stored in
the database.

Rules:

 - Never directly store reference to StoredObject, instead store the 'oid'
   and restore it on 'onRestore' (or when it is invoked).

 - Try to have compact, simple representation of your stored objects, so they
   don't take too much space and can be easily exported to JSON.

"""

# -----------------------------------------------------------------------------
#
# STORED OBJECT
#
# -----------------------------------------------------------------------------

# FIXME: StoredObject should have a locked revision counter that allows to
# compare snapshots
# NOTE: StoredObjects are designed to be pickleable and jsonable
class StoredObject(Storable):
	"""Stored objects provides an abstraction for storing objects in an
	ObjectStorage. Each object has an `oid` (object id) which is unique for
	its type.

	The actual key used to store the object in the object storage
	is returns by `StorageKey` and is by default the canonical class name
	and the object id.

	Note that stored objects share a `STORAGE` singleton. If you want multiple
	or different storages per stored object class, then simply Implement
	a storage proxy that implements you specific strategy.
	"""

	OID_GENERATOR         = Identifier.Stamp
	SKIP_EXTRA_PROPERTIES = False
	COLLECTION            = None
	STORAGE               = None
	PROPERTIES            = {}
	COMPUTED_PROPERTIES   = ()
	RELATIONS             = {}
	RESERVED              = ("type", "oid", "timestamp")
	INDEXES               = None

	@classmethod
	def Recognizes( self, data ):
		if type(data) == (dict):
			for key in self.PROPERTIES:
				if not data.has_key(key):
					return False
			return True
		else:
			return False

	@classmethod
	def AddIndex( cls, index ):
		if cls.INDEXES is None: cls.INDEXES = []
		if index not in cls.INDEXES:
			cls.INDEXES.append(index)
		return cls

	@classmethod
	def RebuildIndexes( cls ):
		for v in cls.All():
			# FIXME: Does not work!
			for i in index:
				i.clear()
				i.add(v)

	@classmethod
	def GenerateOID( cls ):
		"""Generates a new object ID for this class"""
		return cls.OID_GENERATOR()

	@classmethod
	def StoragePrefix( cls ):
		return self._getStoragePrefix(cls)[0]

	@classmethod
	def All( cls ):
		"""Iterates on all the objects of this type in the storage."""
		assert cls.STORAGE, "Class has not been registerd in an ObjectStorage yet: %s" % (cls)
		for storage_id in cls.STORAGE.keys(cls):
			yield cls.STORAGE.get( storage_id )

	@classmethod
	def Keys( cls, prefix=None ):
		"""List all the keys for objects of this type in the storage."""
		assert cls.STORAGE, "Class has not been registerd in an ObjectStorage yet: %s" % (cls)
		return cls.STORAGE.keys(cls, prefix=prefix)

	# NOTE: We should return Non when the object does not exist, and provide
	# an Ensure method that will create the object if necessary.
	@classmethod
	def Get( cls, oid ):
		"""Returns the instance associated with the given Object ID, if any"""
		assert cls.STORAGE, "Class has not been registered in an ObjectStorage yet: %s" % (cls)
		if oid is None: return None
		return cls.STORAGE.get(cls.StorageKey(oid))

	@classmethod
	def Count( cls ):
		"""Returns the count of objects of this type stored in the storage."""
		assert cls.STORAGE, "Class has not been registered in an ObjectStorage yet: %s" % (cls)
		return cls.STORAGE.count(cls)

	@classmethod
	def List( cls, count=-1, start=0, end=None ):
		"""Returns the list of objects of this type stored in the storage."""
		assert cls.STORAGE, "Class has not been registerd in an ObjectStorage yet: %s" % (cls)
		return cls.STORAGE.list(cls, count, start, end)

	@classmethod
	def Has( cls, oid ):
		"""Tells if there is an object stored with the given object id."""
		assert cls.STORAGE, "Class has not been registerd in an ObjectStorage yet: %s" % (cls)
		return cls.STORAGE.has(cls.StorageKey(oid))

	@classmethod
	def Ensure( cls, oid ):
		"""Ensures that there is an object with the given object id in the
		storage. If not, it will create a new instance of this specific
		stored object sub-class"""
		res = cls.Get(oid)
		if res is None:
			res  = cls(oid)
		return res

	@classmethod
	def StorageKey( cls, oid ):
		"""Returns the storage key associated with the given oid of this class."""
		if isinstance(oid, StoredObject): oid = oid.oid
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

	@classmethod
	def Import( cls, properties, skipExtraProperties=None, updateProperties=False ):
		"""Turns the given primitive export into an instance of this class.
		Properties can be either a primtive export or a StoredObject instance,
		in which case this function will just return its parameter.
		"""
		if skipExtraProperties is None: skipExtraProperties = cls.SKIP_EXTRA_PROPERTIES
		if isinstance(properties, StoredObject):
			assert isinstance(properties, cls), "Expected class %s, got %s" % (cls, properties.__class__)
			return properties
		else:
			oid   = properties.get("oid")
			otype = properties.get("type")
			assert not otype or otype == getCanonicalName(cls), "Expected type %s, got %s" % (getCanonicalName(cls), otype)
			# If there is an object ID
			if oid:
				# We look in the storage for this specific object
				obj = cls.Get(oid)
				# If it exists, we update its properties
				if obj:
					# FIXME: I don't see the use case for an `updateProperties`, but am
					# leaving here as an option. The default behaviour is that we should
					# only update the properties if the object does not exist in the
					# storage. If it does, then we assume the storage's version is the
					# most up to date.
					if updateProperties:
						obj.set(properties, skipExtraProperties=skipExtraProperties)
					return obj
				# Otherwise we create a new one
				else:
					return cls(properties=properties, skipExtraProperties=skipExtraProperties)
			else:
				return cls(properties=properties, skipExtraProperties=skipExtraProperties)

	@classmethod
	def Export( cls, oid, **options ):
		"""A convenient fonction that will return the full object corresponding
		to the oid if it is in base, or will return a stripped down version with
		oid and class."""
		o = cls.STORAGE.get( cls.StorageKey(oid) )
		if o:
			return o.export(**options)
		else:
			return {"oid":oid, "type":getCanonicalName(cls)}

	HAS_DESCRIPTORS = False
	@classmethod
	def _GenerateDescriptors( cls, instance ):
		"""Generates the descriptors that allow for wrapping values in
		Property and Relations, allowing transparent lazy restoring  of
		values."""
		# NOTE: In some cases, PROPERTIES and RELATIONS might reference
		# the current class, in which case we simply wrap everything in a
		# lambda that will be evaluated only once the class has been
		# declared.
		#
		# For example:
		#     class User: RELATIONS=dict(knows=[User]))
		# is not valid, but this would be:
		#     class User: RELATIONS=lambda:dict(knows=[User]))
		# NOTE: We have to pass an instance, as otherwise we'll get the
		# following exception:
		# TypeError: unbound method <lambda>() must be called with Tutorial instance as first argument (got type instance instead)
		if type(cls.PROPERTIES) != dict: cls.PROPERTIES = cls.PROPERTIES(instance)
		if type(cls.RELATIONS) != dict:  cls.RELATIONS  = cls.RELATIONS(instance)
		for _ in cls.PROPERTIES: setattr(cls, _, PropertyDescriptor(_))
		for _ in cls.RELATIONS : setattr(cls, _, RelationDescriptor(_))
		cls.HAS_DESCRIPTORS = True
		return cls

	def __init__( self, oid=None, properties=None, restored=False, skipExtraProperties=None, **kwargs ):
		"""Creates a new stored object instance with the given oid. If none is given, then a new oid will be generated."""# If the oid is not directly given, it might be listed in the properties
		if skipExtraProperties is None: skipExtraProperties = self.SKIP_EXTRA_PROPERTIES
		if oid is None and properties:
			oid = properties.get("oid")
		# If we really can't find an oid, we generate a new one
		if oid is None:
			self.oid = self.GenerateOID()
		else:
			self.oid = oid
		self.storage = self.STORAGE
		if not self.__class__.HAS_DESCRIPTORS: self.__class__._GenerateDescriptors(self)
		self._properties = {}
		self._relations  = {}
		self._isNew      = restored
		self.set(properties, skipExtraProperties=skipExtraProperties)
		self.set(kwargs, skipExtraProperties=skipExtraProperties)
		if self.STORAGE: self.STORAGE.register(self, restored=restored)
		assert self.getStorageKey(), "Object must have a key once created"
		self.init()

	def init( self ):
		"""Can be overriden to do post-creation/import processing"""

	def set( self, propertiesAndRelations, skipExtraProperties=None ):
		if skipExtraProperties is None: skipExtraProperties = self.SKIP_EXTRA_PROPERTIES
		if propertiesAndRelations:
			for name, value in propertiesAndRelations.items():
				if name in self.PROPERTIES:
					self.setProperty(name, value)
				elif name in self.RELATIONS:
					self.setRelation(name, value)
				elif name in self.RESERVED:
					pass
				elif name in self.COMPUTED_PROPERTIES:
					pass
				elif skipExtraProperties:
					pass
				else:
					raise Exception("Extra property given to %s: %s=%s" % (self.__class__.__name__, name, value))
		return self

	def update( self, propertiesAndRelations ):
		return self.set(propertiesAndRelations)

	def setProperty( self, name, value ):
		"""Sets a property of the given object. The property must match the
		properties defined in `PROPERTIES`"""
		# TODO: Check type
		assert name in self.PROPERTIES, "Property `%s` not one of: %s" % (name, self.PROPERTIES.keys() + self.RELATIONS.keys())
		if not name in self._properties: self._properties[name] = Property(name)
		self._properties[name].set(value)
		return self

	def setRelation( self, name, value ):
		"""Sets a relation of the given object. The value must match the
		definition in `RELATIONS`"""
		# TODO: Check type
		assert name in self.RELATIONS, "Relation `%s` not one of: %s" % (name, self.PROPERTIES.keys() + self.RELATIONS.keys())
		if not name in self._relations: self._relations[name] = Relation(self.__class__, self.RELATIONS[name])
		self._relations[name].set(value)
		return self

	def getProperty( self, name ):
		"""Returns the property value bound to the given name"""
		if  self.__class__.PROPERTIES.has_key(name):
			if not name in self._properties: self._properties[name] = Property(name)
			return self._properties[name].get()
		else:
			raise Exception("Property %s.%s is not declared in PROPERTIES" % (self.__class__.__name__, name))

	def getRelation( self, name ):
		"""Returns the given relation object"""
		if self.__class__.RELATIONS.has_key(name):
			if not name in self._relations: self._relations[name] = Relation(self, self.RELATIONS[name])
			return self._relations[name]
		else:
			raise Exception("Property %s.%s is not declared in RELATIONS" % (self.__class__.__name__, name))

	def getID( self ):
		self.oid

	def getStorageKey( self ):
		"""Returns the key used to store this object in a storage."""
		return self.__class__.StorageKey(self.oid)

	def setStorage( self, storage ):
		"""Sets the storage object associated with this object."""
		# NOTE: For now we just expect the storage not to change... but maybe
		# there is a case where we'd need multiple storages
		assert self.storage is None or self.storage == storage
		self._isNew  = False
		self.storage = storage

	def getCollection( self ):
		"""Returns the collection for this stored object"""
		return self.COLLECTION or self.__class__.__name__

	def remove( self ):
		"""Removes the stored element from the storage."""
		#print "[DEBUG] Removing stored element", self.__class__.__name__, "|", self.oid, "|", self
		assert self.storage, "StoredObject has no associated storage"
		self.storage.remove(self)

	def save( self ):
		"""Saves this object to the storage."""
		assert self.storage, "StoredObject has no associated storage"
		key = self.getStorageKey()
		if self.storage.has(key):
			self.storage.update(self)
		else:
			self.storage.create(self)
		return self

	def onStore( self, d ):
		"""Processes the dictionnary that will be stored as a value in the
		storage back-end. Override this to remove non-picklable values."""
		return d

	def onRestore( self ):
		"""Invoked when the stored element is restored from the database. This
		registers the object in the database cache."""
		#print "XXX ON RESTORE", self.oid, self
		self.storage.register(self)

	def onRemove( self ):
		"""Invoked after the element is removed from the cache"""

	def __getstate__( self ):
		"""This strips the state of events, and object storage reference which
		cannot really be pickled."""
		d = self.__dict__
		s = d.copy()
		s = self.onStore(s)
		for k,v in d.items():
			pass
			# FIXME: We should have a more generic mechanism
			# if isinstance(v, Event) or isinstance(v, ObjectStorage):
			# 	del s[k]
		return s

	def __setstate__( self, state ):
		"""Sets the state of this object given a dictionary loaded from the
		object storage. Override this to re-construct the object state
		from what is returned by `__getstate__`"""
		self.__dict__.update(state)
		self.__dict__["storage"] = self.STORAGE
		# FIXME: Should not be direct like that
		assert not self.STORAGE._cache.has_key(self.getStorageKey()), "StoredObject already in cache: %s:%s" % (self.oid, self)
		self.onRestore()
		#print "[DEBUG] Setting state for", self.oid, "|", self.__class__.__name__,  "|",  self

	def export( self, **options ):
		"""Returns a dictionary representing this object. By default, it
		just returns the object id (`oid`) and its class (`class`)."""
		res   = {"oid": self.oid, "type":getCanonicalName(self.__class__)}
		depth = 1
		if "depth" in options: depth = options["depth"]
		if depth > 0:
			value = None
			for key in self.PROPERTIES:
				value = self.getProperty(key)
				if value is not None:
					res[key] = asPrimitive(value, depth=depth - 1)
			for key in self.RELATIONS:
				relation = getattr(self, key)
				res[key] = asPrimitive(relation, depth=depth - 1)
		return res

	def asJSON( self, jsonifier=asJSON, **options ):
		"""Returns a JSON representation of this object using the
		given 'jsonifier' and 'options'"""
		return jsonifier(self.export(**options))

	def __repr__( self ):
		return "<obj:%s %s:%s>" % (self.__class__.__name__, id(self), self.oid)

# -----------------------------------------------------------------------------
#
# PROPERTY DESCRIPTOR
#
# -----------------------------------------------------------------------------

class PropertyDescriptor(object):
	"""Provides transparent access to setProperty/getProperty to StoredObjects."""

	def __init__( self, name ):
		self.name   = name

	def __get__( self, instance, owner ):
		# NOTE: Instance is null when the descriptor is directly accessed
		# at class level, in which case we return the descriptor itself
		if not instance: return self
		return instance.getProperty(self.name)

	def __set__( self, instance, value ):
		assert instance, "Property descriptors cannot be re-assigned in class"
		return instance.setProperty(self.name, value)

# -----------------------------------------------------------------------------
#
# RELATION DESCRIPTOR
#
# -----------------------------------------------------------------------------

class RelationDescriptor(object):
	"""Provides transparent access to setRelation/getRelation to StoredObjects."""

	def __init__( self, name ):
		self.name = name

	def __get__( self, instance, owner ):
		# See not on PropertyDescriptor
		if not instance: return self
		return instance.getRelation(self.name)

	def __set__( self, instance, value ):
		assert instance, "Relation descriptors cannot be re-assigned in class"
		return instance.setRelation(self.name, value)

# -----------------------------------------------------------------------------
#
# PROPERTY
#
# -----------------------------------------------------------------------------

class Property(object):
	"""Wraps a value, making sure it is restored on the first access. This
	allows to lazily convert primitives to their Storable objects, avoiding
	chain reactions of loading."""

	def __init__(self, name):
		self.name  = name
		self.value = None
		self.restored = False

	def set( self, value ):
		self.value = value
		assert not isinstance(value, Property)
		self.restored = False
		return self

	def get( self ):
		if not self.restored and self.value:
			self.value = restore(self.value)
			self.restored = True
		return self.value

	def export( self, **options ):
		if "depth" not in options: options["depth"] = 0
		return asPrimitive(self.value, **options)

	def __repr__( self ):
		return "@property:" + repr(self.value)

# -----------------------------------------------------------------------------
#
# RELATION
#
# -----------------------------------------------------------------------------

class Relation(object):
	"""Represents a relation between one object and another. This is a one-to-many
	relationship that can be lazily loaded."""

	def __init__( self, parentClass, definition ):
		# FIXME: Parent introduces a circular reference
		self.parentClass  = parentClass
		self.definition   = definition
		self.values       = None

	def init( self, values ):
		"""Initializes the relation with the given values"""
		self.values = values
		return self

	def add( self, value ):
		return self.append(value)

	def append( self, value ):
		assert type(value) in (dict, types.InstanceType, getattr(value,"__class__")), "Relation only accepts object or exported object, got: %s" % (value)
		assert isinstance(value, self.getRelationClass()) or value.get("type") == getCanonicalName(self.getRelationClass()), "Relation expects value of type %s, got: %s" % (self.getRelationClass(), value)
		if self.values is None: self.values = []
		self.values.append(value)
		assert len(self.values) <= 1 or self.isMany(), "Too many elements in relation single relation %s: %s" % (self, self.values)
		return self

	def clear( self ):
		self.values = []
		return self

	def set( self, values ):
		is_many        = self.isMany()
		relation_class = self.getRelationClass()
		self.clear()
		if type(values) not in (list,tuple): values = (values,)
		map(self.add, values)
		return self

	# FIXME: Should have better access methods to return one or many
	def get( self, start=0, limit=-1, resolve=True, depth=0 ):
		# FIXME: We should always have resolve=True, as otherwise the data
		# might get out of sync. For instance, in ARTNet when a TutorialStep changes
		# its media, the serialized version of TutorialStep on disk might change
		is_many        = self.isMany()
		relation_class = self.getRelationClass()
		values         = self.values
		i = 0
		if values is not None:
			for v in values:
				if i >= start and (limit == -1 or i < limit):
					if resolve:
						# If we resolve the value, we make sure to give
						# and actual storable
						if not isinstance(v, Storable):
							if type(v) is dict:
								yield relation_class.Import(v)
							else:
								yield relation_class.Get(v)
						else:
							yield v
					else:
						# If we do not resolve, we make sure to give a
						# (compact) representation of the value, or the
						# value itself
						if isinstance(v, Storable):
							yield v.export(depth=depth)
						# NOTE: Here we only export the minimum fields so that
						# we're explicit that this is a reference and not the
						# full value
						elif isinstance(v, dict) and "oid" in v and "type" in v:
							yield {"oid":v["oid"],"type":v["type"]}
						else:
							yield v
				i += 1

	def one( self, index=0 ):
		try:
			return self.get(resolve=True, start=index).next()
		except StopIteration, e:
			return None

	def isEmpty( self ):
		try:
			self.get(resolve=False).next()
			return False
		except StopIteration, e:
			return True

	def has( self, objectOrID ):
		oid = objectOrID.oid if isinstance(objectOrID, StoredObject) else objectOrID
		for v in self.get(resolve=False):
			if isinstance(v, dict) and "oid" in v and "type" in v and v["oid"] == oid:
				return True
		return False

	def list( self ):
		return self.get(resolve=True)

	def all( self ):
		# FIXME: This does not work!
		return tuple(self.list())

	def isMany( self ):
		return type(self.definition) in (tuple,list)

	def getRelationClass( self ):
		if self.isMany():
			return self.definition[0]
		else:
			return self.definition

	def export( self, **options ):
		o = {} ; o.update(options)
		# FIXME: For serialization we want relations to be shallow (oid/type)
		# We change depth as we want relations to be transparent
		if "depth" in o: o["depth"] += 1
		# FIXME: We have to be very clear about the resolve here -- is it a
		# good thing?
		return [asPrimitive(_, **o) for _ in self.get(resolve=options.get("resolve", True))]

	def __len__( self ):
		if self.values:
			return len(self.values)
		else:
			return 0

	def __call__( self, *args, **kwargs ):
		return self.get(*args, **kwargs)

	def __getitem__( self, key ):
		assert type(key) in (int,float), "Relations can only be queried by index, got: %s" % (key)
		assert key >= 0, "Relations can only be accessed by positive numbers, got: %s" % (key)
		for _ in self.get():
			if key == 0: return _
			else: key -= 1

	def __iter__( self ):
		return self.get(resolve=True)

	def __repr__( self ):
		return "<relation:%s=%s>" % (self.definition, self.values)

	def __delete__( self, instance, owner ):
		self.clear()
		return self

# -----------------------------------------------------------------------------
#
# OBJECT STORAGE
#
# -----------------------------------------------------------------------------

# TODO: Cache should use weak references, and only cache objects which can be
# turned to weak references (otherwise Shove's cache will be sufficient)
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

	def __init__( self, backend ):
		self.backend          = backend
		self.lock             = threading.RLock()
		# FIXME: This is wrong, we should make sure the object is persisted
		# when it is removed from cache!
		self._cache           = weakref.WeakValueDictionary()
		self._syncQueue       = weakref.WeakValueDictionary()
		self._lastSync        = 0
		self._declaredClasses = {}

	def register( self, storedObject, restored=False ):
		"""Registers this new StoredObject in this storage. This allows a get()
		to be successful, even before the object is actually stored in the db."""
		assert isinstance(storedObject, StoredObject), "Only stored objects can be registered"
		self.lock.acquire()
		# Here we don't need to check, as we're already sure it's a stored
		# object
		key = storedObject.getStorageKey()
		if not restored: self._syncQueue[key] = storedObject
		self._cache[key] = storedObject
		self.lock.release()
		return self

	def _restore( self, exportedStoredObject ):
		# NOTE: We call restore only when the object was not already in cache
		# NOTE: Exported stored object  is expected to be a dict as give
		# by StoredObject.export
		assert type(exportedStoredObject) is dict, "Expected a dictionary as exported by StoredObject.export(), got a %s" % (type(exportedStoredObject))
		oid          = exportedStoredObject["oid"]
		oclass       = exportedStoredObject["type"]
		# FIXME: Should check if the exported stored object is in cache first!
		actual_class = self._declaredClasses.get(oclass)
		if actual_class:
			key        = actual_class.StorageKey(oid)
			assert key not in self._cache
			# We instanciate the object, which will then be available in the cache, as
			# the constructor calls Storage.register.
			new_object = actual_class(oid, exportedStoredObject, restored=True)
			assert key in self._cache
			return new_object
		else:
			raise Exception("Class not registered in ObjectStorage: %s" % (oclass))

	def add( self, storedObject, creation=False ):
		"""Sets the given value to the given key, storing it in cache."""
		self.lock.acquire()
		try:
		#if True:
			key = storedObject.getStorageKey()
			self.backend.add(key, self.serializeObjectExport(storedObject.export()))
			try:
				self._cache[key] = storedObject
			except TypeError:
				pass
			if isinstance(StoredObject, StoredObject):
				storedObject.setStorage(self)

			self.lock.release()
		except Exception, e:
			self.lock.release()
			exception_format = repr(traceback.format_exc()).split("\\n")
			error_msg = u"\n|".join(exception_format[:-1])
			raise Exception(error_msg)
		# We update the indexes
		if hasattr(storedObject, "INDEXES"):
			for index in (storedObject.INDEXES or ()):
				if creation: index.add   (storedObject)
				else:        index.update(storedObject)
				index.save()
		return storedObject

	def create( self, storedObject ):
		"""Alias for `add`, but checks that the object does not already exists"""
		#assert not self.has(key), "ObjectStorage already has object with key: '%s'" % (key)
		return self.add(storedObject, creation=True)

	def update( self, storedObject ):
		"""Alias for update, but checks that the object already exist"""
		#assert self.has(key), "ObjectStorage has no object with key: '%s'" % (key)
		return self.add(storedObject, creation=False)

	def get( self, key ):
		"""Returns the instance attached to the given key in the storage.
		We use an intermediate cache as shove's cache may delete instances
		whenever it find it necessary."""
		self.lock.acquire()
		if True: #try:
			res = self._get(key)
			self.lock.release()
			return res
		# except Exception, e:
		# 	self.lock.release()
		# 	raise e

	def _get( self, key):
		result = None
		# We look in the cache first
		if self._cache.has_key(key):
			result = self._cache[key]
			return result
		# Or we get it directly from shove
		else:
			value = self.backend.get(key)
			if value:
				value = self.deserializeObjectExport(value)
				value = self._restore(value)
				if not (value is None):
					try:
						self._cache[key] = value
					except TypeError:
						pass
				if isinstance(value, StoredObject):
					value.setStorage(self)
					return value
				else:
					assert isinstance(value, StoredObject), "Stored object expected, got: %s" % (value)
			else:
				return None

	def has( self, key ):
		"""Tells if the storage has such a key."""
		# We have to hit the cache first, as Shove has a cache that will remove
		# instances after some time, we might end up with two objects for the
		# same key if we don't hit the cache first
		if self._cache.has_key(key):
			return True
		elif self.backend.has(key):
			return True
		else:
			return False

	def count( self, storedObjectClasses=None ):
		return self.backend.count(self._getStoragePrefix(storedObjectClasses))

	def keys( self, storedObjectClasses=None, prefix=None ):
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
		for key in self.backend.keys(p):
			yield key

	# FIXME: Should be updated according to raw storage
	def list( self, storedObjectClasses=None, count=-1, start=0, end=None):
		"""Lists (iterates) the stored objects belonging to the given class. Note that
		there is no guaranteed ordering in the keys, so this might return different
		results depending on how many keys there are."""
		end = start + count if end is None else end
		i   = 0
		for key in self.keys(storedObjectClasses):
			if count != 0:
				if (i >= start and i < end):
					if count > 0: count -= 1
					yield self.get(key)
			i += 1

	def isCached( self, key ):
		"""Tells if the given key is found in cache."""
		return self._cache.has_key(key)

	def uncache( self, key ):
		"""Uncaches the given key. If it is a stored object, it will be saved
		before being uncached."""
		if self._cache.has_key(key):
			v = self._cache[key]
			if isinstance(v, StoredObject):
				v.save()
		if self._cache.has_key(key):
			del self._cache[key]

	def remove( self, key ):
		"""Removes the given key from the storage and from the cache"""
		if isinstance(key, StoredObject):
			old_value = key
			key       = old_value.getStorageKey()
		else:
			old_value = self.get(key)
		if self._cache.has_key(key):
			del self._cache[key]
		# We update the indexes
		if hasattr(old_value, "INDEXES"):
			for index in (old_value.INDEXES or ()):
				index.remove(old_value)
				index.save()
		self.backend.remove(key)
		if old_value and isinstance(old_value, StoredObject):
			old_value.onRemove()

	def sync( self ):
		"""Synchronizes the modifications with the backend."""
		# We store the cached objects in the db, prefetching the keys as the
		# dictionary may change during iteration
		keys = self._cache.keys()
		for key, storedObject in self._syncQueue.items():
			v = storedObject
			if v:
				self.backend.update(key, v.export())
		self.backend.sync()

	def use( self, *classes ):
		"""Makes this storage register itself with the given classes."""
		for c in classes:
			if c.STORAGE == self: continue
			assert c.STORAGE is None, "Class %s already has a STORAGE" % (c)
			c.STORAGE = self
			self._declaredClasses[getCanonicalName(c)] = c
			Storable.DeclareClass(c)
		return self

	def _getStoragePrefix( self, storedObjectClasses=None ):
		"""Returns the list of prefixes for keys that are used to store objects
		of the given classes."""
		prefix = None
		if storedObjectClasses:
			if issubclass(storedObjectClasses, StoredObject):
				storedObjectClasses = (storedObjectClasses,)
			prefix = [_.StoragePrefix() for _ in storedObjectClasses]
		return prefix

	def export( self ):
		"""Exports all the objects in this storage. You should only use that
		in development mode as it could bring down your machine as it will
		load all the objects and export them."""
		res = {}
		for key in self.keys():
			res[key] = self.get(key)
		return res

	# FIXME: This shoudn't be asJSON bu asPrimitive
	def serializeObjectExport( self, data ):
		return asJSON(data)

	def deserializeObjectExport( self, data):
		# We unJSON without restoring, as we just want the raw primitive
		# export. The object class Import will take care of propertly
		# reconstructing the object when necessary.
		return unJSON(data, useRestore=False)

# EOF - vim: tw=80 ts=4 sw=4 noet
