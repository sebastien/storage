"""Object persistence models and descriptors.

This module contains stored object declarations and lifecycle behavior.

- stored object declarations and lifecycle
- descriptor bindings are supplied by ``objects.descriptors``

The main public API is `StoredObject`; the runtime lives in ``objects.storage``.
"""

import inspect
from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, Any, Callable, ClassVar, Iterator, Optional, Self, Type

from ..core import (
	NOTHING,
	Storable,
	Identifier,
	asJSON,
	asPrimitive,
	getCanonicalName,
	getTimestamp,
	isSame,
	restore,
)
from ..index import Index
from .descriptors import (
	InverseRelation,
	Property,
	PropertyDescriptor,
	Relation,
	RelationDescriptor,
)
from ..utils import TPrimitive

if TYPE_CHECKING:
	from .storage import ObjectStorage

# TODO: Do some garbage-collection in the cache or use weak-references

# FIXME: Relations should be exported as shallow by default (objects can change)
# The problem is that sometimes the objects have changed, or might even have
# been removed, in which case the serialized data in the relation will still
# recreate the object as it was when added to the relation.

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

# TODO: Use JSON-patch to record history of changes


@dataclass(frozen=True)
class Ownership:
	ownerType: Type["StoredObject"]
	required: bool = True
	cascade: bool = False


class PublicID(Enum):
	"""Strategies for assigning human-facing object identifiers."""

	Partition = "partition"


# -----------------------------------------------------------------------------
#
# STORED OBJECT MODEL
#
# -----------------------------------------------------------------------------

# FIXME: StoredObject should have a locked revision counter that allows to
# compare snapshots
# NOTE: StoredObjects are designed to be pickleable and jsonable
class StoredObject(Storable):
	"""Stored objects provides an abstraction for storing objects in an
	ObjectStorage. Each object has an `id` (object id) which is unique for
	its type.

	The actual key used to store the object in the object storage
	is returns by `StorageKey` and is by default the canonical class name
	and the object id.

	Note that stored objects share a `STORAGE` singleton. If you want multiple
	or different storages per stored object class, then simply Implement
	a storage proxy that implements you specific strategy.
	"""

	ID_GENERATOR: ClassVar[Callable[[], str | int]] = Identifier.ID
	ID_PREFIX: ClassVar[Optional[str]] = None
	SKIP_EXTRA_PROPERTIES: ClassVar[bool] = False
	COLLECTION = None
	STORAGE: ClassVar[Optional["ObjectStorage"]] = None
	PROPERTIES: ClassVar[dict[str, Any]] = {}
	COMPUTED_PROPERTIES: ClassVar[list[str]] = []
	RELATIONS: ClassVar[dict[str, Type["StoredObject"]]] = {}
	OWNERSHIP: ClassVar[Optional[Ownership]] = None
	RESERVED: ClassVar[list[str]] = ["type", "id", "owner", "partition", "revision", "updates"]
	INDEXES: ClassVar[list[Index]] = []
	INDEX_FOR: ClassVar[dict[str, Index]] = {}
	PUBLIC_ID: ClassVar[Optional[PublicID]] = None
	PUBLIC_ID_NAMESPACE: ClassVar[Optional[str]] = None

	@classmethod
	def Owns(cls, *, required: bool = True, cascade: bool = False) -> Ownership:
		return Ownership(ownerType=cls, required=required, cascade=cascade)

	@classmethod
	def GetOwnership(cls):
		ownership_definition = cls.OWNERSHIP
		if ownership_definition is None or isinstance(ownership_definition, Ownership):
			return ownership_definition
		if inspect.isclass(ownership_definition) and issubclass(
			ownership_definition, StoredObject
		):
			ownership_definition = Ownership(ownership_definition)
		if callable(ownership_definition):
			parameters = inspect.signature(ownership_definition).parameters
			ownership_definition = (
				ownership_definition(None)
				if len(parameters) == 1
				else ownership_definition()
			)
		if not isinstance(ownership_definition, Ownership):
			raise ValueError(
				f"OWNERSHIP for {cls.__name__} must resolve to Ownership or StoredObject class"
			)
		cls.OWNERSHIP = ownership_definition
		return ownership_definition

	@classmethod
	def _ensureStorage(cls) -> "ObjectStorage":
		if not cls.STORAGE:
			raise RuntimeError(
				f"Class has not been registered in an ObjectStorage yet: {cls}"
			)
		return cls.STORAGE

	@classmethod
	def Recognizes(cls, data: Any) -> bool:
		if isinstance(data, dict):
			for key in cls.PROPERTIES:
				if key not in data:
					return False
			return True
		else:
			return False

	@classmethod
	def AddIndex(cls, index: Index):
		if "INDEXES" not in cls.__dict__:
			cls.INDEXES = list(cls.INDEXES)
		if index not in cls.INDEXES:
			cls.INDEXES.append(index)
		return cls

	@classmethod
	def IndexFor(cls, name: str):
		return (cls.__dict__.get("INDEX_FOR") or {}).get(name)

	@classmethod
	def RebuildIndexes(cls) -> tuple[int, int]:
		indexes: int = 0
		objects: int = 0
		for i in cls.INDEXES:
			indexes += 1
			i.clear()
		for v in cls.All():
			objects += 1
			for i in cls.INDEXES:
				i.add(v)
		return (indexes, objects)

	@classmethod
	def GenerateID(cls):
		"""Generates a new object ID for this class"""
		id = cls.ID_GENERATOR()
		return f"{cls.ID_PREFIX}-{id}" if cls.ID_PREFIX else id

	@classmethod
	def HasPublicID(cls) -> bool:
		"""Returns whether this class opts into public ID allocation."""
		return cls.PUBLIC_ID is not None

	@classmethod
	def PublicIDScope(cls, partition: Any = NOTHING, owner: Any = NOTHING) -> str:
		"""Returns the allocation scope for a public object reference."""
		if cls.PUBLIC_ID is not PublicID.Partition:
			raise ValueError(
				f"{cls.__name__} does not define a supported public ID scope"
			)
		partition = cls.NormalizePartition(partition=partition, owner=owner)
		if partition is None:
			raise ValueError(f"{cls.__name__} requires a partition for public IDs")
		namespace = cls.PUBLIC_ID_NAMESPACE or getCanonicalName(cls)
		return asJSON([namespace, partition])

	@classmethod
	def PartitionBucket(cls, partition: Optional[Any] = None) -> str:
		if partition is None:
			return "0"
		if isinstance(partition, StoredObject):
			return str(partition.id)
		if isinstance(partition, dict):
			partition_id = partition.get("partition") or partition.get("id")
			if partition_id is not None:
				return str(partition_id)
		if isinstance(partition, (tuple, list)) and partition:
			return str(partition[0])
		return str(partition)

	@classmethod
	def OwnerBucket(cls, owner: Optional[Any] = None) -> str:
		return cls.PartitionBucket(owner)

	@classmethod
	def NormalizeID(cls, id: Any = None, owner: Any = NOTHING) -> Any:
		if isinstance(id, list):
			return id[1] if cls.GetOwnership() and len(id) == 2 else tuple(id)
		if isinstance(id, tuple) and cls.GetOwnership() and len(id) == 2:
			return id[1]
		return id

	@classmethod
	def NormalizeOwnerID(cls, owner: Any = None) -> Any:
		if owner is NOTHING:
			return NOTHING
		if owner is None:
			return None
		if isinstance(owner, StoredObject):
			return owner.id
		if isinstance(owner, dict):
			return owner.get("id")
		if isinstance(owner, (tuple, list)) and owner:
			return owner[0]
		return owner

	@classmethod
	def NormalizePartition(cls, partition: Any = NOTHING, owner: Any = NOTHING) -> Any:
		if partition is not NOTHING:
			return cls.NormalizeOwnerID(partition)
		if owner is not NOTHING:
			return cls.NormalizeOwnerID(owner)
		return None

	@classmethod
	def All(cls, since=None, order=0) -> Iterator["StoredObject"]:
		"""Iterates on all the objects of this type in the storage."""
		storage = cls._ensureStorage()
		for storage_id in cls.Keys(order=order):
			obj = storage.get(storage_id)
			if obj and (since is None or since < obj.getUpdateTime()):
				yield obj

	@classmethod
	def Keys(cls, prefix=None, order=0) -> Iterator[str]:
		"""List all the keys for objects of this type in the storage."""
		return cls._ensureStorage().keys(cls, prefix=prefix, order=order)

	# NOTE: We should return Non when the object does not exist, and provide
	# an Ensure method that will create the object if necessary.
	@classmethod
	def Get(
		cls,
		id: Optional[Any] = None,
		owner: Optional[Any] = NOTHING,
		partition: Optional[Any] = NOTHING,
		key: Optional[str] = None,
	) -> Optional["StoredObject"]:
		"""Returns the instance associated with the given Object ID, if any"""
		storage = cls._ensureStorage()
		if id is None and key is None:
			return None
		return storage.get(cls.StorageKey(id, owner=owner, partition=partition) if key is None else key)

	@classmethod
	def GetByPublicID(
		cls,
		publicID: int,
		owner: Optional[Any] = NOTHING,
		partition: Optional[Any] = NOTHING,
	) -> Optional["StoredObject"]:
		"""Returns the object identified by a partition-scoped public ID."""
		key = cls._ensureStorage().backend.getPublicObjectKey(
			cls.PublicIDScope(partition=partition, owner=owner), publicID
		)
		return cls._ensureStorage().get(key) if key is not None else None

	@classmethod
	def Count(cls) -> int:
		"""Returns the count of objects of this type stored in the storage."""
		return cls._ensureStorage().count(cls)

	@classmethod
	def List(cls, count: int = -1, start: int = 0, end: Optional[int] = None, order=0):
		"""Returns the list of objects of this type stored in the storage."""
		return cls._ensureStorage().list(cls, count, start, end, order=order)

	@classmethod
	def OwnedBy(cls, owner: "StoredObject") -> Iterator["StoredObject"]:
		"""Lists objects of this type owned by the given owner."""
		return cls._ensureStorage().ownedBy(owner, cls)

	@classmethod
	def OwnerPrefix(cls, owner: Any) -> str:
		"""Returns the storage key prefix for objects owned by the given owner."""
		return "%s.%s." % (cls.StoragePrefix(), cls.PartitionBucket(owner))

	@classmethod
	def Has(cls, id: Any, owner: Optional[Any] = NOTHING, partition: Optional[Any] = NOTHING) -> bool:
		"""Tells if there is an object stored with the given object id."""
		return cls._ensureStorage().has(cls.StorageKey(id, owner=owner, partition=partition))

	@classmethod
	def Ensure(cls, id, owner=NOTHING, partition=NOTHING):
		"""Ensures that there is an object with the given object id in the
		storage. If not, it will create a new instance of this specific
		stored object sub-class"""
		res = cls.Get(id, owner=owner, partition=partition)
		if res is None:
			res = cls(id, owner=owner, partition=partition)
		return res

	@classmethod
	def StorageKey(cls, id, owner=NOTHING, partition=NOTHING):
		"""Returns the storage key associated with the given id of this class."""
		if isinstance(id, StoredObject):
			partition = id.partition if partition is NOTHING else partition
			id = id.id
		local_id = cls.NormalizeID(id)
		partition_value = cls.NormalizePartition(partition=partition, owner=owner)
		if cls.COLLECTION:
			return str(cls.COLLECTION) + "." + cls.PartitionBucket(partition_value) + "." + str(local_id)
		else:
			cls.COLLECTION = cls.__name__.split(".")[-1]
			return cls.StorageKey(local_id, partition=partition_value)

	@classmethod
	def StoragePrefix(cls):
		"""Returns the storage prefix for storage keys of objects of this
		class."""
		if not cls.COLLECTION:
			cls.COLLECTION = cls.__name__.split(".")[-1]
		return cls.COLLECTION

	@classmethod
	def Import(cls, properties, skipExtraProperties=None, updateProperties=False):
		"""Turns the given primitive export into an instance of this class.
		Properties can be either a primtive export or a StoredObject instance,
		in which case this function will just return its parameter.
		"""
		if skipExtraProperties is None:
			skipExtraProperties = cls.SKIP_EXTRA_PROPERTIES
		if isinstance(properties, StoredObject):
			assert isinstance(properties, cls), "Expected class %s, got %s" % (
				cls,
				properties.__class__,
			)
			return properties
		else:
			id = properties.get("id")
			owner = properties.get("owner")
			partition = properties.get("partition", NOTHING)
			if cls.GetOwnership() and isinstance(id, (tuple, list)) and len(id) == 2:
				owner = owner if owner is not None else id[0]
				partition = partition if partition is not NOTHING else id[0]
				id = id[1]
			otype = properties.get("type")
			assert not otype or otype == getCanonicalName(cls), (
				"Expected type %s, got %s" % (getCanonicalName(cls), otype)
			)
			# If there is an object ID
			if id:
				# We look in the storage for this specific object
				obj = cls.Get(id, owner=owner if owner is not None else NOTHING, partition=partition)
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
					return cls(
						properties=properties, skipExtraProperties=skipExtraProperties
					)
			else:
				return cls(
					properties=properties, skipExtraProperties=skipExtraProperties
				)

	@classmethod
	def Export(cls, id, owner=NOTHING, partition=NOTHING, **options):
		"""A convenient fonction that will return the full object corresponding
		to the id if it is in base, or will return a stripped down version with
		id and class."""
		storage = cls._ensureStorage()
		o = storage.get(cls.StorageKey(id, owner=owner, partition=partition))
		if o:
			return o.export(**options)
		else:
			return {"id": asPrimitive(cls.NormalizeID(id)), "type": getCanonicalName(cls)}

	HAS_DESCRIPTORS = False

	@classmethod
	def _GenerateDescriptors(cls, instance):
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
		if not isinstance(cls.PROPERTIES, dict):
			cls.PROPERTIES = cls.PROPERTIES(instance)
		if not isinstance(cls.RELATIONS, dict):
			cls.RELATIONS = cls.RELATIONS(instance)
		cls.GetOwnership()
		for _ in cls.PROPERTIES:
			setattr(cls, _, PropertyDescriptor(_))
		for _ in cls.RELATIONS:
			setattr(cls, _, RelationDescriptor(_))
		cls.HAS_DESCRIPTORS = True
		return cls

	def __init__(
		self,
		id=None,
		properties=None,
		restored=False,
		skipExtraProperties=None,
		**kwargs,
	):
		"""Creates a new stored object instance with the given id. If none is given, then a new id will be generated."""  # If the id is not directly given, it might be listed in the properties
		if skipExtraProperties is None:
			skipExtraProperties = self.SKIP_EXTRA_PROPERTIES
		if id is None and properties:
			id = properties.get("id")
		publicID = kwargs.pop("publicId", None)
		if properties and "publicId" in properties:
			publicID = properties.pop("publicId")
		if publicID is not None and not restored:
			raise ValueError("publicId is assigned by storage and cannot be supplied")
		owner = kwargs.pop("owner", NOTHING)
		if properties and "owner" in properties and owner is NOTHING:
			owner = properties.pop("owner")
		partition = kwargs.pop("partition", NOTHING)
		if properties and "partition" in properties and partition is NOTHING:
			partition = properties.pop("partition")
		if self.__class__.GetOwnership() and isinstance(id, (tuple, list)) and len(id) == 2:
			legacy_owner, id = id
			if owner is NOTHING:
				owner = legacy_owner
			if partition is NOTHING:
				partition = legacy_owner
		# If we really can't find an id, we generate a new one
		if id is None:
			id = self.GenerateID()
		self.id = self.__class__.NormalizeID(id)
		self._publicID = int(publicID) if publicID is not None else None
		self.storage = self.STORAGE
		if not self.__class__.HAS_DESCRIPTORS:
			self.__class__._GenerateDescriptors(self)
		self._properties = {}
		self._relations = {}
		self._owner = None
		self._ownerID = None
		self._partition = None
		self._allowPartitionChange = True
		self._revision = {}
		self._isNew = restored
		if partition is NOTHING and owner is not NOTHING:
			partition = owner
		if partition is not NOTHING:
			self.setPartition(partition)
		if owner is not NOTHING:
			self.setOwner(owner)
		self.set(properties, skipExtraProperties=skipExtraProperties)
		self.set(kwargs, skipExtraProperties=skipExtraProperties)
		# We make sure revision is updated first.
		if properties:
			self._revision.update(properties.get("revision") or properties.get("updates") or {})
		if kwargs:
			self._revision.update(kwargs.get("revision") or kwargs.get("updates") or {})
		if self.STORAGE and (not self.getOwnership() or self.hasOwner() or restored):
			self._allowPartitionChange = False
			self.STORAGE.register(self, restored=restored)
		# We make sure that there's a timestamp for the object, we default it to 0
		if "id" not in self._revision:
			self._revision["id"] = getTimestamp()
		# FIXME: Should we make sure that the object had updates for everything?
		assert (not self.getOwnership() or self.id), "Owned object must have an id once created"
		self._allowPartitionChange = False
		self.init()

	@property
	def typeName(self) -> str:
		return getCanonicalName(self.__class__)

	def init(self):
		"""Can be overriden to do post-creation/import processing"""
		pass

	def set(self, propertiesAndRelations, skipExtraProperties=None, timestamp=None):
		if skipExtraProperties is None:
			skipExtraProperties = self.SKIP_EXTRA_PROPERTIES
		if propertiesAndRelations:
			for name, value in list(propertiesAndRelations.items()):
				if name in self.PROPERTIES:
					self.setProperty(name, value, timestamp)
				elif name in self.RELATIONS:
					self.setRelation(name, value, timestamp)
				elif name == "owner":
					self.setOwner(value, timestamp)
				elif name == "partition":
					self.setPartition(value, timestamp)
				elif name in self.RESERVED:
					if name in ("revision", "updates"):
						for k in value:
							self._revision[k] = max(
								value.get(k, -1), self._revision.get(k, -1)
							)
				elif name in self.COMPUTED_PROPERTIES:
					pass
				elif skipExtraProperties:
					pass
				else:
					raise ValueError(
						f"Extra property '{name}' given to {self.__class__.__name__}: {name}={value}"
					)
		return self

	def update(self, propertiesAndRelations):
		return self.set(propertiesAndRelations)

	def setProperty(self, name, value, timestamp=None):
		"""Sets a property of the given object. The property must match the
		properties defined in `PROPERTIES`"""
		# TODO: Check type
		if name not in self.PROPERTIES:
			raise ValueError(
				f"Property `{name}` not one of: {list(self.PROPERTIES.keys()) + list(self.RELATIONS.keys())}"
			)
		old_value = self.getProperty(name) if not self._isNew else None
		if not (p := self.ensureProperty(name)):
			raise ValueError(f"StoredObject does not define property {name}: {self}")
		new_value = p.set(value)
		if not self._isNew and old_value != new_value:
			# We update the `revision` map only if the object is not new (has
			# been registered)
			self._revision[name] = self._revision["id"] = max(
				getTimestamp() if timestamp is None else timestamp,
				self._revision.get(name, -1),
			)
		return self

	def setRelation(self, name, value, timestamp=None):
		"""Sets a relation of the given object. The value must match the
		definition in `RELATIONS`"""
		# TODO: Check type
		assert name in self.RELATIONS, "Relation `%s` not one of: %s" % (
			name,
			list(self.PROPERTIES.keys()) + list(self.RELATIONS.keys()),
		)
		if name not in self._relations:
			self._relations[name] = Relation(self, self.RELATIONS[name])
		self._relations[name].set(value)
		if isinstance(self.RELATIONS[name], InverseRelation):
			return self
		if not self._isNew:
			# We update the `revision` map only if the object is not new (has
			# been registered)
			self._revision[name] = self._revision["id"] = max(
				getTimestamp() if timestamp is None else timestamp,
				self._revision.get(name, -1),
			)
		return self

	def ensureProperty(self, name) -> Optional["Property"]:
		"""Returns the Property instance bound to the given name"""
		if name in self.__class__.PROPERTIES:
			if name not in self._properties:
				self._properties[name] = Property(name, self)
			return self._properties[name]
		else:
			return None

	# TODO: This is actually the property value... should be renamed.
	def getProperty(self, name: str):
		"""Returns the property value bound to the given name"""
		if name in self.__class__.PROPERTIES and (p := self.ensureProperty(name)):
			return p.get()
		else:
			raise ValueError(
				f"Property {self.__class__.__name__}.{name} is not declared in PROPERTIES"
			)

	def iterProperties(self) -> Iterator[tuple[str, Any]]:
		yield from ((_, self.getProperty(_)) for _ in self.__class__.PROPERTIES)

	def getRelation(self, name: str) -> "Relation":
		"""Returns the given relation object"""
		if name in self.__class__.RELATIONS:
			# We Lazily create the relation
			if name not in self._relations:
				self._relations[name] = Relation(self, self.RELATIONS[name])
			return self._relations[name]
		else:
			raise ValueError(
				f"Relation {self.__class__.__name__}.{name} is not declared in RELATIONS"
			)

	def iterRelations(self) -> Iterator[tuple[str, "Relation"]]:
		yield from ((_, self.getRelation(_)) for _ in self.__class__.RELATIONS)

	def getOwnership(self) -> Optional[Ownership]:
		return self.__class__.GetOwnership()

	def getOwner(self) -> Optional["StoredObject"]:
		ownership = self.getOwnership()
		owner_id = self.getOwnerID()
		if not ownership or owner_id is None:
			return None
		if self._owner and isSame(self._owner, {"type": getCanonicalName(ownership.ownerType), "id": owner_id}):
			return self._owner
		self._owner = ownership.ownerType.Get(owner_id)
		return self._owner

	def getOwnerID(self):
		return getattr(self, "_ownerID", None) if self.getOwnership() else None

	def getLocalID(self):
		return self.id

	def getPartition(self):
		return getattr(self, "_partition", None)

	def setPartition(self, partition: Optional[Any], timestamp=None) -> Self:
		partition_id = self.__class__.NormalizePartition(partition=partition)
		current = getattr(self, "_partition", None)
		if current is not None and str(current) == str(partition_id):
			return self
		if current != partition_id and not getattr(self, "_allowPartitionChange", False):
			raise ValueError(f"Partition is immutable for {self.__class__.__name__}:{self.id}")
		self._partition = partition_id
		if not self._isNew:
			self._revision["partition"] = self._revision["id"] = max(
				getTimestamp() if timestamp is None else timestamp,
				self._revision.get("partition", -1),
			)
		return self

	def hasPartition(self) -> bool:
		return self.getPartition() is not None

	partition = property(getPartition, setPartition)

	def ownerKey(self) -> Optional[str]:
		owner = self.getOwner()
		return owner.getStorageKey() if owner else None

	def setOwner(self, owner: Optional["StoredObject"], timestamp=None) -> Self:
		ownership = self.getOwnership()
		if owner is None:
			if ownership and ownership.required:
				raise ValueError(
					f"{self.__class__.__name__} requires an owner of type {ownership.ownerType.__name__}"
				)
			self._owner = None
			self._ownerID = None
			return self
		if not ownership:
			raise ValueError(f"{self.__class__.__name__} does not declare OWNERSHIP")
		restored = restore(owner)
		if isinstance(restored, StoredObject):
			if not isinstance(restored, ownership.ownerType):
				raise ValueError(
					f"{self.__class__.__name__} expects owner of type {ownership.ownerType.__name__}, got {type(restored).__name__}"
				)
			owner_id = restored.id
			self._owner = restored
		else:
			owner_id = self.__class__.NormalizeOwnerID(restored)
			self._owner = None
		if owner_id is None:
			raise ValueError(
				f"Owner must be a stored object or owner id, got {type(restored)}: {restored}"
			)
		current_owner_id = self.getOwnerID()
		if (
			current_owner_id is not None
			and str(current_owner_id) != str(owner_id)
			and not getattr(self, "_allowPartitionChange", False)
		):
			raise ValueError(f"Owner is immutable for {self.__class__.__name__}:{self.id}")
		self._ownerID = owner_id
		if self.getPartition() is None:
			self.setPartition(owner_id, timestamp=timestamp)
		if not self._isNew:
			self._revision["owner"] = self._revision["id"] = max(
				getTimestamp() if timestamp is None else timestamp,
				self._revision.get("owner", -1),
			)
		return self

	def hasOwner(self) -> bool:
		return self.getOwnerID() is not None

	owner = property(getOwner, setOwner)

	def validateOwnership(self) -> Self:
		ownership = self.getOwnership()
		if ownership and ownership.required and self.getOwnerID() is None:
			raise ValueError(
				f"{self.__class__.__name__} requires an owner of type {ownership.ownerType.__name__}"
			)
		return self

	def iterReferences(self, limit: int = -1) -> Iterator["StoredObject"]:
		if limit != 0:
			for o in (
				v for _, v in self.iterProperties() if isinstance(v, StoredObject)
			):
				yield o
				yield from o.iterReferences(limit=limit - 1)
			for _, r in self.iterRelations():
				if r.isInverse():
					continue
				for o in r:
					if isinstance(o, StoredObject):
						yield o
						yield from o.iterReferences(limit=limit - 1)

	def getUpdateTime(self, key="id") -> int:
		"""Returns the time at with the given object (or key) was updated. The time
		is returned as a storage timestamp."""
		if self._revision:
			return self._revision.get(key, 0)
		else:
			return 0

	def getStorageKey(self) -> str:
		"""Returns the key used to store this object in a storage."""
		return self.__class__.StorageKey(self.id, partition=self.partition)

	def getPublicID(self) -> Optional[int]:
		return self._publicID

	def _setPublicID(self, publicID: int) -> Self:
		if not self.__class__.HasPublicID():
			raise ValueError(f"{self.__class__.__name__} does not use public IDs")
		if self._publicID is not None and self._publicID != publicID:
			raise ValueError(f"Public ID is immutable for {self.__class__.__name__}:{self.id}")
		if publicID <= 0:
			raise ValueError(f"Public ID must be positive, got: {publicID}")
		self._publicID = publicID
		return self

	publicId = property(getPublicID)

	def setStorage(self, storage: "ObjectStorage") -> Self:
		"""Sets the storage object associated with this object."""
		# NOTE: For now we just expect the storage not to change... but maybe
		# there is a case where we'd need multiple storages
		if self.storage and self.storage != storage:
			raise RuntimeError(
				f"StoredObject already has an assigned storage {self.storage}: {self}"
			)
		self._isNew = False
		self.storage = storage
		return self

	def getCollection(self) -> str:
		"""Returns the collection name for this stored object"""
		return self.COLLECTION or self.__class__.__name__

	def remove(self) -> bool:
		"""Removes the stored element from the storage."""
		# print "[DEBUG] Removing stored element", self.__class__.__name__, "|", self.id, "|", self
		if self.storage:
			self.storage.remove(self)
			return True
		else:
			return False

	def save(self) -> Self:
		"""Saves this object to the storage."""
		if not self.storage:
			raise RuntimeError(f"StoredObject has no assigned storage: {self}")
		self.validateOwnership()
		key = self.getStorageKey()
		if self.storage.has(key):
			self.storage.update(self)
		else:
			self.storage.create(self)
		return self

	def onStore(self, d: TPrimitive) -> TPrimitive:
		"""Processes the dictionary that will be stored as a value in the
		storage back-end. Override this to remove non-picklable values."""
		return d

	def onRestore(self):
		"""Invoked when the stored element is restored from the database. This
		registers the object in the database cache."""
		# print "XXX ON RESTORE", self.id, self
		self.storage.register(self)

	def onRemove(self):
		"""Invoked after the element is removed from the cache"""
		pass

	def __getstate__(self) -> TPrimitive:
		"""This strips the state of events, and object storage reference which
		cannot really be pickled."""
		d = self.__dict__
		s = d.copy()
		s = self.onStore(s)
		# for k, v in list(d.items()):
		#     pass
		#     # FIXME: We should have a more generic mechanism
		#     # if isinstance(v, Event) or isinstance(v, ObjectStorage):
		#     # 	del s[k]
		return s

	def __setstate__(self, state: TPrimitive):
		"""Sets the state of this object given a dictionary loaded from the
		object storage. Override this to re-construct the object state from
		what is returned by `__getstate__`"""
		if "_updates" in state and "_revision" not in state:
			state["_revision"] = state.pop("_updates")
		if self.__class__.GetOwnership() and isinstance(state.get("id"), (tuple, list)) and len(state.get("id")) == 2:
			owner_id, local_id = state["id"]
			state["id"] = local_id
			state.setdefault("_ownerID", owner_id)
			state.setdefault("_partition", owner_id)
		state.setdefault("_ownerID", None)
		state.setdefault("_partition", None)
		state.setdefault("_publicID", None)
		state.setdefault("_allowPartitionChange", False)
		self.__dict__.update(state)
		self.__dict__["storage"] = self.STORAGE
		# FIXME: Should not be direct like that
		assert self.getStorageKey() not in self.STORAGE._cache, (
			"StoredObject already in cache: %s:%s" % (self.id, self)
		)
		self.onRestore()
		# print "[DEBUG] Setting state for", self.id, "|", self.__class__.__name__,  "|",  self

	def exportWith(self, *keys: str, depth: int = 1):
		res: dict[str, TPrimitive] = {}
		for key in keys:
			if key == "id":
				res[key] = asPrimitive(self.id)
			elif key == "type":
				res[key] = self.getTypeName()
			elif key in self.PROPERTIES:
				value = self.getProperty(key)
				if value is not None:
					res[key] = asPrimitive(value, depth=depth - 1)
			elif key in self.RELATIONS:
				if isinstance(self.RELATIONS[key], InverseRelation):
					continue
				relation = getattr(self, key)
				res[key] = asPrimitive(relation, depth=depth - 1)
		return res

	def export(self, **options):
		"""Returns a dictionary representing this object. By default, it
		just returns the object id (`id`) and its class (`class`)."""
		# SEE: http://stackoverflow.com/questions/1379934/large-numbers-erroneously-rounded-in-javascript
		# We cannot allow IDs to be long numbers...
		res = {
			"id": asPrimitive(self.id),
			"type": self.getTypeName(),
			"revision": self._revision,
		}
		if self.publicId is not None:
			res["publicId"] = self.publicId
		if self.getPartition() is not None:
			res["partition"] = asPrimitive(self.getPartition())
		if self.getOwnership():
			owner_id = self.getOwnerID()
			if owner_id is not None:
				res["owner"] = asPrimitive(owner_id)
		depth = 1
		if "depth" in options:
			depth = options["depth"]
		if depth > 0:
			value = None
			for key in self.PROPERTIES:
				value = self.getProperty(key)
				if value is not None:
					res[key] = asPrimitive(value, depth=depth - 1)
			for key in self.RELATIONS:
				if (
					isinstance(self.RELATIONS[key], InverseRelation)
					and options.get("target") != "web"
				):
					continue
				relation = getattr(self, key)
				res[key] = asPrimitive(relation, depth=depth - 1)
		return res

	def getTypeName(self):
		return getCanonicalName(self.__class__)

	def asJSON(self, jsonifier=asJSON, **options):
		"""Returns a JSON representation of this object using the
		given 'jsonifier' and 'options'"""
		return jsonifier(self.export(**options))

	def __repr__(self):
		return "<obj:%s %s:%s>" % (self.__class__.__name__, id(self), self.id)


__all__ = [
	"InverseRelation",
	"Ownership",
	"PublicID",
	"Property",
	"Relation",
	"StoredObject",
]


# EOF
