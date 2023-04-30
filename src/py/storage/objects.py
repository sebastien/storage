import time, threading, json, weakref, types, datetime, traceback, inspect
from typing import ClassVar, Self, Callable, Optional, Iterator, Any, List
from .backends import StorageBackend
from .index import Index
from .utils import atomic, TPrimitive
from .core import (
    Storable,
    Identifier,
    getCanonicalName,
    asPrimitive,
    asJSON,
    restore,
    isSame,
    getTimestamp,
)

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

    OID_GENERATOR: ClassVar[Callable[[], int]] = Identifier.OID
    SKIP_EXTRA_PROPERTIES: ClassVar[bool] = False
    COLLECTION = None
    STORAGE: ClassVar[Optional["ObjectStorage"]] = None
    PROPERTIES: ClassVar[dict[str, Any]] = {}
    COMPUTED_PROPERTIES: ClassVar[list[str]] = []
    RELATIONS = {}
    RESERVED: ClassVar[list[str]] = ["type", "oid", "updates"]
    INDEXES: ClassVar[list[Index]] = []

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
        if index not in cls.INDEXES:
            cls.INDEXES.append(index)
        return cls

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
    def GenerateOID(cls):
        """Generates a new object ID for this class"""
        return cls.OID_GENERATOR()

    @classmethod
    def All(cls, since=None) -> Iterator["StoredObject"]:
        """Iterates on all the objects of this type in the storage."""
        if not cls.STORAGE:
            raise RuntimeError(
                f"Class has not been registered in an ObjectStorage yet: {cls}"
            )
        for storage_id in cls.Keys():
            obj = cls.STORAGE.get(storage_id)
            if obj and (since is None or since < obj.getUpdateTime()):
                yield obj

    @classmethod
    def Keys(cls, prefix=None) -> Iterator[str]:
        """List all the keys for objects of this type in the storage."""
        if not cls.STORAGE:
            raise RuntimeError(
                f"Class has not been registered in an ObjectStorage yet: {cls}"
            )
        return cls.STORAGE.keys(cls, prefix=prefix)

    # NOTE: We should return Non when the object does not exist, and provide
    # an Ensure method that will create the object if necessary.
    @classmethod
    def Get(
        cls, oid: Optional[str] = None, key: Optional[str] = None
    ) -> Optional["StoredObject"]:
        """Returns the instance associated with the given Object ID, if any"""
        if not cls.STORAGE:
            raise RuntimeError(
                f"Class has not been registered in an ObjectStorage yet: {cls}"
            )
        if oid is None and key is None:
            return None
        return cls.STORAGE.get(cls.StorageKey(oid) if key is None else key)

    @classmethod
    def Count(cls) -> int:
        """Returns the count of objects of this type stored in the storage."""
        if not cls.STORAGE:
            raise RuntimeError(
                f"Class has not been registered in an ObjectStorage yet: {cls}"
            )
        return cls.STORAGE.count(cls)

    @classmethod
    def List(cls, count: int = -1, start: int = 0, end: Optional[int] = None):
        """Returns the list of objects of this type stored in the storage."""
        if not cls.STORAGE:
            raise RuntimeError(
                f"Class has not been registered in an ObjectStorage yet: {cls}"
            )
        return cls.STORAGE.list(cls, count, start, end)

    @classmethod
    def Has(cls, oid: str) -> bool:
        """Tells if there is an object stored with the given object id."""
        if not cls.STORAGE:
            raise RuntimeError(
                f"Class has not been registered in an ObjectStorage yet: {cls}"
            )
        return cls.STORAGE.has(cls.StorageKey(oid))

    @classmethod
    def Ensure(cls, oid):
        """Ensures that there is an object with the given object id in the
        storage. If not, it will create a new instance of this specific
        stored object sub-class"""
        res = cls.Get(oid)
        if res is None:
            res = cls(oid)
        return res

    @classmethod
    def StorageKey(cls, oid):
        """Returns the storage key associated with the given oid of this class."""
        if isinstance(oid, StoredObject):
            oid = oid.oid
        if cls.COLLECTION:
            return str(cls.COLLECTION) + "." + str(oid)
        else:
            cls.COLLECTION = cls.__name__.split(".")[-1]
            return cls.StorageKey(oid)

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
            oid = properties.get("oid")
            otype = properties.get("type")
            assert not otype or otype == getCanonicalName(
                cls
            ), "Expected type %s, got %s" % (getCanonicalName(cls), otype)
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
                    return cls(
                        properties=properties, skipExtraProperties=skipExtraProperties
                    )
            else:
                return cls(
                    properties=properties, skipExtraProperties=skipExtraProperties
                )

    @classmethod
    def Export(cls, oid, **options):
        """A convenient fonction that will return the full object corresponding
        to the oid if it is in base, or will return a stripped down version with
        oid and class."""
        o = cls.STORAGE.get(cls.StorageKey(oid))
        if o:
            return o.export(**options)
        else:
            return {"oid": oid, "type": getCanonicalName(cls)}

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
        if type(cls.PROPERTIES) != dict:
            cls.PROPERTIES = cls.PROPERTIES(instance)
        if type(cls.RELATIONS) != dict:
            cls.RELATIONS = cls.RELATIONS(instance)
        for _ in cls.PROPERTIES:
            setattr(cls, _, PropertyDescriptor(_))
        for _ in cls.RELATIONS:
            setattr(cls, _, RelationDescriptor(_))
        cls.HAS_DESCRIPTORS = True
        return cls

    def __init__(
        self,
        oid=None,
        properties=None,
        restored=False,
        skipExtraProperties=None,
        **kwargs,
    ):
        """Creates a new stored object instance with the given oid. If none is given, then a new oid will be generated."""  # If the oid is not directly given, it might be listed in the properties
        if skipExtraProperties is None:
            skipExtraProperties = self.SKIP_EXTRA_PROPERTIES
        if oid is None and properties:
            oid = properties.get("oid")
        # If we really can't find an oid, we generate a new one
        if oid is None:
            self.oid = self.GenerateOID()
        else:
            self.oid = oid
        self.storage = self.STORAGE
        if not self.__class__.HAS_DESCRIPTORS:
            self.__class__._GenerateDescriptors(self)
        self._properties = {}
        self._relations = {}
        self._updates = {}
        self._isNew = restored
        self.set(properties, skipExtraProperties=skipExtraProperties)
        self.set(kwargs, skipExtraProperties=skipExtraProperties)
        # We make sure updates are updated first
        if properties and "updates" in properties:
            self._updates.update(properties.get("updates"))
        if kwargs and "updates" in kwargs:
            self._updates.update(kwargs.get("updates"))
        if self.STORAGE:
            self.STORAGE.register(self, restored=restored)
        # We make sure that there's a timestamp for the object, we default it to 0
        if "oid" not in self._updates:
            self._updates["oid"] = 0
        # FIXME: Should we make sure that the object had updates for everything?
        assert self.getStorageKey(), "Object must have a key once created"
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
                elif name in self.RESERVED:
                    if name == "updates":
                        # If the reserved keyword is "updates", we refresh the updates
                        for k in value:
                            self._updates[k] = max(
                                value.get(k, -1), self._updates.get(k, -1)
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
            # We update the `updates` map only if the object is not new (has
            # been registered)
            self._updates[name] = self._updates["oid"] = max(
                getTimestamp() if timestamp is None else timestamp,
                self._updates.get(name, -1),
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
        if not name in self._relations:
            self._relations[name] = Relation(self.__class__, self.RELATIONS[name])
        self._relations[name].set(value)
        if not self._isNew:
            # We update the `updates` map only if the object is not new (has
            # been registered)
            self._updates[name] = self._updates["oid"] = max(
                getTimestamp() if timestamp is None else timestamp,
                self._updates.get(name, -1),
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
            if not name in self._relations:
                self._relations[name] = Relation(self, self.RELATIONS[name])
            return self._relations[name]
        else:
            raise ValueError(
                f"Relation {self.__class__.__name__}.{name} is not declared in RELATIONS"
            )

    def iterRelations(self) -> Iterator[tuple[str, "Relation"]]:
        yield from ((_, self.getRelation(_)) for _ in self.__class__.RELATIONS)

    def iterReferences(self, limit: int = -1) -> Iterator["StoredObject"]:
        if limit != 0:
            for o in (
                v for _, v in self.iterProperties() if isinstance(v, StoredObject)
            ):
                yield o
                yield from o.iterReferences(limit=limit - 1)
            for _, r in self.iterRelations():
                for o in r:
                    if isinstance(o, StoredObject):
                        yield o
                        yield from o.iterReferences(limit=limit - 1)

    def getID(self) -> str:
        return self.oid

    def getUpdateTime(self, key="oid") -> int:
        """Returns the time at with the given object (or key) was updated. The time
        is returned as a storage timestamp."""
        if self._updates:
            return self._updates.get(key, 0)
        else:
            return 0

    def getStorageKey(self) -> str:
        """Returns the key used to store this object in a storage."""
        return self.__class__.StorageKey(self.oid)

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
        # print "[DEBUG] Removing stored element", self.__class__.__name__, "|", self.oid, "|", self
        if self.storage:
            self.storage.remove(self)
            return True
        else:
            return False

    def save(self) -> Self:
        """Saves this object to the storage."""
        if not self.storage:
            raise RuntimeError(f"StoredObject has no assigned storage: {self}")
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
        # print "XXX ON RESTORE", self.oid, self
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
        self.__dict__.update(state)
        self.__dict__["storage"] = self.STORAGE
        # FIXME: Should not be direct like that
        assert (
            self.getStorageKey() not in self.STORAGE._cache
        ), "StoredObject already in cache: %s:%s" % (self.oid, self)
        self.onRestore()
        # print "[DEBUG] Setting state for", self.oid, "|", self.__class__.__name__,  "|",  self

    def exportWith(self, *keys: str, depth: int = 1):
        res: dict[str, TPrimitive] = {}
        for key in keys:
            if key == "oid":
                res[key] = str(self.oid)
            elif key == "type":
                res[key] = self.getTypeName()
            elif key in self.PROPERTIES:
                value = self.getProperty(key)
                if value is not None:
                    res[key] = asPrimitive(value, depth=depth - 1)
            elif key in self.RELATIONS:
                relation = getattr(self, key)
                res[key] = asPrimitive(relation, depth=depth - 1)
        return res

    def export(self, **options):
        """Returns a dictionary representing this object. By default, it
        just returns the object id (`oid`) and its class (`class`)."""
        # SEE: http://stackoverflow.com/questions/1379934/large-numbers-erroneously-rounded-in-javascript
        # We cannot allow IDs to be long numbers...
        res = {
            "oid": str(self.oid),
            "type": self.getTypeName(),
            "updates": self._updates,
        }
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
        return "<obj:%s %s:%s>" % (self.__class__.__name__, id(self), self.oid)


# -----------------------------------------------------------------------------
#
# PROPERTY DESCRIPTOR
#
# -----------------------------------------------------------------------------


class PropertyDescriptor(object):
    """Provides transparent access to setProperty/getProperty to StoredObjects."""

    def __init__(self, name):
        self.name = name

    def __get__(self, instance, owner):
        # NOTE: Instance is null when the descriptor is directly accessed
        # at class level, in which case we return the descriptor itself
        if not instance:
            return self
        return instance.getProperty(self.name)

    def __set__(self, instance, value):
        assert instance, "Property descriptors cannot be re-assigned in class"
        return instance.setProperty(self.name, value)


# -----------------------------------------------------------------------------
#
# RELATION DESCRIPTOR
#
# -----------------------------------------------------------------------------


class RelationDescriptor(object):
    """Provides transparent access to setRelation/getRelation to StoredObjects."""

    def __init__(self, name):
        self.name = name

    def __get__(self, instance, owner):
        # See not on PropertyDescriptor
        if not instance:
            return self
        return instance.getRelation(self.name)

    def __set__(self, instance, value):
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

    def __init__(self, name, storedObject):
        self.name = name
        self.value = None
        self.restored = False
        assert storedObject, "Property requires stored object instance"
        self.storedObject = storedObject
        cap_name = name[0].upper() + name[1:]
        setter_name = "set" + cap_name
        getter_name = "get" + cap_name
        self._setter = (
            getattr(storedObject, setter_name)
            if hasattr(storedObject, setter_name)
            else None
        )
        self._getter = (
            getattr(storedObject, getter_name)
            if hasattr(storedObject, getter_name)
            else None
        )

    def set(self, value):
        if self._setter:
            # We only use the setter if it is defined
            old_value = value
            value = self._setter(value)
            # If the setter returns None or self, we restore the old_value
            if value is None or value is self.storedObject:
                value = old_value
        self.value = value
        assert not isinstance(value, Property)
        self.restored = False
        return self.value

    def get(self):
        value = None
        if not self.restored:
            value = self.value = restore(self.value) if self.value else self.value
            self.restored = True
            if self._getter:
                old_value = value
                value = self._getter(value)
                # If the setter returns None or self, we restore the old_value
                if value is None or value is self.storedObject:
                    value = old_value
        return self.value

    def export(self, **options) -> TPrimitive:
        if "depth" not in options:
            options["depth"] = 0
        return asPrimitive(self.value, **options)

    def __repr__(self):
        return "@property:" + repr(self.value)


# -----------------------------------------------------------------------------
#
# RELATION
#
# -----------------------------------------------------------------------------


class Relation:
    """Represents a relation between one object and another. This is a one-to-many
    relationship that can be lazily loaded."""

    def __init__(self, parentClass, definition):
        # FIXME: Parent introduces a circular reference
        self.parentClass = parentClass
        self.definition = definition
        self.values = None

    def init(self, values):
        """Initializes the relation with the given values"""
        self.values = values
        return self

    def add(self, value):
        return self.append(value)

    def append(self, value):
        if not value:
            return self
        if not (isinstance(value, dict) or isinstance(value, StoredObject)):
            raise ValueError(
                f"Relation only accepts object or exported object, got {type(value)}: {value}"
            )
        restored: StoredObject = restore(value)
        if not isinstance(
            restored, self.getRelationClass()
        ) or restored.typeName != getCanonicalName(self.getRelationClass()):
            raise ValueError(
                f"Relation expects value of type {self.getRelationClass()}, got {type(restored)}: {restored}"
            )
        # We create values if empty
        if self.values is None:
            self.values = []
        if not self.isMany() and len(self.values):
            raise RuntimeError(
                f"Cannot append to a single value relation, relation has {len(self.values)} values: {restored}"
            )
        else:
            self.values.append(restored)
        return self

    def remove(self, value):
        if not value:
            return self
        self.values = [_ for _ in self.get(resolve=False) if not isSame(_, value)]
        return self

    def clear(self):
        self.values = []
        return self

    def set(self, values):
        is_many = self.isMany()
        relation_class = self.getRelationClass()
        self.clear()
        if type(values) not in (list, tuple):
            values = (values,)
        list(map(self.add, values))
        return self

    # FIXME: Should have better access methods to return one or many
    def get(self, start=0, limit=-1, resolve=True, depth=0):
        # FIXME: We should always have resolve=True, as otherwise the data
        # might get out of sync. For instance, in ARTNet when a TutorialStep changes
        # its media, the serialized version of TutorialStep on disk might change
        is_many = self.isMany()
        relation_class = self.getRelationClass()
        values = self.values
        i = 0
        if values is not None:
            for v in values:
                if i >= start and (limit == -1 or i < limit):
                    if resolve:
                        # If we resolve the value, we make sure to give
                        # and actual storable
                        if not isinstance(v, Storable):
                            if type(v) is dict:
                                yield restore(v)
                            else:
                                # NOTE: This will nor work if relation_class
                                # is not the direct class to instanciate.
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
                            yield {"oid": v["oid"], "type": v["type"]}
                        else:
                            yield v
                i += 1

    def one(self, index=0):
        try:
            return next(self.get(resolve=True, start=index))
        except StopIteration as e:
            return None

    def isEmpty(self) -> bool:
        try:
            next(self.get(resolve=False))
            return False
        except StopIteration as e:
            return True

    def contains(self, objectOrID: StoredObject | str) -> bool:
        """Alias for has"""
        return self.has(objectOrID)

    def has(self, objectOrID: StoredObject | str) -> bool:
        oid = objectOrID.oid if isinstance(objectOrID, StoredObject) else objectOrID
        for v in self.get(resolve=False):
            if isinstance(v, dict) and "oid" in v and "type" in v and v["oid"] == oid:
                return True
        return False

    def list(self):
        return self.get(resolve=True)

    def all(self):
        """Alias for `list()`"""
        return self.list()

    def isMany(self) -> bool:
        return isinstance(self.definition, tuple) or isinstance(self.definition, list)

    def getRelationClass(self):
        if self.isMany():
            return self.definition[0]
        else:
            return self.definition

    def export(self, **options) -> List[TPrimitive]:
        o = {}
        o.update(options)
        # FIXME: For serialization we want relations to be shallow (oid/type)
        # We change depth as we want relations to be transparent
        if "depth" in o:
            o["depth"] += 1
        # FIXME: We have to be very clear about the resolve here -- is it a
        # good thing?
        # FIXME: What do we do if an element is referenced but got removed?
        return [
            asPrimitive(_, **o) for _ in self.get(resolve=options.get("resolve", True))
        ]

    def __len__(self) -> int:
        if self.values:
            return len(self.values)
        else:
            return 0

    def __call__(self, *args, **kwargs):
        return self.get(*args, **kwargs)

    def __getitem__(self, key):
        if type(key) not in (
            int,
            float,
        ):
            raise IndexError(f"Relations can only be queried by index, got: {key}")
        elif key < 0:
            key = max(0, len(self) + key)
        for _ in self.get():
            if key == 0:
                return _
            else:
                key -= 1
        return None

    def __iter__(self):
        return self.get(resolve=True)

    def __repr__(self):
        return "<relation:%s=%s>" % (self.definition, self.values)

    def __delete__(self, instance, owner):
        self.clear()
        return self


# -----------------------------------------------------------------------------
#
# OBJECT STORAGE
#
# -----------------------------------------------------------------------------


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

    def __init__(self, backend: StorageBackend):
        self.backend = backend
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

    def _restore(self, exportedStoredObject: dict[str, Any]) -> StoredObject:
        # NOTE: We call restore only when the object was not already in cache
        # NOTE: Exported stored object  is expected to be a dict as give
        # by StoredObject.export
        assert (
            type(exportedStoredObject) is dict
        ), "Expected a dictionary as exported by StoredObject.export(), got a %s" % (
            type(exportedStoredObject)
        )
        oid = exportedStoredObject["oid"]
        oclass = exportedStoredObject["type"]
        # FIXME: Should check if the exported stored object is in cache first!
        actual_class = self._declaredClasses.get(oclass)
        if actual_class:
            key = actual_class.StorageKey(oid)
            assert key not in self._cache
            # We instanciate the object, which will then be available in the cache, as
            # the constructor calls Storage.register.
            new_object = actual_class(oid, exportedStoredObject, restored=True)
            assert key in self._cache
            return new_object
        else:
            raise Exception("Class not registered in ObjectStorage: %s" % (oclass))

    def add(self, storedObject: StoredObject, creation: bool = False):
        """Sets the given value to the given key, storing it in cache. Note that
        this does not store all referenced objects."""
        self.lock.acquire()
        try:
            # if True:
            key = storedObject.getStorageKey()
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
            if isinstance(StoredObject, StoredObject):
                storedObject.setStorage(self)
            self.lock.release()
        except Exception as e:
            # We make sure to always release the lock here
            self.lock.release()
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
                    assert isinstance(
                        value, StoredObject
                    ), "Stored object expected, got: %s" % (value)
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

    def keys(self, storedObjectClasses=None, prefix=None):
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
    def list(self, storedObjectClasses=None, count=-1, start=0, end=None):
        """Lists (iterates) the stored objects belonging to the given class. Note that
        there is no guaranteed ordering in the keys, so this might return different
        results depending on how many keys there are."""
        end = end if end >= 0 else (start + count if count > 0 else None)
        i = 0
        for key in self.keys(storedObjectClasses):
            if count != 0:
                if i >= start and (i < end or end is None):
                    if count > 0:
                        count -= 1
                    yield self.get(key)
            i += 1

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
        if key in self._cache:
            del self._cache[key]
        # We update the indexes
        if hasattr(old_value, "INDEXES"):
            for index in old_value.INDEXES or ():
                index.remove(old_value)
                index.save()
        self.backend.remove(key)
        if old_value and isinstance(old_value, StoredObject):
            old_value.onRemove()

    def sync(self):
        """Synchronizes the modifications with the backend."""
        # We store the cached objects in the db, prefetching the keys as the
        # dictionary may change during iteration
        keys = list(self._cache.keys())
        for key, storedObject in list(self._syncQueue.items()):
            v = storedObject
            if v:
                self.backend.update(key, v.export())
        self.backend.sync()

    def use(self, *classes):
        """Makes this storage register itself with the given classes."""
        for c in classes:
            name = getCanonicalName(c)
            c.STORAGE = self
            if name not in self._declaredClasses:
                self._declaredClasses[name] = c
                Storable.DeclareClass(c)
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


# EOF
