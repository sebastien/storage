"""Stored raw payload model.

This module contains the raw value model independently from the persistence
runtime in :mod:`storage.raw`.
"""

import base64
import io

from ..core import Identifier, NOTHING, Storable, getCanonicalName, getTimestamp


def _coerceCacheKey(rawOrKey):
	return rawOrKey.id if isinstance(rawOrKey, StoredRaw) else rawOrKey


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
			data = meta.get("data") if data is None else None
			if "data" in meta:
				data = base64.b64decode(data)
				meta = dict(((k, v) for k, v in list(meta.items()) if k != "data"))
			if id:
				obj = cls.Get(id)
				if obj:
					for key, value in list(meta.items()):
						if key == "type" or key == "id":
							continue
						elif key in ("revision", "updates"):
							obj._revision = value
						else:
							obj.meta(key, value)
					return obj
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
	def Export(cls, id, **options):
		"""Returns the stored raw payload, or a transient placeholder if missing."""
		obj = cls.Get(id)
		if obj:
			return obj.export(**options)
		return {"id": id, "type": getCanonicalName(cls)}

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
		"""Returns the storage prefix for storage keys of objects of this class."""
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
			try:
				self._data.close()
			except Exception:
				pass
			self._data = None
		self._hasDataChanged = False

	def setData(self, data, timestamp=None):
		self._data = data
		self._hasDataChanged = True
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
		"""Returns the update time for the object or the given key."""
		return self._revision.get(key, 0) if self._revision else 0

	def update(self, propertiesAndRelations):
		"""Compatibility method for Storable, is an alias to setMeta."""
		return self.setMeta(propertiesAndRelations)

	def meta(self, name=NOTHING, value=NOTHING, **options):
		"""Returns the meta data."""
		if name is NOTHING:
			if options:
				for key in options:
					if key not in self.RESERVED:
						self._meta[key] = options[key]
				return self
			return self._meta
		if value is NOTHING:
			if isinstance(name, dict):
				for key in name:
					if key not in self.RESERVED:
						self._meta[key] = name[key]
			else:
				return self._meta.get(name)
		else:
			assert name not in self.RESERVED, "Reserved meta property: {0}".format(name)
			self._meta[name] = value
			return self

	def data(self, size=None):
		"""Iterates through the data with chunks of the given size."""
		if self._data:
			yield self._data
		elif self.STORAGE:
			for chunk in self.STORAGE.streamData(self, size=None):
				if chunk is not None:
					yield chunk

	def loadData(self):
		v = io.BytesIO()
		for d in self.data():
			if d is not None:
				v.write(d)
		return v.getvalue()

	def path(self):
		"""Returns the path of the data file."""
		if self.STORAGE:
			return self.STORAGE.path(self)
		raise Exception("No storage attached to stored raw: {0}".format(self))

	def length(self):
		return len(self._data) if self._data else None

	def export(self, **options):
		depth = options.get("depth", 1)
		res = dict(id=str(self.id), type=self.getTypeName(), revision=self._revision)
		if depth > 0:
			res.update(self._meta)
		if options.get("data"):
			res["data"] = base64.b64encode(self.loadData())
		return res

	def getTypeName(self):
		return getCanonicalName(self.__class__)

	def save(self):
		self._ensureStorage().update(self)

	def __repr__(self):
		return "<raw:%s %s:%s>" % (self.__class__.__name__, id(self), self.id)


# Keep persisted names stable while allowing the implementation to live here.
StoredRaw.__module__ = "storage.raw"


__all__ = ["StoredRaw", "_coerceCacheKey"]
