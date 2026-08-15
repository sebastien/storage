"""Index extraction, registry, and index storage runtime."""

from typing import Type

from .backends import StorageBackend
from .core import Storable, getCanonicalName
from .indexing import Indexing as Indexing, RE_NOALPHANUM as RE_NOALPHANUM, RE_SPACES as RE_SPACES
from .index_storage import IndexStorage as IndexStorage


# -----------------------------------------------------------------------------
#
# INDEX REGISTRY
#
# -----------------------------------------------------------------------------


# FROM: http://code.activestate.com/recipes/473786-dictionary-with-attribute-style-access/
class AttrDict(dict):
	"""A dictionary with attribute-style access. It maps attribute access to
	the real dictionary."""

	def __init__(self, init={}):
		dict.__init__(self, init)

	def __getstate__(self):
		return list(self.__dict__.items())

	def __setstate__(self, items):
		for key, val in items:
			self.__dict__[key] = val

	def __repr__(self):
		return "%s(%s)" % (self.__class__.__name__, dict.__repr__(self))

	def __setitem__(self, key, value):
		return super(AttrDict, self).__setitem__(key, value)

	def __getitem__(self, name):
		return super(AttrDict, self).__getitem__(name)

	def __delitem__(self, name):
		return super(AttrDict, self).__delitem__(name)

	__getattr__ = __getitem__
	__setattr__ = __setitem__

	def copy(self):
		ch = AttrDict(self)
		return ch


class Indexes:
	"""Manages a collection of indexes."""

	# TODO: Document shortcut usage
	def __init__(self, backendClass: Type[StorageBackend], prefix=""):
		self.backendClass = backendClass
		self.prefix = prefix
		self.indexes: list[tuple[int, Index]] = []

	def all(self):
		"""Lists all the indexes registered in this index registry"""
		return [_[0] for _ in self.indexes]

	def rebuild(self, sync=False) -> int:
		"""Lists all the indexes registered in this index registry"""
		count = 0
		for index, storable_class in self.indexes:
			count += index.rebuild(storable_class.All())
			if sync:
				index.save()
		return count

	def use(self, *storableClasses):
		for c in storableClasses:
			if type(c) in (list, tuple):
				self.use(*c)
				continue
			if not hasattr(c, "INDEX_BY"):
				continue
			path = self.prefix + getCanonicalName(c)
			for indexed_property, indexing_function in list(c.INDEX_BY.items()):
				index_path = path + "." + indexed_property
				# FIXME: Should provide a single backend for both forward and backward, no?
				storage = IndexStorage(
					self.backendClass(index_path + "-fwd"),
					self.backendClass(index_path + "-bwd"),
				)
				extractor, restorer = self._createIndexFunctions(
					indexed_property, indexing_function, c
				)
				index = Index(storage, extractor=extractor, restorer=restorer)
				c.AddIndex(index)
				self.indexes.append((index, c))
				# We register shortcuts so that <StorableClass>.by.<property> will
				# give access to the index
				name = c.__name__.split(".")[-1]
				if not hasattr(self, name):
					setattr(self, name, AttrDict())
				d = getattr(self, name)
				d.setdefault("by", AttrDict())
				d.by[indexed_property] = index
		# We build indexes that need to be built
		for index, storable_class in self.indexes:
			if not index.STORAGE.getLastUpdate():
				index.rebuild(storable_class.All())
				index.save()
		return self


# -----------------------------------------------------------------------------
#
# INDEX API
#
# -----------------------------------------------------------------------------

	def _createIndexFunctions(self, name, extractor, storableClass):
		def r(value, storableClass=storableClass):
			return storableClass.STORAGE.get(value)

		if name == "_":

			def e1(value, name=name, extractor=extractor):
				return extractor(None, value)

			return e1, r
		else:

			def e2(value, name=name, extractor=extractor):
				return extractor(
					hasattr(value, name) and getattr(value, name) or None, value
				)

			return e2, r


class Index:
	STORAGE = None

	def __init__(self, storage, extractor=None, restorer=None):
		self.extractor = extractor
		self.restorer = restorer
		self.STORAGE = storage

	def getSignature(self, value):
		"""Returns the signature of the value, which will allow to find
		it in the reverse index. This will typically be the StoredObject
		`getStorageKey` method."""
		if isinstance(value, Storable):
			return value.getStorageKey()
		else:
			raise Exception("Index only support stored object for now")

	def _restoreValue(self, value):
		if self.restorer:
			return self.restorer(value)
		else:
			return value

	def getIndexKey(self, value):
		"""Returns the key used to find the value"""
		return self.extractor(value)

	def add(self, value):
		self.STORAGE.add(self.getSignature(value), self.getIndexKey(value))

	def update(self, value):
		self.STORAGE.update(self.getSignature(value), self.getIndexKey(value))

	def get(self, key, restore=True):
		for _ in self.STORAGE.get(key) or ():
			if _ is not None:
				yield self._restoreValue(_) if restore else _

	def one(self, key, index=0, restore=True):
		i = 0
		for _ in self.STORAGE.get(key) or ():
			if _ is not None:
				if i == index:
					return self._restoreValue(_) if restore else _
				else:
					i += 1

	def has(self, key):
		try:
			next(self(key))
			return True
		except StopIteration:
			return False

	def count(self, key):
		return len(list(self.STORAGE.get(key) or ()))

	def getKeys(self, sig):
		return self.STORAGE.getKeys(sig)

	def keys(self, start=0, end=None, count=None, order=0):
		return self.STORAGE.keys(start=start, end=end, count=count, order=order)

	def list(self, start=0, end=None, count=None, order=0, restore=True):
		for _ in self.STORAGE.list(start=start, end=end, count=count, order=order):
			yield self._restoreValue(_) if restore else _

	def remove(self, value):
		self.STORAGE.remove(self.getSignature(value))

	def clear(self):
		self.STORAGE.clear()

	def rebuild(self, values):
		self.clear()
		count = 0
		for _ in values:
			self.add(_)
			count += 1
		return count

	def save(self):
		self.STORAGE.sync()

	def __call__(self, key, restore=True):
		return self.get(key, restore=restore)


# -----------------------------------------------------------------------------
#
# PUBLIC API
#
# -----------------------------------------------------------------------------

__all__ = [
	"AttrDict",
	"Index",
	"Indexes",
	"Indexing",
	"IndexStorage",
]


# EOF
