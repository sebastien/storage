"""Lazy property and relation implementations used by stored objects.

This module deliberately does not import ``storage.objects``.  Keeping the
value wrappers independent lets ``objects`` remain the public façade without
creating an import cycle with the rest of the storage model.
"""

from typing import Any, List

from ..core import Storable, asPrimitive, getCanonicalName, isSame, restore


def _resolveAccessor(storedObject, prefix: str, name: str):
	cap_name = name[0].upper() + name[1:]
	accessor_name = prefix + cap_name
	return getattr(storedObject, accessor_name) if hasattr(storedObject, accessor_name) else None


class PropertyDescriptor:
	"""Provides transparent access to setProperty/getProperty."""

	def __init__(self, name):
		self.name = name

	def __get__(self, instance, owner):
		if not instance:
			return self
		return instance.getProperty(self.name)

	def __set__(self, instance, value):
		assert instance, "Property descriptors cannot be re-assigned in class"
		return instance.setProperty(self.name, value)


class RelationDescriptor:
	"""Provides transparent access to setRelation/getRelation."""

	def __init__(self, name):
		self.name = name

	def __get__(self, instance, owner):
		if not instance:
			return self
		return instance.getRelation(self.name)

	def __set__(self, instance, value):
		assert instance, "Relation descriptors cannot be re-assigned in class"
		return instance.setRelation(self.name, value)


class Property:
	"""Lazily restores and optionally transforms a stored property value."""

	def __init__(self, name, storedObject):
		self.name = name
		self.value = None
		self.restored = False
		assert storedObject, "Property requires stored object instance"
		self.storedObject = storedObject
		self._setter = _resolveAccessor(storedObject, "set", name)
		self._getter = _resolveAccessor(storedObject, "get", name)

	def set(self, value):
		if self._setter:
			old_value = value
			value = self._setter(value)
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
				if value is None or value is self.storedObject:
					value = old_value
		return self.value

	def export(self, **options):
		if "depth" not in options:
			options["depth"] = 0
		return asPrimitive(self.value, **options)

	def __repr__(self):
		return "@property:" + repr(self.value)


class InverseRelation:
	"""Parent-side view of children indexed by a foreign key.

	Not stored on the parent. ``parent.children`` lists ``Child`` rows whose
	``field`` equals ``parent.id``. Requires ``Child.INDEX_BY[field]``.
	"""

	def __init__(self, target, field: str):
		if not field:
			raise ValueError("InverseRelation requires a field name")
		self.target = target
		self.field = field


class Relation:
	"""A lazily loaded one-to-many or single-value object relation."""

	def __init__(self, parentClass, definition):
		self.parentClass = parentClass
		self.definition = definition
		self.values = None

	def isInverse(self) -> bool:
		return isinstance(self.definition, InverseRelation)

	def _assertMutable(self):
		if self.isInverse():
			raise TypeError("InverseRelation has no stored order; set the child foreign key")

	def _parent(self):
		parent = self.parentClass
		if isinstance(parent, type):
			raise RuntimeError("InverseRelation requires a stored object instance")
		return parent

	def _index(self):
		target = self.getRelationClass()
		index = target.IndexFor(self.definition.field) if hasattr(target, "IndexFor") else None
		if not index:
			raise RuntimeError(
				f"InverseRelation requires INDEX_BY {self.definition.field!r} on {target.__name__} and Indexes.use()"
			)
		return index

	def init(self, values):
		self.values = values
		return self

	def _attach(self, value, attached: bool):
		if not value:
			return self
		restored = restore(value)
		if (
			not isinstance(restored, self.getRelationClass())
			or not hasattr(restored, "typeName")
			or restored.typeName != getCanonicalName(self.getRelationClass())
		):
			raise ValueError(
				f"Relation expects value of type {self.getRelationClass()}, got {type(restored)}: {restored}"
			)
		field = self.definition.field
		new_value = self._parent().id if attached else None
		if getattr(restored, field, None) == new_value:
			return self
		setattr(restored, field, new_value)
		if restored.storage:
			restored.save()
		return self

	def add(self, value):
		return self.append(value)

	def append(self, value):
		if self.isInverse():
			return self._attach(value, True)
		if not value:
			return self
		if not (isinstance(value, dict) or isinstance(value, Storable)):
			raise ValueError(
				f"Relation only accepts object or exported object, got {type(value)}: {value}"
			)
		restored = restore(value)
		if (
			not isinstance(restored, self.getRelationClass())
			or not hasattr(restored, "typeName")
			or restored.typeName != getCanonicalName(self.getRelationClass())
		):
			raise ValueError(
				f"Relation expects value of type {self.getRelationClass()}, got {type(restored)}: {restored}"
			)
		if self.values is None:
			self.values = []
		if not self.isMany() and len(self.values):
			raise RuntimeError(
				f"Cannot append to a single value relation, relation has {len(self.values)} values: {restored}"
			)
		self.values.append(restored)
		return self

	def remove(self, value):
		if self.isInverse():
			return self._attach(value, False)
		if not value:
			return self
		self.values = [_ for _ in self.get(resolve=False) if not isSame(_, value)]
		return self

	def swap(self, a, b):
		self._assertMutable()
		if not self.isMany():
			raise RuntimeError("Cannot swap values in a single value relation")
		if self.values is None:
			self.values = []
		self.values[a], self.values[b] = self.values[b], self.values[a]
		return self

	def clear(self):
		if self.isInverse():
			for child in list(self.get(resolve=True)):
				self._attach(child, False)
			return self
		self.values = []
		return self

	def set(self, values):
		if self.isInverse():
			if type(values) not in (list, tuple):
				values = (values,) if values else ()
			desired = [restore(_) for _ in values if _]
			desired_ids = {_.id for _ in desired}
			for child in list(self.get(resolve=True)):
				if child.id not in desired_ids:
					self._attach(child, False)
			for child in desired:
				self._attach(child, True)
			return self
		self.clear()
		if type(values) not in (list, tuple):
			values = (values,)
		list(map(self.add, values))
		return self

	def get(self, start=0, limit=-1, resolve=True, depth=0):
		if self.isInverse():
			items = [
				value
				for value in self._index().get(self._parent().id, restore=True)
				if value is not None
			]
			for i, value in enumerate(items):
				if i < start or (limit != -1 and i >= limit):
					continue
				if resolve:
					yield value
				else:
					yield {"id": value.id, "type": value.getTypeName()}
			return
		relation_class = self.getRelationClass()
		if self.values is not None:
			for i, value in enumerate(self.values):
				if i < start or (limit != -1 and i >= limit):
					continue
				if resolve:
					yield restore(value) if type(value) is dict else (relation_class.Get(value) if not isinstance(value, Storable) else value)
				elif isinstance(value, Storable):
					yield value.export(depth=depth)
				elif isinstance(value, dict) and "id" in value and "type" in value:
					yield {"id": value["id"], "type": value["type"]}
				else:
					yield value

	def one(self, index=0):
		try:
			return next(self.get(resolve=True, start=index))
		except StopIteration:
			return None

	def isEmpty(self) -> bool:
		try:
			next(self.get(resolve=False))
			return False
		except StopIteration:
			return True

	def contains(self, objectOrID) -> bool:
		return self.has(objectOrID)

	def has(self, objectOrID) -> bool:
		id = objectOrID.id if isinstance(objectOrID, Storable) else objectOrID
		return any(isinstance(value, dict) and "id" in value and "type" in value and value["id"] == id for value in self.get(resolve=False))

	def list(self):
		return self.get(resolve=True)

	def all(self):
		return self.list()

	def isMany(self) -> bool:
		if self.isInverse():
			return True
		return isinstance(self.definition, (tuple, list))

	def getRelationClass(self):
		if self.isInverse():
			return self.definition.target
		return self.definition[0] if self.isMany() else self.definition

	def export(self, **options) -> List[Any]:
		options = dict(options)
		if "depth" in options:
			options["depth"] += 1
		return [asPrimitive(value, **options) for value in self.get(resolve=options.get("resolve", True))]

	def __len__(self) -> int:
		if self.isInverse():
			return self._index().count(self._parent().id)
		return len(self.values) if self.values else 0

	def __call__(self, *args, **kwargs):
		return self.get(*args, **kwargs)

	def __getitem__(self, key):
		if type(key) not in (int, float):
			raise IndexError(f"Relations can only be queried by index, got: {key}")
		if key < 0:
			key = max(0, len(self) + key)
		for value in self.get():
			if key == 0:
				return value
			key -= 1
		return None

	def __iter__(self):
		return self.get(resolve=True)

	def __repr__(self):
		return "<relation:%s=%s>" % (self.definition, self.values)

	def __delete__(self, instance, owner):
		self.clear()
		return self
