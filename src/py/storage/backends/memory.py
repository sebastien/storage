from typing import Dict, Optional

from . import StorageBackend


class MemoryBackend(StorageBackend):
	"""A really simple backend that wraps Python's dictionary. Keys are converted
	to JSON while values are kept as-is."""

	def __init__(self):
		super().__init__()
		self.values = {}

	def add(self, key, data):
		key = self._serialize(key)
		self.values[key] = data

	def update(self, key, data):
		key = self._serialize(key)
		self.values[key] = data

	def remove(self, key):
		key = self._serialize(key)
		del self.values[key]

	def sync(self):
		pass

	def has(self, key):
		key = self._serialize(key)
		return key in self.values

	def get(self, key):
		key = self._serialize(key)
		return self.values.get(key)

	def list(self, key=None):
		assert key is None, "Not implemented"
		return list(self.values.values())

	def count(self, key=None):
		assert key is None, "Not implemented"
		return len(self.values)

	def keys(self, collection=None, order=StorageBackend.ORDER_NONE):
		keys = list(self.values.keys())
		if order == StorageBackend.ORDER_ASCENDING:
			keys = sorted(keys)
		elif order == StorageBackend.ORDER_DESCENDING:
			keys = sorted(keys, reverse=True)
		for key in keys:
			yield self._deserialize(key=key)

	def clear(self):
		self.values = {}

	def export(self, **options):
		return self.values


class KVMemoryBackend(StorageBackend):
	"""Byte-oriented in-memory backend used by `KVStorage`."""

	def __init__(self):
		super().__init__()
		self._data: Dict[str, bytes] = {}

	def set(self, key: str, data: bytes):
		self._data[key] = data

	def add(self, key, data):
		return self.set(key, data)

	def update(self, key, data):
		return self.set(key, data)

	def remove(self, key):
		self._data.pop(key, None)

	def delete(self, key):
		return self.remove(key)

	def has(self, key):
		return key in self._data

	def get(self, key):
		return self._data.get(key)

	def keys(self, collection=None, order=StorageBackend.ORDER_NONE):
		keys: list[str] = list(self._data.keys())
		prefix: Optional[str] = None
		if isinstance(collection, (tuple, list)):
			prefix = collection[0] if collection else None
		else:
			prefix = collection
		if prefix:
			keys = [k for k in keys if k.startswith(prefix)]
		if order == StorageBackend.ORDER_ASCENDING:
			keys = sorted(keys)
		elif order == StorageBackend.ORDER_DESCENDING:
			keys = sorted(keys, reverse=True)
		for key in keys:
			yield key

	def count(self, key=None):
		if key is None:
			return len(self._data)
		return len(tuple(self.keys(key)))

	def size(self) -> int:
		return len(self._data)

	def clear(self):
		self._data.clear()


__all__ = [
	"MemoryBackend",
	"KVMemoryBackend",
]


# EOF
