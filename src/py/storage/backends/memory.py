from .base import StorageBackend


class MemoryBackend(StorageBackend):
	"""A really simple backend that wraps Python's dictionary. Keys are converted
	to JSON while values are kept as-is."""

	def __init__(self):
		super().__init__()
		self.values = {}
		self.metadata = {}

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
		self.metadata = {}

	def getMetadata(self, key=None, default=None):
		if key is None:
			return dict(self.metadata)
		return self.metadata.get(key, default)

	def setMetadata(self, key, value):
		self.metadata[key] = value
		return value

	def removeMetadata(self, key):
		if key in self.metadata:
			del self.metadata[key]
		return self

	def export(self, **options):
		return self.values


# Compatibility name for callers that previously chose the KV-specific backend.
KVMemoryBackend = MemoryBackend


__all__ = [
	"MemoryBackend",
	"KVMemoryBackend",
]


# EOF
