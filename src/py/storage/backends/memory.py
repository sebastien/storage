from . import StorageBackend


class MemoryBackend(StorageBackend):
	"""A really simple backend that wraps Python's dictionary. Keys are converted
	to JSON while values are kept as-is."""

	def __init__(self):
		super().__init__(self)
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


# EOF
