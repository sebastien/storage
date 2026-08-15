"""Backend composition helpers."""

from .base import StorageBackend


class MultiBackend(StorageBackend):
	"""Replicates writes across backends while routing reads to one backend."""

	def __init__(self, *backends):
		super().__init__()
		self.backends = backends
		self._readBackend = None
		self._fileBackend = None
		for backend in self.backends:
			if backend.HAS_READ:
				self._readBackend = backend
			if backend.HAS_FILE:
				self._fileBackend = backend
			backend.onPublish(
				lambda operation, key, data=None, source=backend: self._onBackendPublish(
					operation, key, data, source
				)
			)

	def _onBackendPublish(self, operation, key, data, source):
		for backend in self.backends:
			if backend != source:
				backend.process(operation, key, data)

	def add(self, key, data):
		for backend in self.backends:
			backend.add(key, data)

	def update(self, key, data):
		for backend in self.backends:
			backend.update(key, data)

	def remove(self, key):
		for backend in self.backends:
			backend.remove(key)

	def sync(self):
		for backend in self.backends:
			backend.sync()

	def _reader(self):
		if not self._readBackend:
			raise RuntimeError(f"Undefined read backend: {self}")
		return self._readBackend

	def has(self, key):
		return self._reader().has(key)

	def get(self, key):
		return self._reader().get(key)

	def list(self, key=None):
		return self._reader().list(key)

	def count(self, key=None):
		return self._reader().count(key)

	def keys(self, collection=None, order=StorageBackend.ORDER_NONE):
		return self._reader().keys(collection, order)

	def getMetadata(self, key=None, default=None):
		return self._reader().getMetadata(key, default)

	def setMetadata(self, key, value):
		for backend in self.backends:
			backend.setMetadata(key, value)
		return value

	def removeMetadata(self, key):
		for backend in self.backends:
			backend.removeMetadata(key)
		return self

	def path(self, key):
		if not self._fileBackend:
			raise RuntimeError(f"Undefined file backend: {self}")
		return self._fileBackend.path(key)


__all__ = ["MultiBackend"]
