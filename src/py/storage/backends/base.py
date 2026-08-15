"""Physical backend contract shared by storage facades and backend wrappers."""

import logging
import threading
from copy import deepcopy

from ..core import NOTHING, Operation, asJSON, unJSON


class StorageBackend:
	"""Abstract key-value persistence backend for primitive storage values."""

	ORDER_NONE = 0
	ORDER_ASCENDING = 1
	ORDER_DESCENDING = -1

	HAS_READ = True
	HAS_WRITE = True
	HAS_STREAM = False
	HAS_FILE = False
	HAS_PUBLISH = True
	HAS_RAW = False
	HAS_ORDERING = False
	VALUE_FORMAT = "primitive"

	def __init__(self):
		self._onPublish = []
		self._subscribers = {}
		self._publicIDLock = threading.RLock()

	def onPublish(self, callback):
		if callback not in self._onPublish:
			self._onPublish.append(callback)
		return self

	def subscribe(self, key, callback):
		callbacks = self._subscribers.setdefault(key, [])
		if callback not in callbacks:
			callbacks.append(callback)
		return self

	def unsubscribe(self, key, callback):
		callbacks = self._subscribers.setdefault(key, [])
		if callback in callbacks:
			callbacks.remove(callback)
		return self

	def notify(self, key, operation, data=None):
		for callback in self._subscribers.get(key, []):
			try:
				callback(key, operation, data)
			except Exception as error:
				logging.error(
					"StorageBackend.notify: Exception in callback {0}: {1}".format(
						callback, error
					)
				)
		return self

	def publish(self, operation, key, data=None):
		for callback in self._onPublish:
			callback(operation, key, data)

	def process(self, operation: Operation, key: str, data=None):
		if operation is Operation.ADD:
			return self.add(key, data)
		elif operation is Operation.UPDATE:
			return self.update(key, data)
		elif operation is Operation.REMOVE:
			return self.remove(key)
		raise NotImplementedError

	def add(self, key, data):
		raise NotImplementedError

	def update(self, key, data):
		raise NotImplementedError

	def set(self, key, data):
		return self.update(key, data) if self.has(key) else self.add(key, data)

	def remove(self, key):
		raise NotImplementedError

	def delete(self, key):
		return self.remove(key)

	def clear(self):
		raise NotImplementedError

	def sync(self):
		raise NotImplementedError

	def has(self, key):
		raise NotImplementedError

	def get(self, key):
		raise NotImplementedError

	def list(self, key=None):
		raise NotImplementedError

	def count(self, key=None):
		raise NotImplementedError

	def size(self):
		return self.count()

	def keys(self, collection=None, order=ORDER_NONE):
		raise NotImplementedError

	def getMetadata(self, key=None, default=None):
		raise NotImplementedError

	def setMetadata(self, key, value):
		raise NotImplementedError

	def removeMetadata(self, key):
		raise NotImplementedError

	def storePublicObject(self, key, scope, factory):
		"""Allocates a public ID and stores its factory-generated object atomically when possible."""
		with self._publicIDLock:
			publicIDs = self.getMetadata("__storage__.publicIDs", {}) or {}
			oldPublicIDs = deepcopy(publicIDs)
			state = dict(publicIDs.get(scope, {}))
			publicID = int(state.get("next", 1))
			state["next"] = publicID + 1
			state.setdefault("objects", {})[str(publicID)] = key
			publicIDs[scope] = state
			data = factory(publicID)
			oldData = self.get(key)
			try:
				self.add(key, data)
				self.setMetadata("__storage__.publicIDs", publicIDs)
			except Exception:
				if oldData is None:
					if self.has(key):
						self.remove(key)
				else:
					self.update(key, oldData)
				self.setMetadata("__storage__.publicIDs", oldPublicIDs)
				raise
			return publicID

	def getPublicObjectKey(self, scope, publicID):
		publicIDs = self.getMetadata("__storage__.publicIDs", {}) or {}
		return (publicIDs.get(scope, {}).get("objects", {}) or {}).get(str(publicID))

	def removeObject(self, key):
		publicIDs = self.getMetadata("__storage__.publicIDs", {}) or {}
		changed = False
		for state in publicIDs.values():
			objects = state.get("objects", {})
			for publicID, objectKey in list(objects.items()):
				if objectKey == key:
					del objects[publicID]
					changed = True
		if changed:
			self.setMetadata("__storage__.publicIDs", publicIDs)
		return self.remove(key)

	def path(self, key):
		raise NotImplementedError

	def stream(self, key, size=None):
		raise NotImplementedError

	def hasRawData(self, key, ext=None):
		raise NotImplementedError

	def saveRawData(self, key, data, ext=None):
		raise NotImplementedError

	def streamRawData(self, key, size=None, ext=None):
		raise NotImplementedError

	def getRawDataPath(self, key, ext=None):
		raise NotImplementedError

	def _serialize(self, key=NOTHING, data=NOTHING):
		if key is NOTHING:
			return asJSON(data)
		elif data is NOTHING:
			return asJSON(key)
		return asJSON(key), asJSON(data)

	def _deserialize(self, key=NOTHING, data=NOTHING):
		if key is NOTHING:
			return unJSON(data, useRestore=False)
		elif data is NOTHING:
			return unJSON(key, useRestore=False)
		return unJSON(key, useRestore=False), unJSON(data, useRestore=False)


__all__ = ["StorageBackend"]
