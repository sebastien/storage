from . import StorageBackend
from ..core import asJSON, unJSON

# FIMXE: We should get away from the DBM backend as it seems to have
# numerous problems -- I got a lot of "cannot write..". Maybe I'm
# using it wrong?
import dbm.ndbm as dbm
import time

# FIXME: This implementation is a bit shit. The changes should be queued and
# a worker should work on that queue.


class DBMBackend(StorageBackend):
	"""A really simple backend that wraps Python's DBM module. Key and value
	data are converted to JSON strings on the fly."""

	def __init__(self, path, autoSync=False):
		super().__init__()
		self._dbm = dbm
		self.path = f"{path}.dbm"
		self.autoSync = autoSync
		self.values = None
		self._metadataKey = "__storage__.metadata"
		self._open()

	def _open(self, mode="a") -> True:
		if self.values is None:
			try:
				self.values = self._dbm.open(self.path, "c")
				return True
			except self._dbm.error as e:
				raise RuntimeError("Cannot open DBM at path {}:{}".format(self.path, e))
		else:
			return False

	def _tryAdd(self, key, data):
		# SEE: http://stackoverflow.com/questions/4995162/python-shelve-dbm-error/12167172#12167172
		# NOTE: I've encountered a lot of problems with DBM, it does not
		# seem to be very reliable for that kind of application
		retries = 5
		if self.values is None:
			self._open()
		if self.values is None:
			raise RuntimeError(f"Could not open DBM database at: {self.path}")
		if key:
			while True:
				try:
					self.values[key] = data
					return True
				except self._dbm.error as e:
					# FIXME: This is not cool, this should be done in a worker.
					time.sleep(0.100 * retries)
					if retries == 0:
						raise Exception(
							"{0} in {1}.db with key={2} data={3}".format(
								e, self.path, key, data
							)
						)
				retries -= 1

	def add(self, key, data):
		key, data = self._serialize(key, data)
		self._tryAdd(key, data)
		return self

	def _dbmKey(self, key):
		return key.encode("utf-8") if isinstance(key, str) else key

	def _rawKeyText(self, key):
		return key.decode("utf-8") if isinstance(key, bytes) else key

	def update(self, key, data):
		key, data = self._serialize(key, data)
		self._tryAdd(key, data)
		return self

	def remove(self, key):
		key = self._serialize(key=key)
		del self.values[self._dbmKey(key)]

	def sync(self):
		# FIXME: Sync is an expensive operation, so it should really not be done on every operation.
		if self.values is None:
			return
		if hasattr(self.values, "sync"):
			self.values.sync()
			return
		self.values.close()
		self.values = None
		self._open()

	def has(self, key):
		key = self._serialize(key=key)
		return self._dbmKey(key) in self.values

	def get(self, key):
		key = self._serialize(key=key)
		data = self.values.get(self._dbmKey(key))
		if data is None:
			return data
		else:
			return self._deserialize(data=data)

	def keys(self, collection=None, order=StorageBackend.ORDER_NONE):
		keys = [
			k
			for k in list(self.values.keys())
			if self._rawKeyText(k) != self._metadataKey
		]
		if order == StorageBackend.ORDER_ASCENDING:
			keys = sorted(keys)
		elif order == StorageBackend.ORDER_DESCENDING:
			keys = sorted(keys, reverse=True)
		for key in keys:
			yield self._deserialize(key=self._rawKeyText(key))

	def clear(self):
		# TODO: Not very optimized
		for k in list(self.keys()):
			self.remove(k)
		self.removeMetadata(None)
		self.close()
		self._open()

	def list(self, key=None):
		assert key is None, "Not implemented"
		for key in list(self.values.keys()):
			if self._rawKeyText(key) == self._metadataKey:
				continue
			data = self.values[key]
			yield self._deserialize(data=data)

	def count(self, key=None) -> int:
		assert key is None, "Not implemented"
		return len(tuple(self.keys())) if self.values is not None else 0

	def getMetadata(self, key=None, default=None):
		metadata = {}
		dbm_key = self._dbmKey(self._metadataKey)
		if self.values is not None and dbm_key in self.values:
			metadata = unJSON(self.values[dbm_key], useRestore=False)
		if key is None:
			return metadata
		return metadata.get(key, default)

	def setMetadata(self, key, value):
		metadata = self.getMetadata()
		metadata[key] = value
		self.values[self._dbmKey(self._metadataKey)] = asJSON(metadata)
		return value

	def removeMetadata(self, key):
		dbm_key = self._dbmKey(self._metadataKey)
		if key is None:
			if self.values is not None and dbm_key in self.values:
				del self.values[dbm_key]
			return self
		metadata = self.getMetadata()
		if key in metadata:
			del metadata[key]
			self.values[dbm_key] = asJSON(metadata)
		return self

	def close(self) -> bool:
		if self.values is not None:
			self.sync()
			self.values.close()
			self.values = None
			return True
		else:
			return False

	def __del__(self):
		self.close()


# EOF
