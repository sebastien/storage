"""Persistent storage implementation for indexes."""

from ..core import getTimestamp


# TODO: Add cached ordered keys


class IndexStorage(object):
	"""An index storage stores indexing information using two key-value
	backends. An Index produces a couple (key, signature) from a given value.
	The `key` represents its indexing key, the `signature` represents a
	way to uniquely identify the value."""

	KEY_LASTUPDATE = "__index__.lastUpdate"

	def __init__(self, forwardBackend, backwardBackend, metaBackend=None):
		"""The forward backend maps the computed value index key to the value
		signature (for instance, a user email to a user id), while the
		backward backend does just the opposite."""
		self.forwardBackend = forwardBackend
		self.backwardBackend = backwardBackend
		self.metaBackend = metaBackend or backwardBackend

	def getLastUpdate(self):
		"""Returns the timestamp of the last update"""
		return self.metaBackend.get(self.KEY_LASTUPDATE)

	def add(self, sig, keys):
		# We convert to multiple keys by default
		if type(keys) not in (tuple, list):
			keys = (keys,)
		# If the object was already there, we remove its entries in both
		# backends and add new ones
		has_backward = self.backwardBackend.has(sig)
		if has_backward:
			previous_keys = self.backwardBackend.get(sig)
			for previous_key in previous_keys:
				if self.forwardBackend.has(previous_key):
					values = [
						_ for _ in self.forwardBackend.get(previous_key) if _ != sig
					]
					if not values:
						self.forwardBackend.remove(previous_key)
					else:
						self.forwardBackend.update(previous_key, values)
				else:
					# FIXME: There should be a warning here, as if there is a
					# backward value, there should be a forward value!
					pass
			self.backwardBackend.update(sig, keys)
		else:
			self.backwardBackend.add(sig, keys)
		# We update the forward backend, ensuring that the value is registered
		for key in keys:
			if self.forwardBackend.has(key):
				values = self.forwardBackend.get(key)
				values.append(sig)
				self.forwardBackend.update(key, values)
			else:
				self.forwardBackend.add(key, [sig])

	def get(self, key):
		return self.forwardBackend.get(key)

	def getKeys(self, sig):
		return self.backwardBackend.get(sig)

	def update(self, sig, key):
		self.add(sig, key)

	def keys(self, start=0, end=None, count=None, order=0):
		"""Returns the given keys in database order (default), ascending order (order > 0)
		or descending order (order < 0)"""
		keys = self.forwardBackend.keys(order=order)
		i = 0
		if count is not None:
			end = start + count
		for k in keys:
			if end is not None and i >= end:
				break
			if i >= start:
				yield k
			i += 1

	def list(self, start=0, end=None, count=None, order=0):
		i = 0
		if count is not None:
			end = start + count
		for k in self.keys(order=order):
			for v in self.get(k):
				if end is not None and i >= end:
					break
				if i >= start:
					yield v
				i += 1

	def remove(self, sig):
		if self.backwardBackend.has(sig):
			previous_keys = self.backwardBackend.get(sig)
			for previous_key in previous_keys:
				# NOTE: We've seen some cases where forward_mapping can be done
				# this most likely happens when the extractor fails
				forward_mapping = self.forwardBackend.get(previous_key)
				values = [_ for _ in forward_mapping or () if _ != sig]
				if not values:
					if forward_mapping is not None:
						self.forwardBackend.remove(previous_key)
				else:
					self.forwardBackend.update(previous_key, values)
			self.backwardBackend.remove(sig)

	def clear(self):
		self.forwardBackend.clear()
		self.backwardBackend.clear()

	def sync(self):
		self.metaBackend.add(self.KEY_LASTUPDATE, getTimestamp())
		self.forwardBackend.sync()
		self.backwardBackend.sync()
		if self.metaBackend != self.backwardBackend:
			self.metaBackend.sync()


__all__ = ["IndexStorage"]
