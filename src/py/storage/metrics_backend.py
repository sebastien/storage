"""Directory persistence backend for metrics."""

import json

from .backends.fs import DirectoryBackend
from .core import NOTHING


class MetricsDirectoryBackend(DirectoryBackend):
	"""A specialized directory back-end designed to store metrics and allows
	for fast-searching."""

	FILE_EXTENSION = ".metric"

	def _defaultWriter(self, backend, operation, path, data):
		line = operation.value + "\t" + data
		return self.appendFile(path, line)

	def get(self, key, after=None, before=None):
		# FIXME: Should be smart when it comes to finding the offset of
		# after/before
		with open(self.path(key), "rb") as f:
			for line in f.readlines():
				data = self._deserialize(data=line)
				yield data

	def _serialize(self, key=NOTHING, data=NOTHING):
		"""Serializes the given metric to a string."""
		# NOTE: Data is expected to be a metric-export
		if data is not NOTHING:
			data = (
				"%d\t%s\t%s\t%s\n"
				% (
					data["timestamp"],
					data["name"],
					json.dumps(data["value"]),
					json.dumps(data.get("meta")),
				)
				if type(data) not in (str, str)
				else data
			)
		if key is not NOTHING:
			assert type(key) in (str, str), (
				self.__class__.__name__ + "._serialize only accepts strings as key."
			)
			key = str(key)
		if key is NOTHING:
			return data
		elif data is NOTHING:
			return key
		else:
			return key, data

	def _deserialize(self, key=NOTHING, data=NOTHING):
		"""Creates a metric instance out of the given (previously serialized)
		value."""
		if data is not NOTHING:
			if isinstance(data, bytes):
				data = data.decode("utf8")
			operation, timestamp, name, value, meta = data.split("\t", 5)
			data = dict(
				name=name,
				timestamp=int(timestamp),
				value=json.loads(value),
				meta=json.loads(meta),
			)
		if key is not NOTHING:
			return key
		if key is NOTHING:
			return data
		elif data is NOTHING:
			return key
		else:
			return key, data


# Keep persisted names stable while allowing the implementation to live here.
MetricsDirectoryBackend.__module__ = "storage.metrics"


__all__ = ["MetricsDirectoryBackend"]
