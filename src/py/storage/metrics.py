"""Monotone metric model and storage helpers."""

from .core import NOTHING, Storable, getCanonicalName, getTimestamp
from .metric_storage import MetricStorage
from .metrics_backend import MetricsDirectoryBackend

# -----------------------------------------------------------------------------
#
# METRIC MODEL
#
# -----------------------------------------------------------------------------


class StoredMetric(Storable):
	"""An interface/abstract class that specifies the methods that should
	be implemented by metric values to be stored by storage providers
	defined in this module."""

	@classmethod
	def Recognizes(cls, data):
		if isinstance(data, dict):
			for key in ("name", "value", "timestamp"):
				if key not in data:
					return False
			return True
		else:
			return False

	@classmethod
	def Import(cls, data):
		return cls(
			name=data["name"],
			value=data["value"],
			timestamp=data["timestamp"],
			meta=data.get("meta"),
		)

	def __init__(self, name, value, meta=None, timestamp=None):
		if timestamp is None:
			timestamp = getTimestamp()
		self.name = name
		self.value = value
		self.meta = meta
		self.timestamp = timestamp

	def isBefore(self, timestamp):
		return timestamp is None or self.timestamp < timestamp

	def isAfter(self, timestamp):
		return timestamp is None or self.timestamp >= timestamp

	def getName(self):
		"""Returns the name for this metric, as a string. The recommended
		notation is dot separated camelCase."""
		return self.name

	def getValue(self):
		"""Returns the actual value for this metric. This is expected to
		be either a number or a string, but in some cases, it might be a
		simple list or map of these elements. In general, values should
		be kept as simple as possible."""
		return self.value

	def getTimestamp(self):
		"""Returns the timestamp corresponding to the moment where the value
		was sampled, as a UTC Unix epoch timestamp in seconds."""
		return self.timestamp

	def get(self, key=NOTHING):
		"""Alias for getMeta"""
		return self.getMeta(key)

	def getMeta(self, key=NOTHING):
		"""Returns a dictionary of meta-informations attached to the event."""
		if key is NOTHING:
			return self.meta
		else:
			return self.meta.get(key) if self.meta else None

	def hasMeta(self, key):
		return key in self.meta if self.meta else False

	def export(self, **options):
		return dict(
			name=self.name,
			value=self.value,
			timestamp=self.timestamp,
			meta=self.meta,
			type=getCanonicalName(self.__class__),
		)


# -----------------------------------------------------------------------------
#
# PUBLIC API
#
# -----------------------------------------------------------------------------

__all__ = [
	"MetricStorage",
	"MetricsDirectoryBackend",
	"StoredMetric",
]


# EOF - vim: tw=80 ts=4 sw=4 noet
