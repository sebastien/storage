"""Metric storage runtime implementation."""

from .core import Operation


_DEFAULT_METRIC_CLASS = object()


class MetricStorage:
	"""A metric storage is typically a monotone (the past doesn't change),
	append-only storage. Values stored in metric are composed of (key, timestamp,
	value, meta), as opposed as just (key, value) as it would be the case with
	object storage.

	A metric storage doesn't need caching as an object does, as the typical
	use it to query the metric by key and time range, usually computing
	aggregates on the fly.

	As with object storage, the metric storage has a sync policy, where
	you should call sync explicitely -- although some backends might
	sync automatically.
	"""

	def __init__(self, backend, metricClass=_DEFAULT_METRIC_CLASS):
		"""Creates a new metric storage with the given backend"""
		if metricClass is _DEFAULT_METRIC_CLASS:
			from .metrics import StoredMetric

			metricClass = StoredMetric
		self.backend = backend
		self.metricClass = metricClass

	def _ensureMetric(self, metric):
		if not isinstance(metric, self.metricClass):
			metric = self.metricClass.Import(metric)
		return metric

	def add(self, metric):
		"""Adds the given metric to the storage."""
		metric = self._ensureMetric(metric)
		self.backend.add(metric.getName(), self._export(metric, Operation.ADD))

	def get(self, name, after=None, before=None):
		"""Gets the metric with the given name from the storage."""
		for _ in self.backend.get(name):
			m = self._ensureMetric(_)
			if after is not None and not m.isAfter(after):
				continue
			if before is not None and not m.isBefore(before):
				continue
			yield m

	def remove(self, metric):
		"""Removes the given metric to the storage. In most cases, the
		metric won't be actually removed, but just invalidated."""
		metric = self._ensureMetric(metric)
		self.backend.remove(metric.getName(), self._export(metric, Operation.REMOVE))

	def update(self, metric):
		"""Updates the value for the given metric in the storage."""
		metric = self._ensureMetric(metric)
		self.backend.update(metric.getName(), self._export(metric, Operation.UPDATE))

	def sync(self):
		"""Explicitely ask the back-end to synchronize. Depending on the
		back-end this might be a long or short, blocking or async
		operation."""
		self.backend.sync()

	def keys(self, prefix=None):
		for k in self.backend.keys(prefix=prefix):
			yield k

	def query(self, name=None, timestamp=None):
		"""Returns a list of metric with the given name and timestamp.
		If name is a string, then only the metrics with the given names
		will be returned. If name is a list, only the metrics within the
		given list will be returned, if name is a function it will be used
		as a predicate to filter the name.

		If timestamp is a number, it will return only the metrics with the
		actual timestamp. If it's a couple `(a,b)`, then it will return all the
		metrics where `a <= timestamp  < b`. If it is a function, it will
		return only the metrics where the timestamp acts as a predicate."""
		return self.backend.queryMetrics(name=name, timestamp=timestamp)

	def list(self):
		"""Lists the metrics available in this backend"""
		return self.backend.listMetrics()

	def _export(self, metric, operation):
		"""Serializes the given metric to a string."""
		return metric.export()

	def _import(self, data):
		"""Creates a metric instance out of the given (previously serialized)
		value."""
		return self.metricClass.Import(data)


# Keep persisted names stable while allowing the implementation to live here.
MetricStorage.__module__ = "storage.metrics"


__all__ = ["MetricStorage"]
