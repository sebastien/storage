"""Contract tests for metric models and directory persistence."""

import shutil
import tempfile
import unittest

from storage.core import getCanonicalName
from storage.metrics import MetricStorage, MetricsDirectoryBackend, StoredMetric


class MetricsTest(unittest.TestCase):
	def setUp(self):
		self.root = tempfile.mkdtemp(prefix="storage-metrics-")
		self.backend = MetricsDirectoryBackend(self.root)
		self.storage = MetricStorage(self.backend)

	def tearDown(self):
		shutil.rmtree(self.root)

	def test_metric_round_trip(self):
		metric = StoredMetric("requests", 3, meta={"host": "test"}, timestamp=10)
		self.assertEqual("storage.metrics.StoredMetric", getCanonicalName(StoredMetric))
		self.assertEqual("storage.metrics.StoredMetric", metric.export()["type"])
		self.storage.add(metric)

		values = list(self.storage.get("requests"))
		self.assertEqual(1, len(values))
		self.assertEqual(3, values[0].getValue())
		self.assertEqual("test", values[0].getMeta("host"))

	def test_metric_time_range(self):
		self.storage.add(StoredMetric("requests", 1, timestamp=10))
		self.storage.add(StoredMetric("requests", 2, timestamp=20))

		values = list(self.storage.get("requests", after=20))
		self.assertEqual([2], [metric.getValue() for metric in values])
