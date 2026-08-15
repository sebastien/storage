"""Public module import compatibility checks."""

import unittest


class ImportCompatibilityTest(unittest.TestCase):
	def test_objects_exports(self):
		from storage.objects import ObjectStorage, Ownership, Property, Relation, StoredObject
		from storage.object_storage import ObjectStorage as MovedObjectStorage

		self.assertTrue(all((ObjectStorage, Ownership, Property, Relation, StoredObject)))
		self.assertIs(ObjectStorage, MovedObjectStorage)

	def test_stored_object_identity_and_canonical_name(self):
		from storage.core import getCanonicalName
		from storage.objects import Ownership, PublicID, StoredObject

		self.assertEqual("storage.objects.Ownership", getCanonicalName(Ownership))
		self.assertEqual("storage.objects.PublicID", getCanonicalName(PublicID))
		self.assertEqual("storage.objects.StoredObject", getCanonicalName(StoredObject))

	def test_object_descriptor_identity(self):
		from storage.object_descriptors import Property as MovedProperty
		from storage.object_descriptors import Relation as MovedRelation
		from storage.objects import Property, Relation

		self.assertIs(Property, MovedProperty)
		self.assertIs(Relation, MovedRelation)

	def test_raw_exports(self):
		from storage.raw import RawStorage, StoredRaw
		from storage.raw_storage import RawStorage as MovedRawStorage

		self.assertTrue(all((RawStorage, StoredRaw)))
		self.assertIs(RawStorage, MovedRawStorage)

	def test_raw_model_exports_preserve_raw_identity(self):
		from storage.raw import StoredRaw
		from storage.raw_model import StoredRaw as MovedStoredRaw
		from storage.core import getCanonicalName

		self.assertIs(StoredRaw, MovedStoredRaw)
		self.assertEqual("storage.raw.StoredRaw", getCanonicalName(StoredRaw))

	def test_metrics_exports(self):
		from storage.metrics import MetricStorage, MetricsDirectoryBackend, StoredMetric

		self.assertTrue(all((MetricStorage, MetricsDirectoryBackend, StoredMetric)))

	def test_metrics_backend_identity(self):
		from storage.core import getCanonicalName
		from storage.metrics import MetricsDirectoryBackend
		from storage.metrics_backend import MetricsDirectoryBackend as MovedBackend

		self.assertIs(MetricsDirectoryBackend, MovedBackend)
		self.assertEqual("storage.metrics.MetricsDirectoryBackend", getCanonicalName(MovedBackend))

	def test_metric_storage_identity_and_canonical_name(self):
		from storage.core import getCanonicalName
		from storage.metric_storage import MetricStorage as MovedMetricStorage
		from storage.metrics import MetricStorage

		self.assertIs(MetricStorage, MovedMetricStorage)
		self.assertEqual("storage.metrics.MetricStorage", getCanonicalName(MovedMetricStorage))

	def test_index_exports(self):
		from storage import IndexStorage as PackageIndexStorage
		from storage.index import Index, IndexStorage, Indexing
		from storage.index_storage import IndexStorage as MovedIndexStorage
		from storage.indexing import Indexing as MovedIndexing
		from storage.indexing import RE_NOALPHANUM, RE_SPACES
		from storage.core import getCanonicalName

		self.assertTrue(all((Index, IndexStorage, Indexing)))
		self.assertIs(PackageIndexStorage, MovedIndexStorage)
		self.assertIs(IndexStorage, MovedIndexStorage)
		self.assertIs(Indexing, MovedIndexing)
		self.assertEqual("storage.index.Indexing", getCanonicalName(MovedIndexing))
		self.assertEqual("hello world", MovedIndexing.Normalize("  HELLO\nWORLD "))
		self.assertEqual("hello world", RE_NOALPHANUM.sub(" ", "hello-world"))
		self.assertEqual("hello world", RE_SPACES.sub(" ", "hello\tworld"))

	def test_web_exports(self):
		from storage.web import StorageChannel, StorageDecoration, StorageServer, StorageWebError, http

		self.assertTrue(all((StorageChannel, StorageDecoration, StorageServer, StorageWebError, http)))

	def test_web_submodules(self):
		from storage.web.channels import StorageChannel
		from storage.web.decorators import StorageDecoration, http
		from storage.web.errors import StorageWebError
		from storage.web.formatting import StorageServer as FormattingServer
		from storage.web.kv import StorageServer as KVServer
		from storage.web.objects import StorageServer as ObjectServer
		from storage.web.relations import StorageServer as RelationServer

		self.assertTrue(all((StorageChannel, StorageDecoration, StorageWebError, http)))
		self.assertTrue(
			all(server is FormattingServer for server in (KVServer, ObjectServer, RelationServer))
		)
