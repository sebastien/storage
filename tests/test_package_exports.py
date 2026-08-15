"""Public package export and namespace checks."""

import unittest


class PackageExportsTest(unittest.TestCase):
	def test_objects_package_exports(self):
		from storage.objects import ObjectStorage, Ownership, Property, Relation, StoredObject
		from storage.objects.descriptors import Property as DescriptorProperty
		from storage.objects.descriptors import Relation as DescriptorRelation
		from storage.objects.storage import ObjectStorage as StorageObjectStorage
		from storage.objects.model import StoredObject as ModelStoredObject

		self.assertTrue(all((ObjectStorage, Ownership, Property, Relation, StoredObject)))
		self.assertIs(ObjectStorage, StorageObjectStorage)
		self.assertIs(StoredObject, ModelStoredObject)
		self.assertIs(Property, DescriptorProperty)
		self.assertIs(Relation, DescriptorRelation)

	def test_stored_object_identity_and_canonical_name(self):
		from storage.core import getCanonicalName
		from storage.objects import Ownership, PublicID, StoredObject

		self.assertEqual("storage.objects.Ownership", getCanonicalName(Ownership))
		self.assertEqual("storage.objects.PublicID", getCanonicalName(PublicID))
		self.assertEqual("storage.objects.StoredObject", getCanonicalName(StoredObject))

	def test_raw_package_exports(self):
		from storage.raw import RawStorage, StoredRaw
		from storage.raw.model import StoredRaw as ModelStoredRaw
		from storage.raw.storage import RawStorage as StorageRawStorage

		self.assertTrue(all((RawStorage, StoredRaw)))
		self.assertIs(RawStorage, StorageRawStorage)
		self.assertIs(StoredRaw, ModelStoredRaw)

	def test_raw_model_preserves_canonical_name(self):
		from storage.raw import StoredRaw
		from storage.core import getCanonicalName

		self.assertEqual("storage.raw.StoredRaw", getCanonicalName(StoredRaw))

	def test_metrics_package_exports(self):
		from storage.core import getCanonicalName
		from storage.metrics import MetricStorage, MetricsDirectoryBackend, StoredMetric
		from storage.metrics.backend import MetricsDirectoryBackend as Backend
		from storage.metrics.model import StoredMetric as Model
		from storage.metrics.storage import MetricStorage as Storage

		self.assertTrue(all((MetricStorage, MetricsDirectoryBackend, StoredMetric)))
		self.assertIs(MetricStorage, Storage)
		self.assertIs(MetricsDirectoryBackend, Backend)
		self.assertIs(StoredMetric, Model)
		self.assertEqual("storage.metrics.StoredMetric", getCanonicalName(StoredMetric))

	def test_index_exports(self):
		from storage import IndexStorage as PackageIndexStorage
		from storage.index import Index, IndexStorage, Indexing
		from storage.index.core import Index as CoreIndex
		from storage.index.indexing import Indexing as ModuleIndexing
		from storage.index.indexing import RE_NOALPHANUM, RE_SPACES
		from storage.index.storage import IndexStorage as StorageIndexStorage
		from storage.core import getCanonicalName

		self.assertTrue(all((Index, IndexStorage, Indexing)))
		self.assertIs(PackageIndexStorage, StorageIndexStorage)
		self.assertIs(IndexStorage, StorageIndexStorage)
		self.assertIs(Index, CoreIndex)
		self.assertIs(Indexing, ModuleIndexing)
		self.assertEqual("storage.index.Indexing", getCanonicalName(ModuleIndexing))
		self.assertEqual("hello world", ModuleIndexing.Normalize("  HELLO\nWORLD "))
		self.assertEqual("hello world", RE_NOALPHANUM.sub(" ", "hello-world"))
		self.assertEqual("hello world", RE_SPACES.sub(" ", "hello\tworld"))

	def test_web_exports(self):
		from storage.web import (
			ObjectWebMixin,
			StorageChannel,
			StorageDecoration,
			StorageServer,
			StorageWebError,
			http,
		)

		self.assertTrue(
			all((ObjectWebMixin, StorageChannel, StorageDecoration, StorageServer, StorageWebError, http))
		)

	def test_web_submodules(self):
		from storage.web.channels import StorageChannel
		from storage.web.decorators import StorageDecoration, http
		from storage.web.errors import StorageWebError
		from storage.web.objects import ObjectWebMixin
		from storage.web.relations import RelationWebMixin
		from storage.web.server import StorageServer

		self.assertTrue(all((StorageChannel, StorageDecoration, StorageWebError, http)))
		self.assertTrue(issubclass(StorageServer, ObjectWebMixin))
		self.assertTrue(issubclass(StorageServer, RelationWebMixin))

	def test_web_mixins_do_not_export_server(self):
		from storage.web import formatting, kv, objects, relations

		for module in (formatting, kv, objects, relations):
			with self.subTest(module=module.__name__):
				self.assertFalse(hasattr(module, "StorageServer"))

	def test_backend_package_exports(self):
		import storage.backends as backends
		from storage.backends import MultiBackend, StorageBackend

		self.assertEqual({"MultiBackend", "StorageBackend"}, set(backends.__all__))
		self.assertTrue(all((MultiBackend, StorageBackend)))

	def test_retired_flat_modules_are_absent(self):
		import importlib

		retired = (
			"storage.object_storage",
			"storage.object_descriptors",
			"storage.raw_model",
			"storage.raw_storage",
			"storage.index_storage",
			"storage.indexing",
			"storage.metric_storage",
			"storage.metrics_backend",
		)
		for module_name in retired:
			with self.subTest(module=module_name):
				with self.assertRaises(ModuleNotFoundError):
					importlib.import_module(module_name)
