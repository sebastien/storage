"""Contract tests for raw payload storage."""

import io
import shutil
import tempfile
import unittest

from storage.backends.fs import DirectoryBackend
from storage.backends.memory import MemoryBackend
from storage.raw import RawStorage, StoredRaw


class RawStorageTest(unittest.TestCase):
	def setUp(self):
		self.root = tempfile.mkdtemp(prefix="storage-raw-")
		self.storage = RawStorage(DirectoryBackend(self.root)).use(StoredRaw)

	def tearDown(self):
		self.storage.release()
		shutil.rmtree(self.root)

	def test_metadata_only_object(self):
		raw = StoredRaw(id="meta-only", title="example")
		raw.save()

		loaded = StoredRaw.Get("meta-only")
		self.assertEqual("example", loaded.meta("title"))
		self.assertEqual([], list(loaded.data()))

	def test_bytes_payload_round_trip(self):
		raw = StoredRaw(b"payload", id="bytes")
		raw.save()

		self.assertEqual(b"payload", b"".join(StoredRaw.Get("bytes").data()))

	def test_file_like_payload_round_trip(self):
		raw = StoredRaw(io.BytesIO(b"streamed"), id="stream")
		raw.save()

		self.assertEqual(b"streamed", b"".join(StoredRaw.Get("stream").data()))

	def test_metadata_update_does_not_replace_payload(self):
		raw = StoredRaw(b"payload", id="stable")
		raw.save()
		raw.setMeta(title="updated").save()

		loaded = StoredRaw.Get("stable")
		self.assertEqual("updated", loaded.meta("title"))
		self.assertEqual(b"payload", b"".join(loaded.data()))


class RawStorageMemoryTest(unittest.TestCase):
	def test_storage_can_be_constructed_with_memory_backend(self):
		storage = RawStorage(MemoryBackend())
		self.assertIsNotNone(storage)
