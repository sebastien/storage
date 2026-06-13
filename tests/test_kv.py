# encoding: utf-8
# -----------------------------------------------------------------------------
# Project   : FFCTN/Storage
# -----------------------------------------------------------------------------
# Author    : Sebastien Pierre                            <sebastien@ffctn.com>
# License   : BSD License
# -----------------------------------------------------------------------------
# Creation  : 14-Jun-2026
# Last mod  : 14-Jun-2026
# -----------------------------------------------------------------------------

import os
import shutil
import tempfile
import unittest

from storage.kv import (
	KVError,
	KVFailure,
	KVFull,
	KVStorage,
	KVStorageBackend,
	StringKVKeyNormalizer,
	PathKVKeyNormalizer,
	TupleKVKeyNormalizer,
)
from storage.formats import JSONCodec
from storage.backends.memory import KVMemoryBackend
from storage.backends.sqlite import KVSqliteBackend
from storage.backends.fs import KVFileBackend


# -----------------------------------------------------------------------------
#
# VALUE SUITES (from storage_backends.py convention)
#
# -----------------------------------------------------------------------------

INT = [0, 1, -1, 255, 4096, 0xFFFFFFFF, -0xFFFFFFFF * 0xFFFF]
FLOAT = [0.0, 3.14, -3.14, float("inf"), float("-inf")]
STR = ["", "hello", "a" * 1000, "\u00e9\u00e7", "key/with/slashes"]
LIST = [[], [1, 2, 3], ["a", {"b": "c"}]]
DICT = [{}, {"a": 1}, {"nested": {"list": [1, 2, 3]}}]
NONE = [None]
BOOL = [True, False]

ALL_VALUES = INT + FLOAT + STR + LIST + DICT + NONE + BOOL


# -----------------------------------------------------------------------------
#
# TEST BACKEND BASE
#
# -----------------------------------------------------------------------------


class KVStorageBackendTestBase:
	"""Mixin that provides a ``make_backend`` factory.

	Subclasses set ``backend_cls`` and override ``make_backend`` if needed.
	"""

	def make_backend(self) -> KVStorageBackend:
		raise NotImplementedError

	def make_kv(self, prefix: str = "", **kw) -> KVStorage:
		b = self.make_backend()
		return KVStorage(
			b,
			prefix=prefix,
			normalizer=StringKVKeyNormalizer(),
			codec=JSONCodec(),
			**kw,
		)


# -----------------------------------------------------------------------------
#
# COMMON TESTS
#
# -----------------------------------------------------------------------------


class KVStorageCommonTests(KVStorageBackendTestBase):
	"""Parametric test class exercised by all backends."""

	def test_set_and_get(self):
		kv = self.make_kv()
		kv.set("hello", "world")
		self.assertEqual(kv.get("hello"), "world")

	def test_get_missing(self):
		kv = self.make_kv()
		self.assertIsNone(kv.get("nonexistent"))

	def test_get_none(self):
		kv = self.make_kv()
		kv.set("null", None)
		self.assertIsNone(kv.get("null"))

	def test_set_overwrite(self):
		kv = self.make_kv()
		kv.set("x", 1)
		kv.set("x", 2)
		self.assertEqual(kv.get("x"), 2)

	def test_has(self):
		kv = self.make_kv()
		self.assertFalse(kv.has("a"))
		kv.set("a", 42)
		self.assertTrue(kv.has("a"))

	def test_delete(self):
		kv = self.make_kv()
		kv.set("a", 1)
		kv.delete("a")
		self.assertFalse(kv.has("a"))

	def test_delete_missing(self):
		kv = self.make_kv()
		kv.delete("nonexistent")  # should not raise

	def test_size(self):
		kv = self.make_kv()
		self.assertEqual(kv.size(), 0)
		kv.set("a", 1)
		self.assertEqual(kv.size(), 1)
		kv.set("b", 2)
		self.assertEqual(kv.size(), 2)
		kv.delete("a")
		self.assertEqual(kv.size(), 1)

	def test_clear(self):
		kv = self.make_kv()
		kv.set("a", 1)
		kv.set("b", 2)
		kv.clear()
		self.assertEqual(kv.size(), 0)
		self.assertFalse(kv.has("a"))

	def test_prefix_isolation(self):
		kv1 = self.make_kv(prefix="app1:")
		kv2 = self.make_kv(prefix="app2:")
		kv1.set("key", 10)
		kv2.set("key", 20)
		self.assertEqual(kv1.get("key"), 10)
		self.assertEqual(kv2.get("key"), 20)

	def test_prefix_list(self):
		kv = self.make_kv(prefix="ns:")
		kv.set("a", 1)
		kv.set("b", 2)
		keys = list(kv.ilist())
		self.assertEqual(sorted(keys), ["a", "b"])

	def test_ilist_prefix_filter(self):
		kv = self.make_kv()
		kv.set("user:1", "alice")
		kv.set("user:2", "bob")
		kv.set("admin:1", "carol")
		users = list(kv.ilist("user:"))
		self.assertEqual(sorted(users), ["user:1", "user:2"])

	def test_iitems(self):
		kv = self.make_kv()
		kv.set("a", 1)
		kv.set("b", 2)
		items = sorted(kv.iitems())
		self.assertEqual(items, [("a", 1), ("b", 2)])

	def test_iitems_includes_none(self):
		kv = self.make_kv()
		kv.set("null", None)
		self.assertEqual(list(kv.iitems()), [("null", None)])

	def test_all_value_types(self):
		kv = self.make_kv()
		for i, v in enumerate(ALL_VALUES):
			kv.set("k%d" % i, v)
			self.assertEqual(kv.get("k%d" % i), v)

	def test_setm(self):
		kv = self.make_kv()
		result = kv.setm({"a": 1, "b": 2, "c": 3})
		self.assertEqual(result, {"a": 1, "b": 2, "c": 3})
		self.assertEqual(kv.get("a"), 1)
		self.assertEqual(kv.get("b"), 2)
		self.assertEqual(kv.get("c"), 3)

	def test_getm(self):
		kv = self.make_kv()
		kv.setm({"a": 1, "b": 2, "c": 3})
		result = kv.getm(["a", "c", "missing"])
		self.assertEqual(result, {"a": 1, "c": 3})

	def test_hasm(self):
		kv = self.make_kv()
		kv.set("a", 1)
		kv.set("b", 2)
		result = kv.hasm(["a", "b", "c"])
		self.assertEqual(result, {"a": True, "b": True, "c": False})

	def test_deletem(self):
		kv = self.make_kv()
		kv.setm({"a": 1, "b": 2, "c": 3})
		kv.deletem(["a", "c"])
		self.assertFalse(kv.has("a"))
		self.assertTrue(kv.has("b"))
		self.assertFalse(kv.has("c"))

	def test_return_value(self):
		kv = self.make_kv()
		result = kv.set("x", [1, 2, 3])
		self.assertEqual(result, [1, 2, 3], "set() should return the stored value")

	def test_iitems_with_prefix(self):
		kv = self.make_kv()
		kv.set("cat:1", "meow")
		kv.set("cat:2", "purr")
		kv.set("dog:1", "woof")
		items = list(kv.iitems("cat:"))
		self.assertEqual(sorted(items), [("cat:1", "meow"), ("cat:2", "purr")])


# -----------------------------------------------------------------------------
#
# KEY NORMALIZER TESTS
#
# -----------------------------------------------------------------------------


class TestStringKVKeyNormalizer(unittest.TestCase):
	def test_serialize_parse_roundtrip(self):
		n = StringKVKeyNormalizer()
		self.assertEqual(n.parse(n.serialize("hello")), "hello")

	def test_normalize_string(self):
		n = StringKVKeyNormalizer()
		self.assertEqual(n.normalize("hello"), "hello")

	def test_normalize_list(self):
		n = StringKVKeyNormalizer()
		self.assertEqual(n.normalize(["a", "b", "c"]), "a/b/c")

	def test_join(self):
		n = StringKVKeyNormalizer()
		self.assertEqual(n.join("pfx:", "key"), "pfx:key")

	def test_matches(self):
		n = StringKVKeyNormalizer()
		self.assertTrue(n.matches("pfx:", "pfx:key"))
		self.assertFalse(n.matches("pfx:", "other"))


class TestPathKVKeyNormalizer(unittest.TestCase):
	def test_serialize_parse_roundtrip(self):
		n = PathKVKeyNormalizer()
		orig = ["a", "b", "c"]
		self.assertEqual(n.parse(n.serialize(orig)), orig)

	def test_normalize_string(self):
		n = PathKVKeyNormalizer()
		self.assertEqual(n.normalize("a/b/c"), ["a", "b", "c"])

	def test_normalize_list(self):
		n = PathKVKeyNormalizer()
		self.assertEqual(n.normalize(["a", "b"]), ["a", "b"])

	def test_join(self):
		n = PathKVKeyNormalizer()
		self.assertEqual(n.join("root", ["a", "b"]), "root/a/b")

	def test_matches(self):
		n = PathKVKeyNormalizer()
		self.assertTrue(n.matches("root", ["root", "a", "b"]))
		self.assertFalse(n.matches("root", ["other", "a"]))


class TestTupleKVKeyNormalizer(unittest.TestCase):
	def test_serialize_parse_roundtrip(self):
		n = TupleKVKeyNormalizer(":")
		orig = ("bucket", "key")
		self.assertEqual(n.parse(n.serialize(orig)), orig)

	def test_normalize_string(self):
		n = TupleKVKeyNormalizer(":")
		self.assertEqual(n.normalize("a:b"), ("a", "b"))

	def test_normalize_tuple(self):
		n = TupleKVKeyNormalizer(":")
		self.assertEqual(n.normalize(("a", "b")), ("a", "b"))

	def test_custom_separator(self):
		n = TupleKVKeyNormalizer("/")
		self.assertEqual(n.normalize("a/b"), ("a", "b"))

	def test_join(self):
		n = TupleKVKeyNormalizer(":")
		self.assertEqual(n.join("pfx", ("store", "key")), "pfxstore:key")

	def test_matches(self):
		n = TupleKVKeyNormalizer(":")
		self.assertTrue(n.matches("pfx", ("pfxstore", "key")))
		self.assertFalse(n.matches("pfx", ("other", "key")))


# -----------------------------------------------------------------------------
#
# BACKEND-SPECIFIC TEST CLASSES
#
# -----------------------------------------------------------------------------


class TestKVMemoryBackend(KVStorageCommonTests, unittest.TestCase):
	def make_backend(self) -> KVStorageBackend:
		return KVMemoryBackend()


class TestKVSqliteBackend(KVStorageCommonTests, unittest.TestCase):
	def make_backend(self) -> KVStorageBackend:
		self._tmp = tempfile.NamedTemporaryFile(suffix=".sqlite3", delete=False)
		self._tmp.close()
		return KVSqliteBackend(self._tmp.name)

	def tearDown(self):
		if hasattr(self, "_tmp") and os.path.exists(self._tmp.name):
			os.unlink(self._tmp.name)


class TestKVFileBackend(KVStorageCommonTests, unittest.TestCase):
	def make_backend(self) -> KVStorageBackend:
		self._tmp_dir = tempfile.mkdtemp(prefix="kv_fs_test_")
		return KVFileBackend(self._tmp_dir)

	def tearDown(self):
		if hasattr(self, "_tmp_dir") and os.path.isdir(self._tmp_dir):
			shutil.rmtree(self._tmp_dir)


# -----------------------------------------------------------------------------
#
# NORMALIZER COMPOSITION TESTS (path keys with FileBackend)
#
# -----------------------------------------------------------------------------


class TestKVFileBackendPathKeys(KVStorageBackendTestBase, unittest.TestCase):
	"""Tests KVStorage with PathKVKeyNormalizer + KVFileBackend."""

	def make_backend(self) -> KVStorageBackend:
		self._tmp_dir = tempfile.mkdtemp(prefix="kv_fs_path_")
		return KVFileBackend(self._tmp_dir)

	def tearDown(self):
		if hasattr(self, "_tmp_dir") and os.path.isdir(self._tmp_dir):
			shutil.rmtree(self._tmp_dir)

	def test_path_keys(self):
		b = self.make_backend()
		kv = KVStorage(
			b,
			normalizer=PathKVKeyNormalizer(),
			codec=JSONCodec(),
		)
		kv.set(["a", "b", "c"], 42)
		self.assertEqual(kv.get(["a", "b", "c"]), 42)

	def test_path_ilist(self):
		b = self.make_backend()
		kv = KVStorage(
			b,
			normalizer=PathKVKeyNormalizer(),
			codec=JSONCodec(),
		)
		kv.set(["2024", "jan"], 100)
		kv.set(["2024", "feb"], 200)
		kv.set(["2023", "dec"], 300)
		keys = list(kv.ilist(["2024"]))
		self.assertEqual(sorted(keys), [["2024", "feb"], ["2024", "jan"]])

	def test_path_prefix(self):
		b = self.make_backend()
		kv = KVStorage(
			b,
			prefix="data",
			normalizer=PathKVKeyNormalizer(),
			codec=JSONCodec(),
		)
		kv.set(["users", "alice"], 1)
		self.assertTrue(kv.has(["users", "alice"]))
		self.assertEqual(kv.get(["users", "alice"]), 1)
		self.assertEqual(kv.size(), 1)
		self.assertEqual(list(kv.ilist(["users"])), [["users", "alice"]])


# -----------------------------------------------------------------------------
#
# TUPLE KEY NORMALIZER TESTS
#
# -----------------------------------------------------------------------------


class TestKVStorageTupleKeys(KVStorageBackendTestBase, unittest.TestCase):
	"""Tests KVStorage with TupleKVKeyNormalizer + MemoryBackend."""

	def make_backend(self) -> KVStorageBackend:
		return KVMemoryBackend()

	def test_tuple_keys(self):
		b = self.make_backend()
		kv = KVStorage(
			b,
			prefix="",
			normalizer=TupleKVKeyNormalizer(":"),
			codec=JSONCodec(),
		)
		kv.set(("bucket", "key"), "value")
		self.assertEqual(kv.get(("bucket", "key")), "value")

	def test_tuple_prefix(self):
		b = self.make_backend()
		kv = KVStorage(
			b,
			prefix="prod",
			normalizer=TupleKVKeyNormalizer(":"),
			codec=JSONCodec(),
		)
		kv.set(("users", "123"), "alice")
		self.assertEqual(kv.get(("users", "123")), "alice")
		self.assertEqual(kv.size(), 1)
		self.assertEqual(list(kv.ilist(("users",))), [("users", "123")])


# -----------------------------------------------------------------------------
#
# MAIN
#
# -----------------------------------------------------------------------------

if __name__ == "__main__":
	unittest.main()
