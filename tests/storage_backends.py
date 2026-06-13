# encoding: utf-8
# -----------------------------------------------------------------------------
# Project   : FFCTN/Storage
# -----------------------------------------------------------------------------
# Author    : Sebastien Pierre                            <sebastien@ffctn.com>
# License   : BSD License
# -----------------------------------------------------------------------------
# Creation  : 17-Jun-2013
# Last mod  : 03-Oct-2013
# -----------------------------------------------------------------------------

import unittest
import storage
import datetime
import io
import time
import os
import shutil

# -----------------------------------------------------------------------------
#
# TEST TYPES
#
# -----------------------------------------------------------------------------

# The following globals define instances of values that can be used to test
# the backends.

INT_DEFAULT = [int()]
UINT32 = [1, 255, 4096, 0xFFFFFFFF]
INT32 = [-0xFFFFFFFE, -1, 1, 0x7FFFFFFF]
ZERO = [0]
INT_OVERFLOW = [-0xFFFFFFFF * 0xFFFF, 0xFFFFFFFF * 0xFFFF]
INT = INT32 + ZERO + INT_OVERFLOW + INT_DEFAULT

LONG_DEFAULT = [int()]
LONG_POSITIVE = [12, 1000000000000000000000]
LONG_NEGATIVE = [-12, -100000000000000000000]
LONG_ZERO = [0]
LONG = LONG_NEGATIVE + LONG_ZERO + LONG_POSITIVE + LONG_DEFAULT

FLOAT_DEFAULT = [float()]
FLOAT_POSITIVE = [3.14]
FLOAT_NEGATIVE = [-3.14]
# FLOAT_OVERFLOW       = [-256.0**256,256.0**256]
# FLOAT_UNDERFLOW      = [1/-256.0**256,1/256.0**256]
FLOAT_ZERO = [0.0]
FLOAT = FLOAT_POSITIVE + FLOAT_NEGATIVE + FLOAT_ZERO + FLOAT_DEFAULT

FLOAT_SPECIAL = [float("NaN"), float("-inf"), float("inf")]

CHAR_ASCII = ["a", chr(100), "Z"]
CHAR_UNICODE = [chr(97), chr(2473)]
CHAR_DIGIT = ["1", "9"]
CHAR_SPECIAL = [".", "?", "&", "*", "(", "\\", '"']
CHAR_FOREIGN = ["é", "ç"]
CHAR = CHAR_ASCII + CHAR_UNICODE + CHAR_DIGIT + CHAR_SPECIAL + CHAR_FOREIGN

STRING_DEFAULT = [str()]
STRING_UNICODE = ["é".encode("utf-8")]
STRING_SHORT = ["A", "a"]
STRING_DIGIT = ["0", "1", "000000", "01", "20", "0xFF"]
STRING_SPECIAL = ["*", "&", "È", "-", "+", "_", "\\"]
STRING_LONG = ["KEY" * 256, "KEY" * 1024, "KEY" * 2048, "KEY" * 4096]
STRING = STRING_DEFAULT + STRING_UNICODE + STRING_SHORT + STRING_DIGIT + STRING_SPECIAL

TUPLE_DEFAULT = [tuple()]
TUPLE_SIMPLE = [(1, 2, 3, 4), (1, "a", 3.6)]
TUPLE_NESTED = [
	((1), (1), (1)),
	((1), ("A"), (1.3)),
	((1), ((2), ("a")), (("v"), ((3.4), ("b", 5)))),
]
TUPLE_MIX = [([1, 2, 3], {3: "a", "4": 5})]
TUPLE = TUPLE_DEFAULT + TUPLE_SIMPLE + TUPLE_NESTED + TUPLE_MIX

LIST_DEFAULT = [list()]
LIST_SIMPLE = [[1, 2, 3, 4], [1, "a", 3.6]]
LIST_NESTED = [
	[[1], [1], [1]],
	[[1], ["A"], [1.3]],
	[[1], [[2], ["a"]], [["v"], [[3.4], ["b", 5]]]],
]
LIST_MIX = [[[1, 2, 3], {3: "a", "4": 5}]]
LIST = LIST_DEFAULT + LIST_SIMPLE + LIST_NESTED + LIST_MIX

DICT_DEFAULT = [dict()]
DICT_SIMPLE = [{"a": 1, "b": 2}, {1: 5}]
DICT_NESTED = [{"AB": {"a": 2, "b": 4}, "CD": {"c": 1, "d": 3}}]
DICT_MIX = [{"a": (1, 2, 3), "b": ["a", 3, 4.5]}]
DICT = DICT_DEFAULT + DICT_SIMPLE + DICT_NESTED + DICT_MIX

SET = [set()]

BOOL = [True, False]

PY_CONST = [None, NotImplemented, Ellipsis]

EXCEPTION = [Exception]

CLASS = [datetime.timedelta()]

GENERATORS = [(_ for _ in range(20))]
LAMBDA = [lambda x: x**2]

# -----------------------------------------------------------------------------
#
# ABSTRACT BACKEND TEST
#
# -----------------------------------------------------------------------------


class AbstractBackendTest:
	"""An abstract test that exercises all the methods of the storage.Backend
	interface. Override the `_createBackend` to return a specific backend
	instance in subclasses."""

	KEYS_VALID = [
		"a",
		"Z",
		"1",
		"0xFF",
		"alpha.beta",
		"é",
		"KEY" * 32,
	]
	KEYS_INVALID = [
		object(),
		(_ for _ in range(2)),
		lambda x: x**2,
	]
	VALUES_VALID = [
		"",
		"A",
		"é",
		True,
		False,
		None,
		1,
		1.0,
		-1,
		[1, 1],
		[1, "a"],
		{"a": 1, "b": 1},
		{"a": 1, "b": "a"},
		[[], [], []],
		{"a": {}, "b": {}, "c": {}},
	]
	VALUES_INVALID = []

	def _createBackend(self):
		raise NotImplementedError

	def setUp(self):
		self.path = "./" + os.path.basename(__file__).split(".")[0]
		self.backend = self._createBackend()
		self.backend.clear()

	def testAdd(self):
		# setup
		self.assertEqual(0, self.backend.count())
		# Tests the keys
		count = 0
		for key in self.KEYS_VALID:
			self.backend.add(key, "OK")
			self.assertEqual(count + 1, self.backend.count())
			count += 1
		# Tests the values
		count = self.backend.count()
		for i, v in enumerate(self.VALUES_VALID):
			self.backend.add("value_" + str(i), v)
			self.assertEqual(count + i + 1, self.backend.count())
		# Tests value transparency
		for i, v in enumerate(self.VALUES_VALID):
			self.assertEqual(self.backend.get("value_" + str(i)), v)
		# Overriding a key (key update)
		for key in self.KEYS_VALID:
			key = "key_" + key
			self.backend.add(key, "OK")
			self.assertEqual(self.backend.get(key), "OK")
			for v in self.VALUES_VALID:
				self.backend.set(key, v)
				self.assertEqual(self.backend.get(key), v)
		# Invalid key [assuming not accepted]
		for k in self.KEYS_INVALID:
			self.assertRaises(Exception, self.backend.add, k, "OK")

	def testUpdate(self):
		# setup
		self.assertEqual(0, self.backend.count())
		for k in self.KEYS_VALID:
			self.backend.add(k, "OK")
		# simple
		for k in self.KEYS_VALID:
			self.assertMultiLineEqual("OK", self.backend.get(k))
			for v in self.VALUES_VALID:
				self.backend.update(k, v)
				self.assertEqual(v, self.backend.get(k))
		# update removed entry
		count = self.backend.count()
		for k in self.KEYS_VALID:
			self.backend.remove(k)
			count = count - 1
			self.assertEqual(count, self.backend.count())
			self.backend.update(k, "OK")
			self.assertEqual("OK", self.backend.get(k))
			self.backend.remove(k)
		# invalid key
		for k in self.KEYS_INVALID:
			self.assertRaises(Exception, self.backend.update, k, "OK")

	def testRemove(self):
		# setup
		self.assertEqual(0, self.backend.count())
		for k in self.KEYS_VALID:
			self.backend.add(k, "OK")
		# simple
		count = len(self.KEYS_VALID)
		for k in self.KEYS_VALID:
			self.assertEqual(count, self.backend.count())
			self.backend.remove(k)
			self.assertEqual(count - 1, self.backend.count())
			count = count - 1
			self.assertNotIn(k, self.backend.keys())
		# invalid keys
		for k in self.KEYS_INVALID:
			self.assertRaises(Exception, self.backend.remove, k)

	def testSync(self):
		# Make sure sync is implemented
		# TODO: Needs to be elaborated
		self.backend.sync()

	def testHas(self):
		# setup
		self.assertEqual(0, self.backend.count())
		for key in self.KEYS_VALID:
			self.assertFalse(self.backend.has(key))
		for key in self.KEYS_VALID:
			self.backend.add(key, "OK")
		for key in self.KEYS_VALID:
			self.assertTrue(self.backend.has(key))
		for key in self.KEYS_VALID:
			self.backend.remove(key)
		for key in self.KEYS_VALID:
			self.assertFalse(self.backend.has(key))

	def testGet(self):
		# setup
		self.assertEqual(0, self.backend.count())
		for k in self.KEYS_VALID:
			self.backend.add(k, "OK")
		# undefined
		self.assertIsNone(self.backend.get("undefined_key"))
		# valid keys test
		for k in self.KEYS_VALID:
			self.assertMultiLineEqual("OK", self.backend.get(k))
		self.backend.clear()
		assert self.backend.count() == 0, "ERROR: Backend is not empty"
		# valid values test
		for k, v in zip(
			[str(i) for i in range(len(self.VALUES_VALID))], self.VALUES_VALID
		):
			self.backend.add(k, v)
			self.assertEqual(v, self.backend.get(k))
		# invalid key
		for k in self.KEYS_INVALID:
			self.assertRaises(Exception, self.backend.get, k)
		# removed
		for k in [str(i) for i in range(len(self.VALUES_VALID))]:
			self.backend.remove(k)
			self.assertIsNone(self.backend.get(k))

	def testKeys(self):
		# setup
		self.assertEqual(0, self.backend.count())
		# empty database
		klist = []
		for k in self.backend.keys():
			klist += [k]
		self.assertListEqual(klist, [])
		# keys
		for k in self.KEYS_VALID:
			self.backend.add(k, "OK")
		for k in self.backend.keys():
			self.assertIn(k, self.KEYS_VALID)
		# removed
		for k in self.KEYS_VALID:
			self.backend.remove(k)
		klist = []
		for k in self.backend.keys():
			klist += [k]
		self.assertListEqual(klist, [])

	def testLongKeys(self):
		"""Makes sure that really long keys can be used."""
		long_keys = ["KEY" * 32, "KEY" * 64, "KEY" * 128]
		for key, i in enumerate(long_keys):
			self.backend.set(str(key), str(i))
		for key, i in enumerate(long_keys):
			key = str(key)
			self.assertTrue(self.backend.has(key))
			self.assertEqual(self.backend.get(key), str(i))

	def testClear(self):
		# setup
		self.assertEqual(0, self.backend.count())
		# clear empty database
		self.backend.clear()
		self.assertEqual(0, self.backend.count())
		# clear database
		for k in self.KEYS_VALID:
			self.backend.add(k, "OK")
		self.assertNotEqual(0, self.backend.count())
		self.backend.clear()
		self.assertEqual(0, self.backend.count())

	def testList(self):
		# setup
		self.assertEqual(0, self.backend.count())
		# empty database
		values_list = []
		for item in self.backend.list():
			values_list += [item]
		self.assertListEqual([], values_list)
		# list of values
		for i, v in enumerate(self.VALUES_VALID):
			self.backend.add("key_" + repr(i), v)
		for item in self.backend.list():
			self.assertIn(item, self.VALUES_VALID)
		# list after remove
		for i in range(len(self.VALUES_VALID)):
			self.backend.remove("key_" + repr(i))
		values_list = []
		for item in self.backend.list():
			values_list += [item]
		self.assertListEqual([], values_list)

	def testCount(self):
		# empty
		self.assertEqual(0, self.backend.count())
		# count entries
		for k in self.KEYS_VALID:
			self.backend.add(k, "OK")
		self.assertEqual(len(self.KEYS_VALID), self.backend.count())
		# remove
		count = len(self.KEYS_VALID)
		for k in self.KEYS_VALID:
			self.backend.remove(k)
			self.assertEqual(count - 1, self.backend.count())
			count = count - 1


# -----------------------------------------------------------------------------
#
# DBM BACKEND TEST
#
# -----------------------------------------------------------------------------


class DBMBackendTest(AbstractBackendTest, unittest.TestCase):
	def _createBackend(self):
		# erase the file to clear the database
		if os.path.exists(self.path + ".dbm"):
			os.remove(self.path + ".dbm")
		return storage.DBMBackend(self.path)

	def tearDown(self):
		self.backend.close()
		if os.path.exists(self.path + ".dbm"):
			os.remove(self.path + ".dbm")

	def testClose(self):
		# setup
		self.assertEqual(0, self.backend.count())
		# close
		for k in self.KEYS_VALID:
			self.backend.add(k, "OK")
		for i, v in enumerate(self.VALUES_VALID):
			self.backend.add("key_" + str(i), v)
		self.assertNotEqual(0, self.backend.count())
		# FIXME: sync before closing
		self.backend.close()
		for k in self.KEYS_VALID:
			self.backend.update(k, "new_value")
			self.assertEqual(self.backend.get(k), "new_value")
		# repeated close is a no-op
		self.assertTrue(self.backend.close())
		# reopening backend
		self.backend._open()
		keys = []
		for k in self.backend.keys():
			keys.append(k)
		for k in self.KEYS_VALID:
			self.assertIn(k, keys)
		for i in range(len(self.VALUES_VALID)):
			self.assertIn("key_" + str(i), keys)
		self.assertEqual(len(keys), len(self.KEYS_VALID) + len(self.VALUES_VALID))


# -----------------------------------------------------------------------------
#
# SQLITE BACKEND TEST
#
# -----------------------------------------------------------------------------


class SQLiteBackendTest(AbstractBackendTest, unittest.TestCase):
	def _createBackend(self):
		return storage.SQLiteBackend(self.path)

	def tearDown(self):
		self.backend.close()
		for suffix in (".sqlite3", ".sqlite3-shm", ".sqlite3-wal"):
			if os.path.exists(self.path + suffix):
				os.remove(self.path + suffix)

	def testClose(self):
		self.assertEqual(0, self.backend.count())
		for k in self.KEYS_VALID:
			self.backend.add(k, "OK")
		self.backend.close()
		for k in self.KEYS_VALID:
			self.backend.update(k, "new_value")
			self.assertEqual(self.backend.get(k), "new_value")
		self.assertTrue(self.backend.close())
		self.backend._open()
		self.assertEqual(len(self.KEYS_VALID), self.backend.count())

	def testPrefixKeysAndCount(self):
		self.backend.add("User.1", "A")
		self.backend.add("User.2", "B")
		self.backend.add("Message.1", "C")
		self.assertEqual(2, self.backend.count("User"))
		self.assertListEqual(["User.1", "User.2"], sorted(self.backend.keys("User")))

	def testRawData(self):
		self.assertFalse(self.backend.hasRawData("blob"))
		self.assertListEqual([None], list(self.backend.streamRawData("blob")))
		self.backend.saveRawData("blob", io.BytesIO(b"abcdef"))
		self.assertTrue(self.backend.hasRawData("blob"))
		self.assertListEqual([b"ab", b"cd", b"ef"], list(self.backend.streamRawData("blob", size=2)))
		self.assertRaises(NotImplementedError, self.backend.getRawDataPath, "blob")


# -----------------------------------------------------------------------------
#
# MEMORY BACKEND TEST
#
# -----------------------------------------------------------------------------


class MemoryBackendTest(AbstractBackendTest, unittest.TestCase):
	def _createBackend(self):
		return storage.MemoryBackend()


# -----------------------------------------------------------------------------
#
# JOURNAL BACKEND TEST
#
# -----------------------------------------------------------------------------


class JournalBackendTest(AbstractBackendTest, unittest.TestCase):
	def _createBackend(self):
		return storage.JournalBackend(storage.MemoryBackend())

	def testJournalChanges(self):
		self.backend.add("User.1", {"id": "1", "type": "User", "value": "A"})
		cursor = self.backend.getCursor()
		self.backend.update("User.1", {"id": "1", "type": "User", "value": "B"})
		self.backend.remove("User.1")

		changes = self.backend.getChanges(since=cursor)
		self.assertEqual(2, len(changes))
		self.assertEqual("+", changes[0]["operation"])
		self.assertEqual("-", changes[1]["operation"])
		self.assertEqual(["User.1"], self.backend.getChangedKeys(since=cursor))

	def testJournalUpdatePayload(self):
		self.backend.add("User.1", {"id": "1", "type": "User", "value": "A"})
		update = self.backend.getUpdate(since=0)
		self.assertEqual(self.backend.getCursor(), update["cursor"])
		self.assertFalse(update["compacted"])
		self.assertEqual(["User.1"], update["changed"])
		self.assertEqual("", update["changes"][0]["patch"][0]["path"])

	def testJournalRelationDelta(self):
		self.backend.add(
			"Message.1",
			{
				"id": "1",
				"type": "Message",
				"replyTo": [{"id": "2", "type": "Message"}],
			},
		)
		self.backend.update(
			"Message.1",
			{
				"id": "1",
				"type": "Message",
				"replyTo": [{"id": "3", "type": "Message"}],
			},
		)
		change = self.backend.getChanges()[-1]
		self.assertEqual(
			[{"id": "3", "type": "Message"}], change["relations"]["replyTo"]["added"]
		)
		self.assertEqual(
			[{"id": "2", "type": "Message"}], change["relations"]["replyTo"]["removed"]
		)

	def testJournalSubscribePrefix(self):
		seen = []
		self.backend.subscribe("User.", lambda key, operation, entry: seen.append(entry))
		self.backend.add("User.1", "A")
		self.backend.add("Message.1", "B")
		self.assertEqual(1, len(seen))
		self.assertEqual("User.1", seen[0]["key"])

	def testJournalCompaction(self):
		self.backend.maxEntriesPerKey = 2
		self.backend.add("User.1", {"id": "1", "type": "User", "value": "A"})
		self.backend.update("User.1", {"id": "1", "type": "User", "value": "B"})
		self.backend.update("User.1", {"id": "1", "type": "User", "value": "C"})

		update = self.backend.getUpdate(since=0)
		self.assertTrue(update["compacted"])
		self.assertEqual(["User.1"], update["changed"])
		self.assertEqual("User.1", update["snapshots"][0]["key"])
		self.assertEqual("C", update["snapshots"][0]["value"]["value"])

	def testJournalRawMetadata(self):
		backend = storage.JournalBackend(storage.SQLiteBackend(self.path + "-raw"))
		try:
			backend.clear()
			backend.saveRawData("blob", b"abcdef")
			change = backend.getChanges()[0]
			self.assertEqual("+R", change["operation"])
			self.assertEqual(6, change["meta"]["size"])
			self.assertIsNotNone(change["meta"]["sha256"])
		finally:
			backend.backend.close()
			for suffix in (".sqlite3", ".sqlite3-shm", ".sqlite3-wal"):
				if os.path.exists(self.path + "-raw" + suffix):
					os.remove(self.path + "-raw" + suffix)


# -----------------------------------------------------------------------------
#
# DIRECTORY BACKEND TEST
#
# -----------------------------------------------------------------------------


class DirectoryBackendTest(AbstractBackendTest, unittest.TestCase):
	def _createBackend(self):
		return storage.DirectoryBackend(os.getcwd() + "/test-dir/")

	@classmethod
	def setUpClass(cls):
		cls.path = os.getcwd() + "/test-dir"
		if not (os.path.exists(cls.path)):
			os.mkdir(cls.path + "/")

	@classmethod
	def tearDownClass(cls):
		if os.path.exists(cls.path):
			shutil.rmtree(cls.path)

	def setUp(self):
		self.backend = self._createBackend()
		keys = self.backend.keys()
		for k in keys:
			self.backend.remove(k)

	def testGetFileName(self):
		# setup
		self.assertEqual(0, self.backend.count())
		# get file
		for k in self.KEYS_VALID:
			# undefined key
			self.assertIsNone(self.backend.getFileName(k))
			# defined key
			self.backend.add(k, "OK")
			self.assertIsNotNone(self.backend.getFileName(k))
			self.assertEqual(self.backend.path(k), self.backend.getFileName(k))
		# invalid
		for k in self.KEYS_INVALID:
			self.assertRaises(Exception, self.backend.getFileName, k)

	def testKeyPathMapping(self):
		for k in self.KEYS_VALID:
			path = self.backend._defaultKeyToPath(None, k)
			self.assertMultiLineEqual(k, self.backend._defaultPathToKey(None, path))

	def testDefaultReadWrite(self):
		for i, v in enumerate(self.VALUES_VALID):
			self.backend.writeFile(self.backend.root + "key_" + repr(i), repr(v))
			val = self.backend.readFile(self.backend.root + "key_" + repr(i))
			self.assertMultiLineEqual(val.decode("utf-8"), repr(v))


if __name__ == "__main__":
	unittest.main()

# EOF
