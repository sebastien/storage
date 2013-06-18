# encoding:UTF-8
# Project   : FFCTN/Storage
# -----------------------------------------------------------------------------
# Author    : Sebastien Pierre                            <sebastien@ffctn.com>
# License   : BSD License
# -----------------------------------------------------------------------------
# Creation  : 17-Jun-2013
# Last mod  : 17-Jun-2013
# -----------------------------------------------------------------------------

import sys, unittest, time, datetime, random, os, shutil
import storage
from test_types import *

# -----------------------------------------------------------------------------
#
# ABSTRACT BACKEND TEST
#
# -----------------------------------------------------------------------------

class AbstractBackendTest:
	"""An abstract test that exercises all the methods of the storage.Backend
	interface. Override the `_createBackend` to return a specific backend
	instance in subclasses."""

	KEYS_VALID    = STRING

	KEYS_INVALID  = []
	KEYS_INVALID += INT
	KEYS_INVALID += LONG
	KEYS_INVALID += FLOAT
	KEYS_INVALID += FLOAT_SPECIAL
	KEYS_INVALID += CHAR
	#KEYS_INVALID += STRING
	KEYS_INVALID += TUPLE
	KEYS_INVALID += LIST
	KEYS_INVALID += DICT
	KEYS_INVALID += BOOL
	KEYS_INVALID += PY_CONST
	KEYS_INVALID += EXCEPTION
	KEYS_INVALID += CLASS
	KEYS_INVALID += GENERATORS
	KEYS_INVALID += LAMBDA

	VALUES_VALID  = KEYS_VALID + [
			True, False, None,
			1, 1.0, long(12313212),
			tuple(),list(),dict(),
			(1,1), [1,1], {"a":1, "b":1},
			(1,"a"), [1,"a"], {"a":1, "b":"a"},
			((),(),()), [[],[],[]], {"a":{}, "b":{}, "c":{}},
			([],[]),[(),()],
			time.time(),
			time.gmtime(),
			datetime.datetime(2013,2,6)
		]

	VALUES_INVALID = [
		object(), range(2)
	]

	def _createBackend( self ):
		raise NotImplementedError

	def setUp(self):
		self.path    = "./" + os.path.basename(__file__).split(".")[0]
		self.backend = self._createBackend()
		self.backend.clear()
		
	def testAdd(self):
		#setup
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
			self.backend.add("value_" + str(i), str(v))
			self.assertEqual(count + i + 1, self.backend.count())
		# Tests value transparency
		for i, v in enumerate(self.VALUES_VALID):
			self.assertEqual(self.backend.get("value_" + str(i)), str(v))
		# Overriding a key (key update)
		# TODO: Should this raise an error?
		for key in self.KEYS_VALID:
			key = "key_" + key
			self.backend.add(key, "OK")
			self.assertEqual(self.backend.get(key), "OK")
			for v in self.VALUES_VALID:
				# FIXME: Should raise an exception because the key is already defined
				self.assertRaises(Exception, self.backend.add, key, v)
		#invalid key [assuming not accepted]
		for k in self.KEYS_INVALID:
			self.assertRaises(Exception, self.backend.add, k, "OK")
		#invalid value [assuming accepted]
		keys = [str(i) for i in range(len(self.VALUES_INVALID))]
		for k, v in zip(keys, self.VALUES_INVALID):
			self.assertRaises(Exception, self.backend.add, k, v)

	def testUpdate(self):
		#setup
		self.assertEqual(0, self.backend.count())
		for k in self.KEYS_VALID:
			self.backend.add(k, "OK")
		#simple
		for k in self.KEYS_VALID:
			self.assertMultiLineEqual("OK", self.backend.get(k))
			for v in self.VALUES_VALID:
				self.backend.update(k, v)
				self.assertMultiLineEqual(str(v), self.backend.get(k))
		#update undefined entry
		self.assertRaises(Exception, self.backend.update, "undefined_key", "OK")
		#update removed entry
		count = self.backend.count()
		for k in self.KEYS_VALID:
			self.backend.remove(k)
			count = count - 1 
			self.assertEqual(count, self.backend.count())
			self.assertRaises(Exception, self.backend.update, k, "OK")
		#invalid key		
		for k in self.KEYS_INVALID:
			self.assertRaises(Exception, self.backend.update, k, "OK")
		#invalid values
		keys = [str(i) for i in range(len(self.VALUES_INVALID))]
		for k, v in zip(keys,self.VALUES_INVALID):
			self.assertRaises(Exception, self.backend.update, k, v)
		
	def testRemove(self):
		#setup
		self.assertEqual(0, self.backend.count())
		for k in self.KEYS_VALID:
			self.backend.add(k, "OK")
		#simple
		count = len(self.KEYS_VALID)
		for k in self.KEYS_VALID:
			self.assertEqual(count, self.backend.count())
			self.backend.remove(k)
			self.assertEqual(count-1, self.backend.count())
			count = count - 1
			self.assertNotIn(k, self.backend.keys())
		#remove undefined entry
		self.assertRaises(Exception, self.backend.remove, "undefined key")
		#invalid keys
		for k in self.KEYS_INVALID:
			self.assertRaises(Exception, self.backend.remove, k)
		
	def testSync(self):
		# Make sure sync is implemented
		# TODO: Needs to be elaborated
		self.backend.sync()
		
	def testHas(self):
		#setup
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
		#setup
		self.assertEqual(0, self.backend.count())
		for k in self.KEYS_VALID:
			self.backend.add(k, "OK")
		#undefined
		self.assertIsNone(self.backend.get("undefined_key"))
		#valid keys test
		for k in self.KEYS_VALID:
			self.assertMultiLineEqual("OK", self.backend.get(k))
		self.backend.clear()
		assert self.backend.count()==0, "ERROR: Backend is not empty"
		#valid values test
		for k, v in zip([str(i) for i in range(len(self.VALUES_VALID))], self.VALUES_VALID):
			self.backend.add(k, v)
			self.assertMultiLineEqual(str(v), self.backend.get(k))
		#invalid key
		for k in KEYS_INVALID:
			self.assertRaises(Exception, self.backend.get, k)
		#removed
		for k in [str(i) for i in range(len(self.VALUES_VALID))]:
			self.backend.remove(k)
			self.assertIsNone(self.backend.get(k))
		
	def testKeys(self):
		#setup
		self.assertEqual(0, self.backend.count())
		#empty database
		klist=[]
		for k in self.backend.keys():
			klist+=[k]
		self.assertListEqual(klist, [])
		#keys
		for k in self.KEYS_VALID:
			self.backend.add(k, "OK")
		for k in self.backend.keys():
			self.assertIn(k, self.KEYS_VALID)
		#removed
		for k in self.KEYS_VALID:
			self.remove(k)
		klist=[]
		for k in self.backend.keys():
			klist+=[k]
		self.assertListEqual(klist, [])
		
	def testClear(self):
		#setup
		self.assertEqual(0, self.backend.count())
		#clear empty database
		self.backend.clear()
		self.assertEqual(0, self.backend.count())
		#clear database
		for k in self.KEYS_VALID:
			self.backend.add(k, "OK")	
		self.assertNotEqual(0, self.backend.count())
		self.backend.clear()
		self.assertEqual(0, self.backend.count())
		
	def testList(self):
		#setup
		self.assertEqual(0, self.backend.count())
		#empty database
		values_list = []
		for item in self.backend.list():
			values_list += [item]
		self.assertListEqual([], values_list)
		#list of values		
		for i, v in enumerate(self.VALUES_VALID):
			self.backend.add("key_"+repr(i), v)	
		for item in self.backend.list():
			self.assertIn(item, [str(i) for i in self.VALUES_VALID])
		#list after remove
		for i in range(len(self.VALUES_VALID)):
			self.remove("key_"+repr(i))
		values_list=[]
		for item in self.backend.list():
			values_list += [item]
		self.assertListEqual([], values_list)

		
	def testCount(self):
		#empty
		self.assertEqual(0, self.backend.count())
		#count entries
		for k in self.KEYS_VALID:
			self.backend.add(k, "OK")
		self.assertEqual(len(self.KEYS_VALID), self.backend.count())
		#remove
		count = len(self.KEYS_VALID)
		for k in self.KEYS_VALID:
			self.backend.remove(k)
			self.assertEqual(count-1, self.backend.count())
			count = count - 1


# -----------------------------------------------------------------------------
#
# DBM BACKEND TEST
#
# -----------------------------------------------------------------------------

class DBMBackendTest(AbstractBackendTest, unittest.TestCase):

	def _createBackend( self ):
		#erase the file to clear the database
		if (os.path.exists(self.path+".db")):
			os.remove(self.path+".db")
		return storage.DBMBackend(self.path)
		
	def tearDown(self):
		self.backend.close()
		if (os.path.exists(self.path+".db")):
			os.remove(self.path+".db")

	def testClose(self):
		#setup
		self.assertEqual(0, self.backend.count())
		#close
		for k in self.KEYS_VALID:
			self.backend.add(k, "OK")
		for i, v in enumerate(self.VALUES_VALID):
			self.backend.add("key_"+str(i), v)
		self.assertNotEqual(0, self.backend.count())
		#FIXME: sync before closing
		self.backend.close()
		for k in self.KEYS_VALID:
			self.assertRaises(Exception, self.backend.update, k, "new_value")
		#closed backend exception
		self.assertRaises(Exception, self.backend.close)
		#reopening backend
		self.backend._open()
		keys = []
		for k in self.backend.keys():
			keys.append(k)
		for k in self.KEYS_VALID:
			self.assertIn(k, keys)
		for i in range(len(self.VALUES_VALID)):
			self.assertIn("key_"+str(i), keys)
		self.assertEqual(len(keys), len(self.KEYS_VALID)+len(self.VALUES_VALID))

# -----------------------------------------------------------------------------
#
# MEMORY BACKEND TEST
#
# -----------------------------------------------------------------------------
#@unittest.skip("Memory")
class MemoryBackendTest(AbstractBackendTest, unittest.TestCase):
	
	def _createBackend(self):
		return storage.MemoryBackend()


# -----------------------------------------------------------------------------
#
# DIRECTORY BACKEND TEST
#
# -----------------------------------------------------------------------------
#@unittest.skip("Directory")
class DirectoryBackendTest(AbstractBackendTest, unittest.TestCase):

	def _createBackend(self):
		return storage.DirectoryBackend(os.getcwd()+"/test-dir/")

	@classmethod
	def setUpClass(cls):
		cls.path=os.getcwd()+"/test-dir"
		if not (os.path.exists(cls.path)):
			os.mkdir(cls.path+"/")
		
	@classmethod
	def tearDownClass(cls):
		if (os.path.exists(cls.path)):
			shutil.rmtree(cls.path)
	
	def setUp(self):
		self.backend = self._createBackend()
		keys = self.backend.keys()
		for k in keys:
			self.backend.remove(k)

	def testGetFileName(self):
		#setup
		self.assertEqual(0, self.backend.count())
		#get file
		for k in self.KEYS_VALID:
			#undefined key
			self.assertIsNone(self.backend.getFileName(k))
			#defined key
			self.backend.add(k, "OK")
			self.assertIsNotNone(self.backend.getFileName(k))
			self.assertMultiLineEqual(self.path+"/"+k, self.backend.getFileName(k))
		#invalid
		for k in self.KEYS_INVALID:
			self.assertRaises(Exception, self.getFileName, k)

	def testKeyPathMapping(self):
		for k in self.KEYS_VALID:
			path = self.backend._defaultKeyToPath(None,k)
			self.assertMultiLineEqual(k, self.backend._defaultPathToKey(None, path))

	def testDefaultReadWrite(self):
		for i, v in enumerate(self.VALUES_VALID):
			self.backend.writeFile(self.backend.root+"key_"+repr(i), v)
			val = self.backend.readFile(self.backend.root+"key_"+repr(i))
			self.assertMultiLineEqual(val, repr(v))

if __name__ == "__main__":
	unittest.main()

# EOF
