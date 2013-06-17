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

# -----------------------------------------------------------------------------
#
# ABSTRACT BACKEND TEST
#
# -----------------------------------------------------------------------------

class AbstractBackendTest:
	"""An abstract test that exercises all the methods of the storage.Backend
	interface. Override the `_createBackend` to return a specific backend
	instance in subclasses."""

	KEYS_VALID    = ["", u"", u"é".encode("utf-8"), "A", "a", "0", "1", "*", "&", "é", "-", "+", "_", "\\", "key", "key_1", "key1", "000", "123", "KEY" * 256, "KEY" * 1024, "KEY" * 2048, "KEY" * 4096]

	KEYS_INVALID  = [Exception(), object(), datetime.datetime(2013,2,6), None, True, False]
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
		object(),range(2)
	]

	def _createBackend( self ):
		raise NotImplementedError

	def setUp(self):
		self.path    = "./" + os.path.basename(__file__).split(".")[0]
		self.backend = self._createBackend()
		self.backend.clear()
		
	def testAdd(self):

		a = self.backend.keys()
		for i in a:
			print (i)
		
		self.assertEqual(0,       self.backend.count())

		# Tests the keys
		count = 0
		for key in self.KEYS_VALID:
			self.backend.add(key, "OK")
			self.assertEqual(count + 1, self.backend.count())
			count += 1

		# Tests the values
		count = self.backend.count()
		for i,v in enumerate(values):
			self.backend.add("value_" + str(i), v)
			self.assertEqual(count + i + 1, self.backend.count())

		# Tests value transparency
		for i,v in enumerate(values):
			self.assertEqual(self.backend.get("value_" + str(i)), v)

		# Overriding a key (key update)
		# TODO: Should this raise an error?
		for key in keys:
			key = "key_" + key
			self.backend.add(key, "OK")
			self.assertEqual(self.backend.get(key), "OK")
			for v in values:
				# FIXME: Should raise an exception because the key is already defined
				self.backend.add(key, v)
				self.assertEqual(self.backend.get(key), v)

		#Test keys --invalid
		invalid_entry = [None,0,1,-1,0.5,list(),dict(),tuple(),(1,"a",3.5),[1,2,3,4],{"a":1,2:"b"},datetime.date(2013,2,6)]

		#invalid key [assuming not accepted]
		for ktype in invalid_entry:
			self.backend.add(ktype,"Value")
			self.assertEqual(5,self.backend.count())

		#invalid value [assuming accepted]
		for i,vtype in enumerate(keys):
			self.backend.add(repr(i),vtype)
			self.assertEqual(6+i,self.backend.count())
			
		#stress test TODO

	def testUpdate(self):
		self.backend.add("key1","value1")
		self.backend.add("key2","value2")
		self.backend.add("key3","value3")
		self.backend.add("key4","value4")
		count = 4

		#simple
		self.assertMultiLineEqual("value1",self.backend.get("key1"))
		self.backend.update("key1","new_value")
		self.assertMultiLineEqual("new_value",self.backend.get("key1"))

		#update undefined entry
		self.backend.update("key5","value5")
		count+=1

		#update removed entry
		self.backend.remove("key1")
		self.backend.update("key1","value4")
		self.assertEqual(count,self.backend.count())
		self.assertMultiLineEqual(self.backend.get("key1"),"value4")
		
		#invalid key [IndexError]
		invalid_entry = [None,0,1,-1,0.5,list(),dict(),tuple(),(1,"a",3.5),[1,2,3,4],{"a":1,2:"b"},datetime.date(2013,2,6)]
		
		for tkey in invalid_entry:
			self.assertRaises(Exception,self.backend.update,tkey,"value")
		
	def testRemove(self):
		self.backend.add("key1","value1")
		self.backend.add("key2","value2")
		self.backend.add("key3","value3")
		self.backend.add("key4","value4")
		
		#simple
		self.assertEqual(self.backend.count(),4)
		self.backend.remove("key1")
		self.assertEqual(self.backend.count(),3)
		self.assertIsNone(self.backend.get("key1"))
		
		#remove undefined entry [IndexError]
		self.assertRaises(Exception,self.backend.remove,"key1")
		
		#invalid keys [IndexError]
		invalid_entry = [None,0,1,-1,0.5,list(),dict(),tuple(),(1,"a",3.5),[1,2,3,4],{"a":1,2:"b"},datetime.date(2013,2,6)]
		for tkey in invalid_entry:
			self.assertRaises(Exception,self.backend.remove,tkey)
			
		#stress test TODO
		
	def testSync(self):
		# Make sure sync is implemented
		# TODO: Needs to be elaborated
		self.backend.sync()
		
	def testHas(self):
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
		#undefined
		for key in self.KEYS_VALID:
			self.assertIsNone(self.backend.get(key))
		
		#existing
		for key in self.KEYS_VALID:
			for v in self.VALUES_VALID:
				self.backend.add(key+repr(v),v)
				self.assertMultiLineEqual(repr(v), self.backend.get(key+repr(v)))
		
		#removed
		for key in self.KEYS_VALID:
			for v in self.VALUES_VALID:
				self.backend.remove(k+repr(v))
				self.assertIsNone(self.backend.get(key+repr(v)))
		
	def testKeys(self):
		#empty database
		klist=[]
		for k in self.backend.keys():
			klist+=[k]
		self.assertListEqual(klist,[])
		
		#keys
		for k in self.KEYS_VALID:
			self.backend.add(k,"value")

		klist=[]
		for k in self.backend.keys():
			klist+=[k]
		self.assertListEqual(self.KEYS_VALID,klist)
		
		#removed
		for k in self.KEYS_VALID:
			self.remove(k)
		klist=[]
		for k in self.backend.keys():
			klist+=[k]
		self.assertListEqual(klist,[])
		
	def testClear(self):
		#clear empty database
		self.assertEqual(0,self.backend.count())
		self.backend.clear()
		self.assertEqual(0,self.backend.count())
		
		#clear database
		for k in self.KEYS_VALID:
			self.backend.add(k,"OK")
			
		self.assertNotEqual(0,self.backend.count())
		self.backend.clear()
		self.assertEqual(0,self.backend.count())
		
	def testList(self):
		#empty database
		values_list = []
		for item in self.backend.list():
			values_list += [item]
		self.assertListEqual([],values_list)
		
		for i,v in enumerate(self.VALUES_VALID):
			self.backend.add("key_"+repr(i),v)	
		values_list=[]
		for item in self.backend.list():
			values_list += [item]
		self.assertListEqual(values_list,self.VALUES_VALID)

		for i in range(len(self.VALUES_VALID)):
			self.remove("key_"+repr(i))
		values_list=[]
		for item in self.backend.list():
			values_list += [item]
		self.assertListEqual([],values_list)

		
	def testCount(self):
		#empty
		self.assertEqual(0,self.backend.count())
		
		#sample
		keys = ["key1","key2","key3","key4"]
		for k in keys:
			self.backend.add(k,"value")
		self.assertEqual(len(keys),self.backend.count())
		self.assertEqual(len(keys),self.backend.count())
		
		#remove
		self.backend.remove(keys[-1])
		self.backend.remove(keys[-2])
		self.assertEqual(len(keys)-2,self.backend.count())
		

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
		self.backend = self.backend
		
		#close
		self.backend.add("key","value")
		self.backend.close()
		self.assertRaises(Exception,self.backend.update,"key","new_value")
		
		#closed backend
		self.assertRaises(Exception,self.backend.close)

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
		if not (os.path.exists(os.getcwd()+"/test-dir")):
			os.mkdir(os.getcwd()+"/test-dir/")
		
	@classmethod
	def tearDownClass(cls):
		if (os.path.exists(os.getcwd()+"/test-dir")):
			shutil.rmtree(os.getcwd()+"/test-dir")
	
	def setUp(self):
		self.backend = self._createBackend()
		keys = self.backend.keys()
		for k in keys:
			self.backend.remove(k)

	def testGetFileName(self):
		self.backend = self.backend

		#undefined item
		self.assertIsNone(self.backend.getFileName("file"))
		
		#get file
		self.backend.add("file","data")
		self.assertIsNotNone(self.backend.getFileName("file"))
		self.assertIsNot("",self.backend.getFileName("file"))
		print self.backend.getFileName("file")
		
		#invalid filename
		self.backend.add("my./.file","data")

	def testKeyPathMapping(self):

		for k in KEYS_VALID:
			path = self_defaultKeyToPath(None,k)
			self.assertMultiLineEqual(k,self._defaultPathToKey(None,path))

	def testDefaultReadWrite(self):
		for i,v in enumerate(VALUES_VALID):
			self.writeFile(self.root+"key_"+repr(i),v)
			val = self.readFile(self.root+"key_"+repr(i))
			self.assertMultiLineEqual(val,repr(v))
if __name__ == "__main__":
	unittest.main()

# EOF
