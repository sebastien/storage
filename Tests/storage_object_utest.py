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
import storage.objects

# -----------------------------------------------------------------------------
#
# STORABLE
#
# -----------------------------------------------------------------------------

class StorableTest:

	def testRecognizes(self):
		pass

	def testImport(self):
		pass

	def testGet(self):
		pass

	def testUpdate(self):
		pass

	def testSave(self):
		pass

	def testExport(self):
		pass

	def testRemove(self):
		pass

	def testGetID(self):
		pass

	def testRevision(self):
		pass

	def getHistory(self):
		pass

	def testCommit(self):
		pass

	def testGetStorageKey(self):
		pass

# -----------------------------------------------------------------------------
#
# ABSTRACT OBJECT
#
# -----------------------------------------------------------------------------

class StoredObjectTest(StorableTest):

	def _createStorage(self):
		raise NotImplementedError

	def setUp(self):
		self.STORAGE = _createStorage()

	VALID_OID   = [0,1,128,65536,4294967296,5L]
	INVALID_OID = [
		-1,
		float("-inf")-1.2,0.0,1.0,3.14,float("inf"),float("NaN"),
		-5L,
		None,False,True,NotImplemented,Ellipsis,
		'a','Z','-2','0','1','!','&','*','\\',unichr(96),unichr(1200),
		str(),unicode(),tuple(),list(),dict(),
		"aZ","0","00","09","?!@*($","_a","oid"*256,"oid"*4096,
		(1,1,1),[1,1,1],{"a":1,"b":1,"c":1},
		((1),(1),(1)),[[1],[1],[1]],{"a":{},"b":{},"c":{}},
		([1],[1],[1]),[(1),(1),(1)],{"a":[1],"b":(1),"c":{"a":1}},
		("a",1,1.5),["b",2,2.5]
	]

	def testRecognizes(self):
		self.assertFalse(StoredObject.Recognizes("myData"))

	def testAddIndexes(self):
		#TODO: Define acceptance set
		ACCEPTED_INDEX=[1,2,3]
		REJECTED_INDEX=[2,4,5]

		for i in ACCEPTED_INDEX:
			StoredObject.addIndexes(i)
			self.assertIn(i,StoredObject.INDEXES)

		for i in REJECTED_INDEX:
			StoredObject.addIndexes(i)
			self.assertNotIn(i,StoredObject.INDEXES)

	def testGenerateOID(self):
		self.assertNotEqual(0,len(StoredObject.GenerateOID()))

	def testEnsure(self):
		oid = StoredObject.GenerateOID()
		self.assertMultiLineEqual(oid,StoredObject.Ensure(oid).oid)

	def testSet(self):
		pass

# -----------------------------------------------------------------------------
#
# ABSTRACT OBJECT STORAGE
#
# -----------------------------------------------------------------------------

class AbstractObjectStorageTest:

	VALID_OID   = [0,1,128,65536,4294967296,5L]
	INVALID_OID = [
		-1,
		float("-inf")-1.2,0.0,1.0,3.14,float("inf"),float("NaN"),
		-5L,
		None,False,True,NotImplemented,Ellipsis,
		'a','Z','-2','0','1','!','&','*','\\',unichr(96),unichr(1200),
		str(),unicode(),tuple(),list(),dict(),
		"aZ","0","00","09","?!@*($","_a","oid"*256,"oid"*4096,
		(1,1,1),[1,1,1],{"a":1,"b":1,"c":1},
		((1),(1),(1)),[[1],[1],[1]],{"a":{},"b":{},"c":{}},
		([1],[1],[1]),[(1),(1),(1)],{"a":[1],"b":(1),"c":{"a":1}},
		("a",1,1.5),["b",2,2.5]
	]

	def _createBackend(self):
		raise NotImplementedError

	def setUp(self):
		self.backend = _createBackend()
		self.storage  = storage.object.ObjectStorage(self.backend)
		self.object = StoredObject()

	def tearDown(self):
		raise NotImplementedError

	def testRegister(self):
		#register storedObject
		storedObject = self.object
		self.storage.register(storedObject)
		self.assertIn(storedObject, self._cache)

		#register an invalid object
		storedObject = datetime.timedelta()
		self.assertRaises(Exception,self.storage.register,storedObject)

	def testRestore(self):
		#restore unregistered object
		self.assertRaise(self.storage.restore(self.object.export()))

		#restore stored object
		self.storage.register(StoredObject)
		self.storage.restore(self.object.export())

		#restore invalid object
		self.storage.restore(datetime.timedelta())

	def testAdd(self):
		#add stored object
		self.storage.add(self.object)
		self.assertIn   (self.object,self.storage._cache)
		self.assertEqual(1,self.storage.count())

		#add same object
		self.add        (self.object)
		self.assertEqual(1,self.storage.count())

		#add invalid object
		self.assertRaise(Exception,self.storage.add, datetime.timedelta())

	def testCreate(self):
		pass

	def testUpdate(self):
		pass

	def testGet(self):
		#get stored object
		self.storage.register(storedObject)
		self.storage.add     (self.object)
		self.storage.get     (self.object.getStorageKey())

		#get undefined key
		self.assertIsNone    (self.storage.get("myKey"))

if __name__ == '__main__':
	unittest.main()



# EOF