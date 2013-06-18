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
from test_type import *
import storage
from storage.objects import *

# -----------------------------------------------------------------------------
#
# STORABLE
#
# -----------------------------------------------------------------------------

class StorableTest:

	VALID_OID   = LONG_POSITIVE + LONG_ZERO

	INVALID_OID = LONG_NEGATIVE
	INVALID_OID += INT
	#INVALID_OID += LONG
	INVALID_OID += FLOAT
	INVALID_OID += FLOAT_SPECIAL
	INVALID_OID += CHAR
	INVALID_OID += STRING
	INVALID_OID += TUPLE
	INVALID_OID += LIST
	INVALID_OID += DICT
	INVALID_OID += BOOL
	INVALID_OID += PY_CONST
	INVALID_OID += EXCEPTION
	INVALID_OID += CLASS
	INVALID_OID += GENERATORS
	INVALID_OID += LAMBDA

	def test_Recognizes( self ):
		# setup
		valid_data   = DICT
		
		invalid_data = []
		invalid_data += INT
		invalid_data += LONG
		invalid_data += FLOAT
		invalid_data += FLOAT_SPECIAL
		invalid_data += CHAR
		invalid_data += STRING
		invalid_data += TUPLE
		invalid_data += LIST
		#invalid_data += DICT
		invalid_data += BOOL
		invalid_data += PY_CONST
		invalid_data += EXCEPTION
		invalid_data += CLASS
		invalid_data += GENERATORS
		invalid_data += LAMBDA

		# undefined property
		for d in valid_data:
			StoredObject.PROPERTIES = d
			self.assertFalse(StoredObject.Recognizes({"undefined_key":"undefined_value"}))
			self.assertTrue(StoredObject.Recognizes(d))
		# invalid data
		for d in invalid_data:
			self.assertFalse(StoredObject.recognizes(d))

	def test_Import(self):
		pass

	def test_Get( self ):
		# undefined storage
		for oid in self.VALID_OID:
			self.assertRaises(Exception, Get, oid)
		for oid in self.INVALID_OID:
			self.assertRaises(Exception, Get, oid)
		_setUp()
		# empty storage
		for oid in self.VALID_OID:
			self.assertIsNone(StoredObject.Get(oid))
		objects = []
		# object instatiation
		for oid in self.VALID_OID:
			o = StoredObject(oid)
			objects.append(o)
			self.assertEqual(o.oid, StoredObject.Get(oid).oid)
		# object remove
		for oid in self.VALID_OID:
			o = StoredObject.Get(oid)
			o.remove()
			self.assertIsNone(StoredObject.Get(oid))
		# invalid oid
		for oid in self.INVALID_OID:
			self.assertRaises(Exception, StoredObject.Get, oid)
		_tearDown()

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
	
	def _setUp(self):
		StoredObject.STORAGE = _createStorage()
		StoredObject.STORAGE.register(StoredObject)

	def _tearDown(self):
		objs = StoredObject.All()
		for o in objs:
			o.remove()
		StoredObject.SKIP_EXTRA_PROPERTIES = False
		StoredObject.COLLECTION            = None
		StoredObject.STORAGE               = None
		StoredObject.PROPERTIES            = {}
		StoredObject.COMPUTED_PROPERTIES   = ()
		StoredObject.RELATIONS             = {}
		StoredObject.RESERVED              = ("type", "oid", "timestamp")
		StoredObject.INDEXES               = None

	@unittest.skip("Index ADD")
	def test_AddIndex( self ):
		pass

	@unittest.skip("Index REBUILD")
	def test_RebuildIndexes( self ):
		pass

	def test_GenerateOID( self ):
		oid_list = []
		for i in range (1000):
			oid = StoredObject.GeneratesOID()
			self.assertNotIn(oid, oid_list)
			oid_list.append(oid)

	@unittest.skip("Storage prefix")
	def test_StoragePrefix( self ):
		pass

	def test_All( self ):
		# unregistered storage
		self.assertRaises(Exception, StoredObject.All)
		self._setUp()
		# empty list
		self.assertListEqual([], StoredObject.All())
		# object instatiation
		count = 30
		for i in range(count):
			obj = StoredObject()
			self.assertEqual(i+1, len(StoredObject.All()))
			self.assertIn(obj, StoredObject.All())
		# objects remove
		objects_in_storage = StoredObject.All()
		self.assertEqual(count, objects_in_storage)
		for i in objects_in_storage:
			i.remove()
			self.assertNotIn(i, StoredObject.All())
			self.assertEqual(count - 1, StoredObject.All())
			count -= 1

		self._tearDown()

	def test_Keys( self ):
		# unregistered storage
		self.assertRaises(Exception, StoredObject.Keys)
		# get valid keys
		self._setUp()
		count = 30
		for i in range(count):
			obj = StoredObject()
			keys = StoredObject.Keys()
			self.assertEqual(i+1, len(keys))
			self.assertIn(obj.getStorageKey(), keys)
		# get keys after remove
		objects_in_storage = StoredObject.All()
		for i in objects_in_storage:
			i.remove()
			self.assertNotIn(i.getStorageKey(), StoredObject.Keys())
		# invalid prefix
		invalid_prefix = []
		invalid_prefix += INT
		invalid_prefix += LONG
		invalid_prefix += FLOAT
		invalid_prefix += FLOAT_SPECIAL
		invalid_prefix += CHAR
		invalid_prefix += STRING
		invalid_prefix += TUPLE
		invalid_prefix += LIST
		invalid_prefix += DICT
		invalid_prefix += BOOL
		invalid_prefix += PY_CONST
		invalid_prefix += EXCEPTION
		invalid_prefix += CLASS
		invalid_prefix += GENERATORS
		invalid_prefix += LAMBDA
		for prefix in invalid_prefix:
			self.assertRaises(Exception, StoredObject.Keys, prefix)
		self._tearDown()

	def test_Count( self ):
		# undefined storage
		self.assertRaises(Exception, StoredObject.Count)
		_setUp()
		count = 30
		objects = []
		for i in range(count):
			self.assertEqual(i, StoredObject.Count())
			objects.append(StoredObject())
			self.assertEqual(i+1, StoredObject.Count())
		# object remove
		for i in objects:
			self.assertEqual(count, StoredObject.Count())
			i.remove()
			self.assertEqual(count-1, StoredObject.Count())
			count -= 1
		_tearDown()

	def test_List( self ):
		# undefined storage
		self.assertRaises(Exception, StoredObject.List())
		_setUp()
		# empty list
		self.assertListEqual([],StoredObject.List())
		# object list
		count = 30
		objects = []
		for i in range(count):
			o = StoredObject()
			objects.append(o)
			self.assertIn(o, StoredObject.List())
			self.assertEqual(i+1, len(StoredObject.List()))
		self.assertEqual(5, len(StoredObject.List(5)))
		self.assertEqual(count-3, len(StoredObject.List(start=2)))
		# invalid constraints
		self.assertRaises(Exception, StoredObject.List, count*2)
		self.assertRaises(Exception, StoredObject.List, -count*2)
		self.assertRaises(Exception, StoredObject.List, 0)
		self.assertRaises(Exception, StoredObject.List, count,int(round(count/2)))
		self.assertRaises(Exception, StoredObject.List, 2,-count)
		self.assertRaises(Exception, StoredObject.List, 2, count-2, 2)
		# object remove
		for o in objects:
			o.remove()
			self.assertNotEqual(0, len(StoredObject.List()))
			self.assertNotIn(o, StoredObject.List())
		# invalid argument
		invalid_args = []
		invalid_args += FLOAT
		invalid_args += FLOAT_SPECIAL
		invalid_args += CHAR
		invalid_args += STRING
		invalid_args += TUPLE
		invalid_args += LIST
		invalid_args += DICT
		invalid_args += BOOL
		invalid_args += PY_CONST
		invalid_args += EXCEPTION
		invalid_args += CLASS
		invalid_args += GENERATORS
		invalid_args += LAMBDA
		for arg in invalid_args:
			self.assertRaises(Exception, StoredObject.List(count=arg))
			self.assertRaises(Exception, StoredObject.List(start=arg))
			self.assertRaises(Exception, StoredObject.List(end=arg))
		_tearDown()

	def test_Has( self ):
		# undefined storage
		for oid in self.VALID_OID:
			self.assertRaises(Exception, StoredObject.Has, oid)
		_setUp()
		objects = []
		for oid in self.VALID_OID:
			self.assertFalse(StoredObject.Has(oid))
			o = StoredObject(oid)
			self.assertTrue(StoredObject.Has(oid))
			o.remove()
			self.assertFalse(StoredObject.Has(oid))
		# invalid oid
		for oid in self.INVALID_OID:
			self.assertRaises(Exception, StoredObject.Has, oid)
		_tearDown()

	@unittest.skip("Ensure")
	def test_Ensure( self ):
		pass

	@unittest.skip("Storage Key")
	def test_StorageKey( self ):
		pass

	@unittest.skip("Storage Prefix")
	def test_StoragePrefix( self ):
		pass

	@unittest.skip("Import")
	def test_Import( self ):
		pass

	@unittest.skip("Export")
	def test_Export( self ):
		pass

	@unittest.skip("Generate Descriptors")
	def test__GenerateDescriptors( self ):
		pass

	def test___init__( self ):
		# undefined storage
		self.assertRaises(Exception, StoredObject())
		_setUp()
		# default
		for i in range(30)
			o = StoredObject()
			self.assertNotEqual(0, o.oid)
			self.assertNotIn(o.oid,[i.oid for i in objects])
			o.remove()
		for oid in self.VALID_OID:
			o = StoredObject(oid)
			self.assertEqual(oid,o.oid)
			o.remove()
		for oid in self.VALID_OID:
			o=StoredObject(propreties={"oid":oid})
			self.assertEqual(oid,o.oid)
			o.remove()
		# propreieties
		for d in DICT:
			o = StoredObject(propreties=d)
			self.assertDictEqual(o.propreties, d)
			o.remove()
		# restored
		# TODO: how to test ?
			o = StoredObject(restored=False)
			o.remove()
			o = StoredObject(restored=True)
			o.remove()
		# TODO: skipExtraProperties

		_tearDown()

	@unittest.skip("Post Initialization")
	def test_init( self ):
		pass

	def test_set( self ):
		# setup
		_setUp()
		o = StoredObject()
		for d in DICT
			StoredObject.PROPERTIES = d
			new_dict = {}
			for i in d.values():
				new_dict = repr(i)
			o.set(new_dict)
			self.assertDictEqual(new_dict,o.propreties)
		# invalid data
		invalid_data = []
		invalid_data += INT
		invalid_data += LONG
		invalid_data += FLOAT
		invalid_data += FLOAT_SPECIAL
		invalid_data += CHAR
		invalid_data += STRING
		invalid_data += TUPLE
		invalid_data += LIST
		# invalid_data += DICT
		invalid_data += SET
		invalid_data += BOOL
		invalid_data += PY_CONST
		invalid_data += EXCEPTION
		invalid_data += CLASS
		invalid_data += GENERATORS
		invalid_data += LAMBDA
		self.assertRaises(Exception, o.set, invalid_data)
		_tearDown()

	def test_update( self ):
		_setUp()
		o = StoredObject()
		for d in DICT
			StoredObject.PROPERTIES = d
			new_dict = {}
			for i in d.values():
				new_dict = repr(i)
			o.update(new_dict)
			self.assertDictEqual(new_dict,o.propreties)
		#invalid data
		invalid_data = []
		invalid_data += INT
		invalid_data += LONG
		invalid_data += FLOAT
		invalid_data += FLOAT_SPECIAL
		invalid_data += CHAR
		invalid_data += STRING
		invalid_data += TUPLE
		invalid_data += LIST
		#invalid_data += DICT
		invalid_data += SET
		invalid_data += BOOL
		invalid_data += PY_CONST
		invalid_data += EXCEPTION
		invalid_data += CLASS
		invalid_data += GENERATORS
		invalid_data += LAMBDA
		self.assertRaises(Exception, o.update, invalid_data)
		_tearDown()

	def test_setProperty( self ):
		pass

	def test_setRelation( self ):
		pass

	def test_getProperty( self ):
		pass

	def test_getRelation( self ):
		pass

	def test_getID( self ):
		pass

	def test_getStorageKey( self ):
		pass

	def test_setStorage( self ):
		pass

	def test_getCollection( self ):
		pass

	def test_remove( self ):
		pass

	def test_save( self ):
		pass

	def test_onStore( self ):
		pass

	def test_onRestore( self ):
		pass

	def test_onRemove( self ):
		pass

	def test___getstate__( self ):
		pass

	def test___setstate__( self ):
		pass

	def test_export( self ):
		pass

	def test_asJSON( self ):
		pass

	def test___repr__( self ):
		pass

# -----------------------------------------------------------------------------
#
# ABSTRACT OBJECT STORAGE
#
# -----------------------------------------------------------------------------

class AbstractObjectStorageTest:

	def _createBackend(self):
		raise NotImplementedError

	def setUp(self):
		self.backend = _createBackend()
		self.storage  = storage.object.ObjectStorage(self.backend)
		self.object = StoredObject()

	def tearDown(self):
		raise NotImplementedError

	def testRegister(self):
		# register storedObject
		storedObject = self.object
		self.storage.register(storedObject)
		self.assertIn(storedObject, self._cache)
		# register an invalid object
		storedObject = datetime.timedelta()
		self.assertRaises(Exception,self.storage.register,storedObject)

	def testRestore(self):
		# restore unregistered object
		self.assertRaises(self.storage.restore(self.object.export()))
		# restore stored object
		self.storage.register(StoredObject)
		self.storage.restore(self.object.export())
		# restore invalid object
		self.storage.restore(datetime.timedelta())

	def testAdd(self):
		# add stored object
		self.storage.add(self.object)
		self.assertIn   (self.object, self.storage._cache)
		self.assertEqual(1, self.storage.count())
		# add same object
		self.add        (self.object)
		self.assertEqual(1, self.storage.count())
		# add invalid object
		self.assertRaises(Exception,self.storage.add, datetime.timedelta())

	def testCreate(self):
		pass

	def testUpdate(self):
		pass

	def testGet(self):
		# get stored object
		self.storage.register(storedObject)
		self.storage.add     (self.object)
		self.storage.get     (self.object.getStorageKey())
		# get undefined key
		self.assertIsNone    (self.storage.get("myKey"))

if __name__ == '__main__':
	unittest.main()



# EOF