import unittest, os, shutil, sys, json, random
from   storage         import DirectoryBackend, DBMBackend, MemoryBackend, Types
from   storage.objects import StoredObject, ObjectStorage
from   storage.index   import Index, IndexStorage

__doc__ = """
A collection of tests that exercise the storage.index module
"""

def _len( v ):
	if type(v) in (tuple, list): return len(v)
	else: return len(tuple(v))

def noneIfEmpty(v):
	if v:
		if type(v) in (tuple, list): return v
		else: return noneIfEmpty(tuple(v))
	else:
		return None

class Value(StoredObject):
	PROPERTIES = dict(value=Types.INTEGER)

	@classmethod
	def ByValue( cls, storedObject ):
		return int(storedObject.value / 10)

	@classmethod
	def ByValueMany( cls, storedObject ):
		return (int(storedObject.value / 10), 0 - int(storedObject.value / 10))

class AutoIndexValue(Value):
	INDEXES = lambda _:[Value.ByValue]

class BasicIndexTest(unittest.TestCase):

	def setUp( self ):
		self.indexStorage  = IndexStorage(MemoryBackend(), MemoryBackend())
		self.index         = Index(self.indexStorage, Value.ByValue, lambda _:_)

	def testAddRemove( self ):
		index = self.index
		index.clear()
		v = Value(value=1)
		index.add(v)
		assert _len(index.get(0)) == 1
		index.remove(v)
		self.assertIsNone(noneIfEmpty(index.get(0)))

	def testAddUpdate( self ):
		index = self.index
		index.clear()
		v = Value(value=1)
		index.add(v)
		self.assertEqual(_len(index.get(0)), 1)
		index.update(v)
		self.assertEqual(_len(index.get(0)), 1)
		v.value = 10
		index.update(v)
		self.assertIsNone(noneIfEmpty(index.get(0)))
		self.assertEqual(_len(index.get(1)), 1)

	def testAddUpdateRemoveMany( self ):
		# We add everything
		values = []
		for i in range(100):
			v = Value(value=i) ; values.append(v)
			self.index.add(v)
		self.assertIsNone(noneIfEmpty(self.index.get(-1)))
		self.assertIsNone(noneIfEmpty(self.index.get(10)))
		for i in range(10):
			self.assertEqual(_len(self.index.get(i)), 10)
		# We update everything to 0, which should change the values
		for v in values:
			self.assertIn(v.getStorageKey(), self.index.get(Value.ByValue(v)))
			v.value = 0
			self.index.update(v)
			self.assertIn(v.getStorageKey(), self.index.get(0))
		self.assertEqual(_len(self.index.get(0)), 100)
		for i in range(1,10):
			self.assertIsNone(noneIfEmpty(self.index.get(i)))
		# We remove everything from the index
		for v in values:
			self.index.remove(v)
		for i in range(10):
			self.assertIsNone(noneIfEmpty(self.index.get(i)))

	def testMultipleValueExtractor( self ):
		"""Some extractors can return more than one value, in which case the
		index element will be indexed under many different keys."""
		index = Index(self.indexStorage, Value.ByValueMany, lambda _:_)
		v = Value(value=10)
		index.add(v)
		self.assertEqual(_len(index.get(1)),  1)
		self.assertEqual(_len(index.get(-1)), 1)
		index.remove(v)
		self.assertIsNone(noneIfEmpty(index.get(1)))
		self.assertIsNone(noneIfEmpty(index.get(-1)))

	def tearDown( self ):
		pass

class StoredObjectIndexTest(unittest.TestCase):
	"""Tests the integration of indexes into the stored objects"""

	def setUp( self ):
		self.indexStorage  = IndexStorage(MemoryBackend(), MemoryBackend())
		self.index         = Index(self.indexStorage, Value.ByValue, lambda _:_) 
		self.objectStorage = ObjectStorage(MemoryBackend()).use(Value)
		# We add the index to the stored type, so that whenever the object will
		# be stored, it will be indexed
		Value.AddIndex(self.index)

	def test( self ):
		values = []
		for i in range(100):
			values.append(Value(value=i))
		# We check that the index is not updated until the object is actually stored
		for i in range(10):
			self.assertIsNone(noneIfEmpty(self.index.get(i)))
		# We store the objects and check that the index worked
		for v in values: v.save()
		for i in range(10):
			self.assertEqual(_len(self.index.get(i)), 10)
		# We now test the updates
		for v in values:
			v.value = 0 ; v.save()
		self.assertEqual(_len(self.index.get(0)), 100)
		for i in range(1, 10):
			self.assertIsNone(noneIfEmpty(self.index.get(i)))
		# We now remove all the objects and expect the index to be cleared
		while values: values.pop().remove()
		for i in range(10):
			self.assertIsNone(noneIfEmpty(self.index.get(i)))

class DBMStorageTest(unittest.TestCase):
	"""Test the persistence of the index when using theDBM backend"""

	def setUp( self ):
		self.clean()
		self.indexStorage  = IndexStorage(DBMBackend("index-fwd"), DBMBackend("index-bwd"))
		self.index         = Index(self.indexStorage, Value.ByValue, lambda _:_)
		self.values        = []
		for i in range(100):
			v = Value(value=i)
			self.values.append(v)
			self.index.add(v)
		self.index.save()
		# We make sure that the index storage works
		for i in range(10):
			self.assertEqual(_len(self.index.get(i)), 10)

	def testAccess( self ):
		new_index_storage  = IndexStorage(DBMBackend("index-fwd"), DBMBackend("index-bwd"))
		new_index          = Index(new_index_storage, Value.ByValue, lambda _:_)
		# We make sure that we can access the index
		self.assertEqual(sorted(list(new_index.STORAGE.forwardBackend.keys())), range(10))
		for i in range(10):
			self.assertEqual(_len(new_index.get(i)), 10)

	def testUpdate( self ):
		for v in self.values:
			v.value = 0
			self.index.update(v)
		self.index.save()
		self.assertEqual(_len(self.index.get(0)), 100)
		for i in range(1,10): self.assertIsNone(noneIfEmpty(self.index.get(i)))
	
	def tearDown( self ):
		self.indexStorage.sync()
		self.indexStorage.forwardBackend.close()
		self.indexStorage.backwardBackend.close()
		self.clean()

	def testUpdateSync( self ):
		new_index_storage  = IndexStorage(DBMBackend("index-fwd"), DBMBackend("index-bwd"))
		new_index          = Index(new_index_storage, Value.ByValue, lambda _:_)
		# Tests the new index (same as testAccess)
		self.assertEqual(sorted(list(new_index.STORAGE.forwardBackend.keys())), range(10))
		for i in range(10): self.assertEqual(_len(new_index.get(i)), 10)
		# Updates the value
		for v in self.values:
			v.value = 0
			self.index.update(v)
		self.index.save()
		# We make sure that main index was updated
		self.assertEqual(_len(self.index.get(0)), 100)
		for i in range(1,10): self.assertIsNone(noneIfEmpty(self.index.get(i)))
		# We need to sync the storage, as there might have been changes
		new_index_storage.sync()
		# And we check that the new index was updated as well
		self.assertEqual(_len(new_index.get(0)), 100)
		for i in range(1,10): self.assertIsNone(noneIfEmpty(new_index.get(i)))

	def clean( self ):
		for f in ("index-fwd.db", "index-bwd.db"):
			if os.path.exists(f):
				os.unlink(f)
			assert not os.path.exists(f)
# 
# 
# 
if __name__ == "__main__":
	unittest.main()

# EOF
