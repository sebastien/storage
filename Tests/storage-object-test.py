import unittest, os, shutil, sys, json
from   storage         import DirectoryBackend, Types
from   storage.objects import StoredObject, ObjectStorage

class A(StoredObject):
	PROPERTIES = dict(value=Types.STRING)
class B(object):pass

class ObjectStorageCache(unittest.TestCase):

	def setUp( self ):
		self.prefix = os.path.basename(__file__).split(".")[0] + "/"
		self.s = ObjectStorage(DirectoryBackend(self.prefix)).use(A)

	def testCacheTransparency( self ):
		s = self.s
		a = A(value="Pouet!")
		b = B()
		assert sys.getrefcount(a) == sys.getrefcount(b) == 2
		oid         = a.oid
		storage_key = a.getStorageKey()
		s.add(a)
		repr_a = repr(a)
		# We make sure that the objects are the same
		assert a is A.Get(oid)
		# We delete a and make sure the objects are different (as repr stores the object memory id)
		assert A.STORAGE._cache.get(storage_key) is a,    "Object should be present in cache"
		assert sys.getrefcount(A.Get(oid)) == 2
		del a
		assert sys.getrefcount(A.Get(oid)) == 1
		assert A.STORAGE._cache.get(storage_key) is None, "Object should be cleared from cache"
		assert A.Get(oid).value == "Pouet!"
		# We change the physical file
		with file(self.prefix + "A/" + oid) as f:
			data = json.load(f)
		assert data["value"] == "Pouet!"
		data["value"]="Changed!"
		with file(self.prefix + "A/" + oid, "w") as f:
			json.dump(data, f)
		assert A.Get(oid).value == "Changed!"

	def tearDown( self ):
		shutil.rmtree(self.prefix)

if __name__ == "__main__":
	unittest.main()

# EOF
