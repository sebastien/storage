from storage_base import (
	Attachment,
	Message,
	ObjectStorage,
	RawStorage,
	DirectoryBackend,
	A,
	B,
)
import unittest, os, shutil, sys, json


class StoredObjectTest(unittest.TestCase):
	def setUp(self):
		if hasattr(self, "objects"):
			self.objects.release()
		if hasattr(self, "raw"):
			self.raw.release()
		self.path = os.path.basename(__file__).split(".")[0]
		self.objects = ObjectStorage(DirectoryBackend(self.path)).use(Message, A, B)
		self.raw = RawStorage(DirectoryBackend(self.path)).use(Attachment)
		self.assertIsNotNone(Message.STORAGE)
		self.assertIsNotNone(A.STORAGE)
		self.assertIsNotNone(B.STORAGE)

	def tearDown(self):
		self.objects.release()
		self.raw.release()
		shutil.rmtree(self.path)

	def testRawRelation(self):
		"""Ensures that raw objects can be references in relations."""
		a = Attachment("pouetpouet")
		a.save()
		m = Message()
		self.assertEqual(len(m.attachments), 0)
		m.attachments.append(a)
		self.assertEqual(len(m.attachments), 1)
		self.assertEqual(m.attachments[0], a)
		m.save()
		mid = m.oid
		aid = a.oid
		a = None
		m = None
		self.assertTrue(Message.Has(mid))
		self.assertTrue(Attachment.Has(aid))
		m = Message.Get(mid)
		a = Attachment.Get(aid)
		self.assertEqual(len(m.attachments), 1)
		self.assertEqual(m.attachments[0], a)

	def testCacheTransparency(self):
		"""Ensures that if you won't have two different physical instances
		(within the same process) for an object with the same oid."""
		s = self.objects
		a = A(value="Pouet!")
		b = B()
		assert sys.getrefcount(a) == sys.getrefcount(b) == 2
		oid = a.oid
		storage_key = a.getStorageKey()
		s.add(a)
		repr_a = repr(a)
		# We make sure that the objects are the same
		assert a is A.Get(oid)
		# We delete a and make sure the objects are different (as repr stores the object memory id)
		assert A.STORAGE._cache.get(storage_key) is a, (
			"Object should be present in cache"
		)
		assert sys.getrefcount(A.Get(oid)) == 2
		del a
		assert sys.getrefcount(A.Get(oid)) == 1
		assert A.STORAGE._cache.get(storage_key) is None, (
			"Object should be cleared from cache"
		)
		assert A.Get(oid).value == "Pouet!"
		# We change the physical file
		with file(self.path + "/A/" + str(oid) + ".json") as f:
			data = json.load(f)
		assert data["value"] == "Pouet!"
		data["value"] = "Changed!"
		with file(self.path + "/A/" + str(oid) + ".json", "w") as f:
			json.dump(data, f)


if __name__ == "__main__":
	unittest.main()

# EOF
