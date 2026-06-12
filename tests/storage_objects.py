try:
	from storage_base import (
		Attachment,
		Message,
		ObjectStorage,
		RawStorage,
		DirectoryBackend,
		A,
		B,
		PrefixedUser,
		PrefixedAttachment,
	)
except ModuleNotFoundError:
	from tests.storage_base import (
		Attachment,
		Message,
		ObjectStorage,
		RawStorage,
		DirectoryBackend,
		A,
		B,
		PrefixedUser,
		PrefixedAttachment,
	)
from storage.core import Identifier
import unittest, os, shutil, sys, json, gc


class StoredObjectTest(unittest.TestCase):
	def setUp(self):
		if hasattr(self, "objects"):
			self.objects.release()
		if hasattr(self, "raw"):
			self.raw.release()
		self.path = os.path.basename(__file__).split(".")[0]
		self.objects = ObjectStorage(DirectoryBackend(self.path)).use(
			Message, A, B, PrefixedUser
		)
		self.raw = RawStorage(DirectoryBackend(self.path)).use(
			Attachment, PrefixedAttachment
		)
		self.assertIsNotNone(Message.STORAGE)
		self.assertIsNotNone(A.STORAGE)
		self.assertIsNotNone(B.STORAGE)
		self.assertIsNotNone(PrefixedUser.STORAGE)
		self.assertIsNotNone(PrefixedAttachment.STORAGE)

	def tearDown(self):
		self.objects.release()
		self.raw.release()
		shutil.rmtree(self.path)

	def testRawRelation(self):
		"""Ensures that raw objects are rejected by object-only relations."""
		a = Attachment("pouetpouet")
		a.save()
		m = Message()
		self.assertEqual(len(m.attachments), 0)
		self.assertRaises(ValueError, m.attachments.append, a)

	def testCacheTransparency(self):
		"""Ensures that if you won't have two different physical instances
		(within the same process) for an object with the same id."""
		s = self.objects
		a = A(value="Pouet!")
		id = a.id
		storage_key = a.getStorageKey()
		s.add(a)
		# We make sure that the objects are the same
		assert a is A.Get(id)
		# The weak cache should hold the object only while a strong reference exists.
		assert A.STORAGE._cache.get(storage_key) is a, (
			"Object should be present in cache"
		)
		del a
		gc.collect()
		assert A.STORAGE._cache.get(storage_key) is None, (
			"Object should be cleared from cache"
		)
		assert A.Get(id).value == "Pouet!"
		# We change the physical file
		with open(self.path + "/A/" + str(id) + ".json") as f:
			data = json.load(f)
		assert data["value"] == "Pouet!"
		data["value"] = "Changed!"
		with open(self.path + "/A/" + str(id) + ".json", "w") as f:
			json.dump(data, f)

	def testIdentifierIDPrefix(self):
		plain_id = Identifier.ID()
		assert len(plain_id.split("-")) == 3

		id = Identifier.ID(prefix="USER")
		assert id.startswith("USER-")
		assert len(id.split("-")) == 4

	def testPrefixedObjectID(self):
		u = PrefixedUser(value="Pouet!")
		assert u.id.startswith("USER-")
		u.save()
		id = u.id
		assert PrefixedUser.Has(id)
		assert PrefixedUser.Get(id).value == "Pouet!"

	def testPrefixedRawID(self):
		a = PrefixedAttachment("pouetpouet")
		assert a.id.startswith("FILE-")
		a.save()
		id = a.id
		assert PrefixedAttachment.Has(id)
		assert list(PrefixedAttachment.Get(id).data()) == ["pouetpouet"]


if __name__ == "__main__":
	unittest.main()

# EOF
