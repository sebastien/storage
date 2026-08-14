try:
	from storage_base import (
		Attachment,
		Message,
		ObjectStorage,
		RawStorage,
		DirectoryBackend,
		A,
		B,
		User,
		Project,
		OwnedTask,
		OwnedComment,
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
		User,
		Project,
		OwnedTask,
		OwnedComment,
		PrefixedUser,
		PrefixedAttachment,
	)
from storage.core import Identifier
from storage.migrations import Migration, MigrationContext
from storage.objects import PublicID, StoredObject
from storage import MemoryBackend, SQLiteBackend, Types
import unittest, os, shutil, sys, json, gc, tempfile, threading


class PublicTicket(StoredObject):
	PUBLIC_ID = PublicID.Partition
	PROPERTIES = dict(title=Types.STRING)


class LegacyTicket(StoredObject):
	PROPERTIES = dict(title=Types.STRING)


class StoredObjectTest(unittest.TestCase):
	def setUp(self):
		if hasattr(self, "objects"):
			self.objects.release()
		if hasattr(self, "raw"):
			self.raw.release()
		self.path = os.path.basename(__file__).split(".")[0]
		self.objects = ObjectStorage(DirectoryBackend(self.path)).use(
			Message, A, B, User, Project, OwnedTask, OwnedComment, PrefixedUser
		)
		self.raw = RawStorage(DirectoryBackend(self.path)).use(
			Attachment, PrefixedAttachment
		)
		self.assertIsNotNone(Message.STORAGE)
		self.assertIsNotNone(A.STORAGE)
		self.assertIsNotNone(B.STORAGE)
		self.assertIsNotNone(User.STORAGE)
		self.assertIsNotNone(Project.STORAGE)
		self.assertIsNotNone(OwnedTask.STORAGE)
		self.assertIsNotNone(OwnedComment.STORAGE)
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

	def testRelationSwap(self):
		m = Message()
		related = [Message(), Message(), Message()]
		for item in related:
			m.replyTo.append(item)

		m.replyTo.swap(0, 2)

		self.assertEqual(
			[related[2].id, related[1].id, related[0].id],
			[_.id for _ in m.replyTo],
		)

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
		with open(self.path + "/A/0/" + str(id) + ".json") as f:
			data = json.load(f)
		assert data["value"] == "Pouet!"
		data["value"] = "Changed!"
		with open(self.path + "/A/0/" + str(id) + ".json", "w") as f:
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

	def testOwnershipRequired(self):
		task = OwnedTask(name="Todo")
		self.assertRaises(ValueError, task.save)

	def testOwnershipTypeAndImmutability(self):
		project = Project(name="Storage").save()
		other = Project(name="Archive").save()
		task = OwnedTask(name="Todo", owner=project).save()
		self.assertIs(task.owner, project)
		self.assertEqual(task.id, task.getLocalID())
		self.assertEqual(task.partition, project.id)
		self.assertRaises(ValueError, task.setOwner, other)

	def testOwnedBy(self):
		project = Project(name="Storage").save()
		other = Project(name="Archive").save()
		task = OwnedTask(name="Todo", owner=project).save()
		OwnedTask(name="Elsewhere", owner=other).save()
		self.assertEqual([task.id], [_.id for _ in OwnedTask.OwnedBy(project)])
		self.assertEqual(task.id, OwnedTask.Get(task.id, owner=project).id)
		self.assertEqual(task.id, OwnedTask.Get(task.id, partition=project.id).id)

	def testOwnershipExportRestore(self):
		project = Project(name="Storage").save()
		task = OwnedTask(name="Todo", owner=project).save()
		exported = task.export()
		self.assertEqual(exported["id"], task.id)
		self.assertEqual(exported["owner"], project.id)
		self.assertEqual(exported["partition"], project.id)
		self.assertEqual(OwnedTask.Get(task.id, owner=project).owner.id, project.id)

	def testOwnershipCascadeDelete(self):
		project = Project(name="Storage").save()
		comment = OwnedComment(body="Hi", owner=project).save()
		project.remove()
		self.assertFalse(OwnedComment.Has(comment.id, partition=project.id))

	def testOwnershipPath(self):
		project = Project(name="Storage").save()
		task = OwnedTask(name="Todo", owner=project).save()
		owner_path = os.path.join(
			self.path,
			"OwnedTask",
			str(project.id),
			str(task.getLocalID()) + ".json",
		)
		self.assertTrue(os.path.exists(owner_path))


class PublicIDTest(unittest.TestCase):
	def setUp(self):
		self.directory = tempfile.TemporaryDirectory()
		self.path = os.path.join(self.directory.name, "public-ids")
		self.backend = SQLiteBackend(self.path)
		self.objects = ObjectStorage(self.backend).use(PublicTicket, LegacyTicket)

	def tearDown(self):
		self.objects.release()
		self.backend.close()
		self.directory.cleanup()

	def testPartitionScopedAllocationAndLookup(self):
		first = PublicTicket(title="First", partition="project-a").save()
		second = PublicTicket(title="Second", partition="project-a").save()
		other = PublicTicket(title="Other", partition="project-b").save()

		self.assertEqual(1, first.publicId)
		self.assertEqual(2, second.publicId)
		self.assertEqual(1, other.publicId)
		self.assertIs(first, PublicTicket.GetByPublicID(1, partition="project-a"))
		self.assertIs(other, PublicTicket.GetByPublicID(1, partition="project-b"))
		self.assertEqual(1, first.export()["publicId"])

	def testPublicIDCannotBeSuppliedOrChanged(self):
		with self.assertRaisesRegex(ValueError, "publicId is assigned"):
			PublicTicket(title="Invalid", partition="project-a", publicId=12)

		ticket = PublicTicket(title="Valid", partition="project-a").save()
		with self.assertRaisesRegex(ValueError, "Extra property 'publicId'"):
			ticket.update(dict(publicId=12))

	def testPublicIDPersistsAcrossStorageInstances(self):
		ticket = PublicTicket(title="Persisted", partition="project-a").save()
		self.objects.release()
		self.backend.close()
		self.backend = SQLiteBackend(self.path)
		self.objects = ObjectStorage(self.backend).use(PublicTicket, LegacyTicket)

		restored = PublicTicket.GetByPublicID(ticket.publicId, partition="project-a")
		self.assertIsNotNone(restored)
		self.assertEqual(ticket.id, restored.id)
		self.assertEqual(ticket.publicId, restored.publicId)

	def testMigrationBackfillsExistingObjects(self):
		legacy = LegacyTicket(title="Legacy", partition="project-a").save()
		LegacyTicket.PUBLIC_ID = PublicID.Partition
		try:
			context = MigrationContext(
				self.objects,
				self.backend,
				Migration("001", "public_ids", "", "", ""),
			)
			context.each(LegacyTicket).publicIDs()
			self.assertEqual(1, legacy.publicId)
			self.assertIs(legacy, LegacyTicket.GetByPublicID(1, partition="project-a"))
		finally:
			LegacyTicket.PUBLIC_ID = None

	def testSQLiteAllocationIsSafeAcrossConnections(self):
		count = 12
		results = []
		lock = threading.Lock()

		def allocate(index):
			backend = SQLiteBackend(self.path)
			try:
				publicID = backend.storePublicObject(
					f"public.{index}", "concurrent", lambda _: dict(index=index)
				)
				with lock:
					results.append(publicID)
			finally:
				backend.close()

		threads = [threading.Thread(target=allocate, args=(index,)) for index in range(count)]
		for thread in threads:
			thread.start()
		for thread in threads:
			thread.join()
		self.assertEqual(list(range(1, count + 1)), sorted(results))

	def testDeletionDoesNotReusePublicID(self):
		first = PublicTicket(title="First", partition="project-a").save()
		first.remove()
		second = PublicTicket(title="Second", partition="project-a").save()

		self.assertIsNone(PublicTicket.GetByPublicID(first.publicId, partition="project-a"))
		self.assertEqual(first.publicId + 1, second.publicId)

	def testPublicIDObjectsCannotChangePartition(self):
		ticket = PublicTicket(title="Ticket", partition="project-a").save()
		with self.assertRaisesRegex(ValueError, "Cannot change partition"):
			self.objects.changePartition(ticket, "project-b")

	def testClearRemovesMappingsAndPreservesPublicIDState(self):
		first = PublicTicket(title="First", partition="project-a").save()
		self.backend.clear()
		second = PublicTicket(title="Second", partition="project-a").save()

		self.assertEqual(2, second.publicId)
		self.assertIsNone(PublicTicket.GetByPublicID(first.publicId, partition="project-a"))

	def testFallbackAllocationDoesNotAdvanceOnFactoryFailure(self):
		backend = MemoryBackend()
		with self.assertRaisesRegex(RuntimeError, "fail"):
			backend.storePublicObject("object", "scope", lambda _: (_ for _ in ()).throw(RuntimeError("fail")))

		self.assertFalse(backend.has("object"))
		self.assertEqual({}, backend.getMetadata("__storage__.publicIDs", {}))


if __name__ == "__main__":
	unittest.main()

# EOF
