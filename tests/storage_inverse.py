import shutil
import tempfile
import unittest

from storage import DirectoryBackend, JournalBackend, Types
from storage.index import Indexes, Indexing
from storage.objects import InverseRelation, ObjectStorage, StoredObject
from storage.query import StoredQuery



class InverseChild(StoredObject):
	COLLECTION = "inverse-child"
	PROPERTIES = dict(parentId=Types.STRING, title=Types.STRING)
	INDEX_BY = dict(parentId=Indexing.Value)


class InverseParent(StoredObject):
	COLLECTION = "inverse-parent"
	PROPERTIES = dict(name=Types.STRING)
	RELATIONS = lambda _: dict(children=InverseRelation(InverseChild, "parentId"))


class InverseOwner(StoredObject):
	COLLECTION = "inverse-owner"
	PROPERTIES = dict(name=Types.STRING)


class InverseOwnedChild(StoredObject):
	COLLECTION = "inverse-owned-child"
	OWNERSHIP = lambda: InverseOwner.Owns(required=True)
	PROPERTIES = dict(parentId=Types.STRING, title=Types.STRING)
	INDEX_BY = dict(parentId=Indexing.Value)


class InverseOwnedParent(StoredObject):
	COLLECTION = "inverse-owned-parent"
	OWNERSHIP = lambda: InverseOwner.Owns(required=True)
	PROPERTIES = dict(name=Types.STRING)
	RELATIONS = lambda _: dict(children=InverseRelation(InverseOwnedChild, "parentId"))


class InversePartitionChild(StoredObject):
	COLLECTION = "inverse-part-child"
	PROPERTIES = dict(parentId=Types.STRING, title=Types.STRING)
	INDEX_BY = dict(parentId=Indexing.Value)


class InverseRelationTest(unittest.TestCase):
	def setUp(self):
		InverseChild.INDEXES = []
		InverseChild.INDEX_FOR = {}
		InverseChild.STORAGE = None
		InverseParent.STORAGE = None
		self.path = tempfile.mkdtemp(prefix="storage-inverse-")
		self.objects = ObjectStorage(JournalBackend(DirectoryBackend(self.path))).use(
			InverseParent, InverseChild
		)
		self.indexes = Indexes(DirectoryBackend, self.path).use(
			InverseParent, InverseChild
		)

	def tearDown(self):
		self.objects.release()
		shutil.rmtree(self.path)

	def testListsChildrenByForeignKey(self):
		parent = InverseParent(name="p").save()
		other = InverseParent(name="q").save()
		child = InverseChild(parentId=parent.id, title="one").save()
		InverseChild(parentId=other.id, title="nope").save()
		self.assertEqual([child.id], [item.id for item in parent.children])
		self.assertEqual(1, len(parent.children))
		self.assertTrue(parent.children.has(child))

	def testMovesWhenForeignKeyChanges(self):
		parent = InverseParent(name="p").save()
		other = InverseParent(name="q").save()
		child = InverseChild(parentId=parent.id, title="one").save()
		child.parentId = other.id
		child.save()
		self.assertEqual([], list(parent.children))
		self.assertEqual([child.id], [item.id for item in other.children])

	def testDropsRemovedChildren(self):
		parent = InverseParent(name="p").save()
		child = InverseChild(parentId=parent.id, title="one").save()
		child.remove()
		self.assertEqual(0, len(parent.children))

	def testDoesNotPersistOnParent(self):
		parent = InverseParent(name="p").save()
		InverseChild(parentId=parent.id, title="one").save()
		exported = parent.export()
		self.assertNotIn("children", exported)
		raw = self.objects.backend.get(parent.getStorageKey())
		self.assertNotIn("children", raw or {})

	def testAddWritesForeignKey(self):
		parent = InverseParent(name="p").save()
		child = InverseChild(title="one").save()
		parent.children.add(child)
		self.assertEqual(parent.id, child.parentId)
		self.assertEqual([child.id], [item.id for item in parent.children])

	def testRemoveClearsForeignKey(self):
		parent = InverseParent(name="p").save()
		child = InverseChild(parentId=parent.id, title="one").save()
		parent.children.remove(child)
		self.assertIsNone(child.parentId)
		self.assertEqual([], list(parent.children))

	def testJournalNotifiesInverseParent(self):
		parent = InverseParent(name="p").save()
		seen = []
		self.objects.backend.subscribe(
			parent.getStorageKey(),
			lambda key, operation, entry: seen.append(entry),
		)
		child = InverseChild(parentId=parent.id, title="one").save()
		added = [
			entry["relations"]["children"]["added"]
			for entry in seen
			if entry.get("relations") and "children" in entry["relations"]
		]
		self.assertTrue(added)
		self.assertEqual(child.id, added[0][0]["id"])
		self.assertEqual("relation", seen[-1]["operation"])
		self.assertEqual(parent.getStorageKey(), seen[-1]["key"])
		self.assertIsNone(StoredQuery(InverseParent).eventFor(seen[-1], self.objects.backend))
		relation_entries = [
			entry
			for entry in self.objects.backend.persistence.getEntries(
				keys=parent.getStorageKey()
			)
			if entry.get("operation") == "relation"
		]
		self.assertEqual(1, len(relation_entries))
		child_entries = [
			entry
			for entry in self.objects.backend.persistence.getEntries(
				keys=child.getStorageKey()
			)
			if entry.get("operation") != "relation"
		]
		self.assertTrue(child_entries)
		self.assertNotEqual(relation_entries[0]["seq"], child_entries[-1]["seq"])

	def testAssignmentWritesForeignKey(self):
		parent = InverseParent(name="p").save()
		child = InverseChild(title="one").save()
		parent.children = [child]
		self.assertEqual(parent.id, child.parentId)
		self.assertEqual([child.id], [item.id for item in parent.children])

	def testUseDoesNotDuplicateIndexes(self):
		self.indexes.use(InverseChild)
		matching = [
			index for index, cls in self.indexes.indexes if cls is InverseChild
		]
		self.assertEqual(1, len(matching))
		self.assertIs(matching[0], InverseChild.IndexFor("parentId"))

	def testParentStorageKeyUsesPartitionWhenOwnerMissing(self):
		payload = {"partition": "ACC-1", "id": "C1", "type": "inverse-part-child"}
		key = self.objects.backend._parentStorageKey(
			InverseOwnedParent, "P1", payload
		)
		self.assertEqual(
			InverseOwnedParent.StorageKey("P1", partition="ACC-1"), key
		)
		payload = {"owner": "ACC-2", "id": "C1", "type": "inverse-owned-child"}
		key = self.objects.backend._parentStorageKey(
			InverseOwnedParent, "P1", payload
		)
		self.assertEqual(InverseOwnedParent.StorageKey("P1", owner="ACC-2"), key)
		key = self.objects.backend._parentStorageKey(
			InverseParent, "P1", {"partition": "ACC-1"}
		)
		self.assertEqual(InverseParent.StorageKey("P1"), key)

	def testWebExportIncludesChildren(self):
		parent = InverseParent(name="p").save()
		child = InverseChild(parentId=parent.id, title="one").save()
		exported = parent.export(target="web")
		self.assertEqual(child.id, exported["children"][0]["id"])
		self.assertNotIn("children", parent.export())
		self.assertEqual(
			child.id, parent.exportWith("children", target="web")["children"][0]["id"]
		)
		self.assertNotIn("children", parent.exportWith("children"))


if __name__ == "__main__":
	unittest.main()

# EOF
