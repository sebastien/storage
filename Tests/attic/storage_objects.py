import unittest, os, shutil, sys, json
from   storage         import MemoryBackend, DirectoryBackend
from   storage.objects import StoredObject, ObjectStorage
from   storage.raw     import StoredRaw,    RawStorage

class Attachment(StoredRaw):
	pass

class Message(StoredObject):

	RELATIONS = lambda _:dict(
		replyTo     = [Message],
		references  = [Message],
		attachments = [Attachment]
	)

class StoredObject(unittest.TestCase):

	def setUp( self ):
		self.path    = os.path.basename(__file__).split(".")[0]
		self.objects = ObjectStorage(DirectoryBackend(self.path)).use(Message)
		self.raw     = RawStorage   (DirectoryBackend(self.path)).use(Attachment)

	def tearDown( self ):
		shutil.rmtree(self.path)

	def testRawRelation( self ):
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




if __name__ == "__main__":
	unittest.main()

# EOF
