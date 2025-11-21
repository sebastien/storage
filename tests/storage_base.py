# -----------------------------------------------------------------------------
# Project   : FFCTN/Storage
# -----------------------------------------------------------------------------
# Author    : Sebastien Pierre                            <sebastien@ffctn.com>
# License   : BSD License
# -----------------------------------------------------------------------------
# Creation  : 17-Jun-2013
# Last mod  : 03-Oct-2013
# -----------------------------------------------------------------------------

import unittest
from storage import MemoryBackend, DirectoryBackend, Types
from storage.objects import StoredObject, ObjectStorage
from storage.raw import StoredRaw, RawStorage


class A(StoredObject):
	PROPERTIES = dict(value=Types.STRING)


class B(StoredObject):
	pass


class Attachment(StoredRaw):
	pass


class Message(StoredObject):
	RELATIONS = lambda _: dict(
		replyTo=[Message], references=[Message], attachments=[Attachment]
	)


if __name__ == "__main__":
	unittest.main()

# EOF
