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


class PrefixedUser(StoredObject):
	ID_PREFIX = "USER"
	PROPERTIES = dict(value=Types.STRING)


class PrefixedAttachment(StoredRaw):
	ID_PREFIX = "FILE"


class User(StoredObject):
	PROPERTIES = dict(name=Types.STRING)


class Project(StoredObject):
	PROPERTIES = dict(name=Types.STRING)


class OwnedTask(StoredObject):
	OWNERSHIP = lambda: Project.Owns(required=True, cascade=False)
	PROPERTIES = dict(name=Types.STRING)


class OwnedComment(StoredObject):
	OWNERSHIP = lambda: Project.Owns(required=True, cascade=True)
	PROPERTIES = dict(body=Types.STRING)


class Message(StoredObject):
	RELATIONS = lambda _: dict(
		replyTo=[Message], references=[Message], attachments=[Attachment]
	)


if __name__ == "__main__":
	unittest.main()

# EOF
