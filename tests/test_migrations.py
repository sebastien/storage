# ----------------------------------------------------------------------------
# Project   : FFCTN/Storage
# ----------------------------------------------------------------------------

import os
import shutil
import tempfile
import textwrap
import unittest

from storage import MemoryBackend, MigrationOperator, Types
from storage.core import Storable, getCanonicalName
from storage.migrations import (
	MIGRATIONS_ENV_VAR,
	MIGRATIONS_METADATA_KEY,
	MIGRATIONS_PROGRESS_METADATA_KEY,
)
from storage.objects import ObjectStorage, StoredObject


INTERRUPT_ONCE = {"enabled": False}


def makeStoredObjectClass(name, **attributes):
	attributes.setdefault("__module__", __name__)
	attributes.setdefault("COLLECTION", name)
	return type(name, (StoredObject,), attributes)


class MigrationTarget:
	def __init__(self, backend):
		self.backend = backend
		self.events = []

	def record(self, value):
		self.events.append(value)


class MigrationsTest(unittest.TestCase):
	def setUp(self):
		self.tempdir = tempfile.mkdtemp(prefix="storage-migrations-")
		self.migrationsPath = os.path.join(self.tempdir, "migrations")
		os.makedirs(self.migrationsPath)
		self.backend = MemoryBackend()
		self.target = MigrationTarget(self.backend)
		self.previousPath = os.environ.get(MIGRATIONS_ENV_VAR)
		os.environ[MIGRATIONS_ENV_VAR] = self.migrationsPath
		self.classes = []

	def tearDown(self):
		INTERRUPT_ONCE["enabled"] = False
		for storedObjectClass in self.classes:
			storedObjectClass.STORAGE = None
			Storable.DECLARED_CLASSES.pop(getCanonicalName(storedObjectClass), None)
		if self.previousPath is None:
			os.environ.pop(MIGRATIONS_ENV_VAR, None)
		else:
			os.environ[MIGRATIONS_ENV_VAR] = self.previousPath
		shutil.rmtree(self.tempdir)

	def writeMigration(self, name, body):
		with open(os.path.join(self.migrationsPath, name), "wt") as f:
			f.write(textwrap.dedent(body).lstrip())

	def makeClass(self, name, **attributes):
		storedObjectClass = makeStoredObjectClass(name, **attributes)
		self.classes.append(storedObjectClass)
		return storedObjectClass

	def testListMigrationsSortsByNumericPrefix(self):
		self.writeMigration("100-c.py", "def apply(storage):\n\tpass\n")
		self.writeMigration("1-a.py", "def apply(storage):\n\tpass\n")
		self.writeMigration("02-b.py", "def apply(storage):\n\tpass\n")

		self.assertEqual(
			[_.filename for _ in MigrationOperator(None).list()],
			["1-a.py", "02-b.py", "100-c.py"],
		)

	def testApplyUsesEnvPathAndRecordsMetadata(self):
		self.writeMigration(
			"1-a.py",
			"""
			def apply(storage):
				storage.record("a")
			""",
		)
		self.writeMigration(
			"2-b.py",
			"""
			def apply(storage):
				storage.record("b")
			""",
		)

		applied = MigrationOperator(self.target).apply()

		self.assertEqual(["a", "b"], self.target.events)
		self.assertEqual(["1-a", "2-b"], sorted(applied.keys()))
		self.assertEqual(applied, MigrationOperator(self.target).applied())
		self.assertIn(MIGRATIONS_METADATA_KEY, self.backend.getMetadata())

	def testApplySkipsAlreadyApplied(self):
		self.writeMigration(
			"1-a.py",
			"""
			def apply(storage):
				storage.record("a")
			""",
		)

		operator = MigrationOperator(self.target)
		operator.apply()
		operator.apply()

		self.assertEqual(["a"], self.target.events)

	def testPrepareFailsOnChecksumDrift(self):
		self.writeMigration(
			"1-a.py",
			"""
			def apply(storage):
				storage.record("a")
			""",
		)
		MigrationOperator(self.target).apply()
		self.writeMigration(
			"1-a.py",
			"""
			def apply(storage):
				storage.record("changed")
			""",
		)

		self.assertRaises(RuntimeError, MigrationOperator(self.target).prepare)

	def testFailedMigrationIsNotRecorded(self):
		self.writeMigration(
			"1-a.py",
			"""
			def apply(storage):
				storage.record("a")
			""",
		)
		self.writeMigration(
			"2-b.py",
			"""
			def apply(storage):
				raise RuntimeError("boom")
			""",
		)

		self.assertRaises(RuntimeError, MigrationOperator(self.target).apply)
		applied = MigrationOperator(self.target).applied()

		self.assertIn("1-a", applied)
		self.assertNotIn("2-b", applied)
		self.assertEqual(["a"], self.target.events)

	def testPendingReturnsOnlyUnappliedMigrations(self):
		self.writeMigration(
			"1-a.py",
			"""
			def apply(storage):
				storage.record("a")
			""",
		)
		self.writeMigration(
			"2-b.py",
			"""
			def apply(storage):
				storage.record("b")
			""",
		)
		MigrationOperator(self.target).apply()
		self.writeMigration(
			"3-c.py",
			"""
			def apply(storage):
				storage.record("c")
			""",
		)

		pending = MigrationOperator(self.target).pending()

		self.assertEqual(["3-c.py"], [_.filename for _ in pending])

	def testDeclarativeMigrationResumesFromCheckpoint(self):
		MigrationMember = self.makeClass(
			"MigrationMember",
			PROPERTIES=dict(name=Types.STRING, firstName=Types.STRING),
		)
		storage = ObjectStorage(self.backend, validateSchema=False).use(MigrationMember)
		MigrationMember(name="Alpha").save()
		MigrationMember(name="Beta").save()
		MigrationMember(name="Gamma").save()
		INTERRUPT_ONCE["enabled"] = True
		self.writeMigration(
			"1-copy_name.py",
			f"""
			from storage import migration
			from {__name__} import INTERRUPT_ONCE, makeStoredObjectClass

			MigrationMember = makeStoredObjectClass("MigrationMember")

			@migration()
			def apply(m):
				def copyName(member):
					if member.name == "Beta" and INTERRUPT_ONCE["enabled"]:
						INTERRUPT_ONCE["enabled"] = False
						raise RuntimeError("boom")
					member.firstName = member.firstName or member.name

				m.each(MigrationMember).run(copyName, operation="copy")
			""",
		)

		with self.assertRaises(RuntimeError):
			MigrationOperator(storage).apply()

		progress = self.backend.getMetadata(MIGRATIONS_PROGRESS_METADATA_KEY)
		self.assertEqual("MigrationMember.copy", progress["1-copy_name"]["step"])
		self.assertEqual(1, len([_ for _ in MigrationMember.All() if _.firstName]))

		MigrationOperator(storage).apply()

		self.assertEqual(
			["Alpha", "Beta", "Gamma"],
			[_.firstName for _ in MigrationMember.All(order=1)],
		)
		self.assertEqual({}, self.backend.getMetadata(MIGRATIONS_PROGRESS_METADATA_KEY))

	def testChangeOwnerMovesKeyAndRemovesLegacyKey(self):
		MigrationUser = self.makeClass("MigrationUser", PROPERTIES=dict(name=Types.STRING))
		LegacyThing = self.makeClass("MigrationThing", PROPERTIES=dict(name=Types.STRING))
		legacyStorage = ObjectStorage(self.backend, validateSchema=False).use(
			MigrationUser, LegacyThing
		)
		user = MigrationUser(name="Owner").save()
		thing = LegacyThing(name="Task").save()
		legacyKey = thing.getStorageKey()
		LegacyThing.STORAGE = None
		Storable.DECLARED_CLASSES.pop(getCanonicalName(LegacyThing), None)
		self.classes.remove(LegacyThing)
		OwnedThing = self.makeClass(
			"MigrationThing",
			PROPERTIES=dict(name=Types.STRING),
			OWNERSHIP=MigrationUser,
		)
		storage = ObjectStorage(self.backend, validateSchema=False).use(MigrationUser, OwnedThing)
		storedThing = storage.get(legacyKey)

		storage.changeOwner(storedThing, user)

		self.assertFalse(self.backend.has(legacyKey))
		self.assertTrue(self.backend.has(storedThing.getStorageKey()))


if __name__ == "__main__":
	unittest.main()


# EOF
