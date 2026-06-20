# ----------------------------------------------------------------------------
# Project   : FFCTN/Storage
# ----------------------------------------------------------------------------

import os
import shutil
import tempfile
import textwrap
import unittest

from storage import MemoryBackend, Types
from storage.core import Storable, getCanonicalName
from storage.migrations import MIGRATIONS_ENV_VAR
from storage.objects import ObjectStorage, StoredObject
from storage.schema import SCHEMA_METADATA_KEY, SchemaValidationError


def makeStoredObjectClass(name, **attributes):
	attributes.setdefault("__module__", __name__)
	attributes.setdefault("COLLECTION", name)
	return type(name, (StoredObject,), attributes)


class SchemaTest(unittest.TestCase):
	def setUp(self):
		self.tempdir = tempfile.mkdtemp(prefix="storage-schema-")
		self.migrationsPath = os.path.join(self.tempdir, "migrations")
		os.makedirs(self.migrationsPath)
		self.previousPath = os.environ.get(MIGRATIONS_ENV_VAR)
		os.environ[MIGRATIONS_ENV_VAR] = self.migrationsPath
		self.classes = []

	def tearDown(self):
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

	def replaceDeclaredClass(self, storedObjectClass):
		storedObjectClass.STORAGE = None
		Storable.DECLARED_CLASSES.pop(getCanonicalName(storedObjectClass), None)

	def className(self, storedObjectClass):
		return getCanonicalName(storedObjectClass)

	def testUseStoresInitialSchemaMetadata(self):
		SchemaUser = self.makeClass("SchemaUser", PROPERTIES=dict(name=Types.STRING))
		backend = MemoryBackend()

		ObjectStorage(backend).use(SchemaUser)

		metadata = backend.getMetadata(SCHEMA_METADATA_KEY)
		self.assertEqual(
			{"type": "string"},
			metadata["classes"][self.className(SchemaUser)]["properties"]["name"],
		)

	def testAdditivePropertyUpdatesSchemaWithoutMigration(self):
		SchemaUserV1 = self.makeClass("SchemaUser", PROPERTIES=dict(name=Types.STRING))
		SchemaUserV2 = self.makeClass(
			"SchemaUser", PROPERTIES=dict(name=Types.STRING, email=Types.EMAIL)
		)
		backend = MemoryBackend()
		ObjectStorage(backend).use(SchemaUserV1)
		self.replaceDeclaredClass(SchemaUserV1)

		ObjectStorage(backend).use(SchemaUserV2)

		metadata = backend.getMetadata(SCHEMA_METADATA_KEY)
		self.assertIn(
			"email",
			metadata["classes"][self.className(SchemaUserV2)]["properties"],
		)

	def testRemovedPropertyFailsWithoutMigration(self):
		SchemaUserFrom = self.makeClass("SchemaUser", PROPERTIES=dict(fullName=Types.STRING))
		SchemaUserTo = self.makeClass("SchemaUser", PROPERTIES=dict(name=Types.STRING))
		backend = MemoryBackend()
		ObjectStorage(backend).use(SchemaUserFrom)
		self.replaceDeclaredClass(SchemaUserFrom)

		self.assertRaises(SchemaValidationError, ObjectStorage(backend).use, SchemaUserTo)

	def testRenamePropertyMigrationCoversRemoval(self):
		SchemaUserFrom = self.makeClass("SchemaUser", PROPERTIES=dict(fullName=Types.STRING))
		SchemaUserTo = self.makeClass("SchemaUser", PROPERTIES=dict(name=Types.STRING))
		backend = MemoryBackend()
		ObjectStorage(backend).use(SchemaUserFrom)
		self.replaceDeclaredClass(SchemaUserFrom)
		self.writeMigration(
			"1-rename_name.py",
			f"""
			from storage import schemaChanges
			from {__name__} import makeStoredObjectClass

			SchemaUser = makeStoredObjectClass("SchemaUser")

			@schemaChanges.renameProperty(SchemaUser, "fullName", "name")
			def apply(storage):
				pass
			""",
		)

		ObjectStorage(backend).use(SchemaUserTo)

		metadata = backend.getMetadata(SCHEMA_METADATA_KEY)
		self.assertIn(
			"name",
			metadata["classes"][self.className(SchemaUserTo)]["properties"],
		)

	def testSplitPropertyMigrationAllowsAdditionalFields(self):
		SchemaUserFrom = self.makeClass("SchemaUser", PROPERTIES=dict(fullName=Types.STRING))
		SchemaUserTo = self.makeClass(
			"SchemaUser",
			PROPERTIES=dict(firstName=Types.STRING, lastName=Types.STRING),
		)
		backend = MemoryBackend()
		ObjectStorage(backend).use(SchemaUserFrom)
		self.replaceDeclaredClass(SchemaUserFrom)
		self.writeMigration(
			"1-split_name.py",
			f"""
			from storage import Types, schemaChanges
			from {__name__} import makeStoredObjectClass

			SchemaUser = makeStoredObjectClass("SchemaUser")

			@schemaChanges.splitProperty(
				SchemaUser,
				"fullName",
				{{"firstName": Types.STRING, "lastName": Types.STRING}},
			)
			def apply(storage):
				pass
			""",
		)

		ObjectStorage(backend).use(SchemaUserTo)

		metadata = backend.getMetadata(SCHEMA_METADATA_KEY)
		self.assertIn(
			"firstName",
			metadata["classes"][self.className(SchemaUserTo)]["properties"],
		)
		self.assertIn(
			"lastName",
			metadata["classes"][self.className(SchemaUserTo)]["properties"],
		)

	def testPartialMigrationCoverageStillFails(self):
		SchemaUserFrom = self.makeClass("SchemaUser", PROPERTIES=dict(fullName=Types.STRING))
		SchemaUserTo = self.makeClass(
			"SchemaUser",
			PROPERTIES=dict(firstName=Types.STRING, lastName=Types.STRING),
		)
		backend = MemoryBackend()
		ObjectStorage(backend).use(SchemaUserFrom)
		self.replaceDeclaredClass(SchemaUserFrom)
		self.writeMigration(
			"1-rename_name.py",
			f"""
			from storage import schemaChanges
			from {__name__} import makeStoredObjectClass

			SchemaUser = makeStoredObjectClass("SchemaUser")

			@schemaChanges.renameProperty(SchemaUser, "fullName", "firstName")
			def apply(storage):
				pass
			""",
		)

		with self.assertRaises(SchemaValidationError) as error:
			ObjectStorage(backend).use(SchemaUserTo)

		self.assertIn("lastName", str(error.exception))


if __name__ == "__main__":
	unittest.main()


# EOF
