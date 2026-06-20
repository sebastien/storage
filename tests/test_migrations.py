# ----------------------------------------------------------------------------
# Project   : FFCTN/Storage
# ----------------------------------------------------------------------------

import os
import shutil
import tempfile
import textwrap
import unittest

from storage import MemoryBackend, MigrationOperator
from storage.migrations import MIGRATIONS_ENV_VAR, MIGRATIONS_METADATA_KEY


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

	def tearDown(self):
		if self.previousPath is None:
			os.environ.pop(MIGRATIONS_ENV_VAR, None)
		else:
			os.environ[MIGRATIONS_ENV_VAR] = self.previousPath
		shutil.rmtree(self.tempdir)

	def writeMigration(self, name, body):
		with open(os.path.join(self.migrationsPath, name), "wt") as f:
			f.write(textwrap.dedent(body).lstrip())

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


if __name__ == "__main__":
	unittest.main()


# EOF
