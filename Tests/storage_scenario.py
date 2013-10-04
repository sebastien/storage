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

__doc__ = """
This test suite covers the
"""

# TODO: Test for chain reaction of restores on object imports (using mutual-reference)
# TODO: Test for export depth: an object set as property/relation should just be exported with (oid,type)
# TODO: Test the object.update preserves already existing properies

class ObjectsTest(unittest.TestCase):

	def testCreate( self ):
		pass

	def testUpdateProperties( self ):
		pass

	def testUpdateRelations( self ):
		pass

	def testLoadFromCache( self ):
		"""Loads an object that we know is already in the cache"""

	def testLoadFromStorage( self ):
		"""Loads an object that we know is NOT already in the cache"""

class ConsistencyTest(unittest.TestCase):

	def testSerialization( self ):
		"""Serializes and de-serializes an object, making sure all the properties are the same."""

	def testReferences( self ):
		"""Makes sures that deserializing an object will always give the same physical object"""
		# When the object is created (so we know it's in cache)
		# When the object already exists and is not in cache

	def testParallelModifySeparateProperties( self ):
		"""Two separate threads modify the same object with separate properties"""

	def testParallelModifySameProperties( self ):
		"""Two separate threads modify the same object with same properties"""

class Search(unittest.TestCase):

	def testModifyAndSearch( self ):
		"""Modify an already existing object and immediately search for it."""

class Performance(object):

	def __init__( self, volume=1000 ):
		self.volume = volume

	def create( self ):
		pass

	def update( self ):
		pass

	def search( self ):
		pass

	def delete( self ):
		pass

if __name__ == "__main__":
	unittest.main()

# EOF
