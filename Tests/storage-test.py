
__doc__ = """
This test suite covers the 
"""

# TODO: Test for chain reaction of restores on object imports (using mutual-reference)
# TODO: Test for export depth: an object set as property/relation should just be exported with (oid,type)
# TODO: Test the object.update preserves already existing properies

class Objects(self):

	def create( self ):
		pass

	def updateProperties( self ):
		pass

	def updateRelations( self ):
		pass

	def loadFromCache( self ):
		"""Loads an object that we know is already in the cache"""

	def loadFromStorage( self ):
		"""Loads an object that we know is NOT already in the cache"""

class Consistency(self):

	def serialization( self ):
		"""Serializes and de-serializes an object, making sure all the properties are the same."""

	def references( self ):
		"""Makes sures that deserializing an object will always give the same physical object"""
		# When the object is created (so we know it's in cache)
		# When the object already exists and is not in cache

	def parallelModifySeparateProperties( self ):
		"""Two separate threads modify the same object with separate properties"""

	def parallelModifySameProperties( self ):
		"""Two separate threads modify the same object with same properties"""

class Search(self):

	def modifyAndSearch( self ):
		"""Modify an already existing object and immediately search for it."""

class Performance(self):

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

# EOF
