# -----------------------------------------------------------------------------
# Project   : FFCTN/Storage                                       encoding=utf8
# -----------------------------------------------------------------------------
# Author    : Sebastien Pierre                            <sebastien@ffctn.com>
# License   : BSD License
# -----------------------------------------------------------------------------
# Creation  : 26-Apr-2012
# Last mod  : 17-Oct-2013
# -----------------------------------------------------------------------------

import os, sys, json, datetime, types, shutil, time, collections
import uuid, calendar, random

__version__ = "0.7.3"

# TODO: Rework the FILE/RAW interface
# TODO: Add worker to sync
# TODO: Add observer to observe changes to the filesystem in DirectoryBackend

# Should do:
# - Each object has a revision number 16bit is enough
# - Each object has a mtime
# - The object state is characterized by (rev, mtime) which allows to resolve conflicts
# - Optionally, objects can carry their history of modifications in the form of alist like
#   [(rev, mtime), {attribute:value}, [attribute]]
# - Objects when importing an object, we look at the revision number and return
#   the latest, unless it is explicit that we want to merge (ex: user saves,
#   but object has changed... so user should be prompted what to do)

# Cababilities:
# - Files: serve file objects for content
# - Filesystem: can give a path for a given data
# - ObjectsOpt: object-specific  optimizations
# - MetricsOpt: metrics-specific optimizaitons
# - RawOpt:     raw-specific     optimizations
# - IndexOpt:   index-specific   optimizations
# - Index:   can store indexes
# TODO: What about limits?

try:
	from retro.core import asJSON
except ImportError:
	from json       import dumps as asJSON

try:
	import reporter as logging
except ImportError:
	import logging

NOTHING = os

def asPrimitive( value, **options ):
	options.setdefault("depth", 1)
	if   value in (None, True, False):
		return value
	elif type(value) in (str, unicode, int, long, float):
		return value
	elif type(value) in (tuple, list):
		options["depth"] -= 1
		res = [asPrimitive(_,**options) for _ in value]
		options["depth"] += 1
		return res
	elif type(value) is dict:
		res = {}
		options["depth"] -= 1
		for key in value:
			res[key] = asPrimitive(value[key], **options)
		options["depth"] += 1
		return res
	elif hasattr(value, "__class__") and value.__class__.__name__ == "datetime":
		return asPrimitive(tuple(value.timetuple()), **options)
	elif hasattr(value, "__class__") and value.__class__.__name__ == "date":
		return asPrimitive(tuple(value.timetuple()), **options)
	elif hasattr(value, "__class__") and value.__class__.__name__ == "struct_time":
		return asPrimitive(tuple(value), **options)
	elif hasattr(value, "export"):
		res = value.export(**options)
		return res
	else:
		raise Exception("Type not supported: %s %s" % (type(value), value))

def restore( value ):
	"""Takes a primitive value and tries to restore a stored value instance
	out of it. If the value is not restorable, will return the value itself"""
	if   isinstance(value, Storable):
		return value
	elif type(value) is dict and "type" in value and "oid" in value:
		value_type  = value["type"]
		i           = value_type.rfind(".")
		assert i >= 0, "Object type should be `module.Class`, got {0}".format(value_type)
		module_name = value_type[:i]
		class_name  = value_type[i+1:]
		# NOTE: An alternative would be to look at declared classes
		if not sys.modules.get(module_name): __import__(module_name)
		module  = sys.modules.get(module_name)
		a_class = getattr(module, class_name)
		return a_class.Import(value)
	elif type(value) is dict:
		# We restore nested values in dicts
		for key in value:
			bound_value = value[key]
			restored_value = restore(bound_value)
			if bound_value is not restored_value:
				value[key] = restored_value
		return value
	elif type(value) in (tuple, list):
		# As well as in lists
		return map(restore, value)
	else:
		return value

def unJSON( text, useRestore=True ):
	"""Parses the given text as JSON, and if the result is an object, will try
	to identify whether the object is serialization of a metric, object or
	raw data, and restore it."""
	value = json.loads(text)
	if useRestore: return restore(value)
	else:       return value

def getCanonicalName( aClass ):
	"""Returns the canonical name for the class"""
	return aClass.__module__ + "." + aClass.__name__

BY = collections.namedtuple("PERIOD",("YEAR", "MONTH", "DAY", "HOUR", "MINUTE"))(10 ** 10, 10 ** 8, 10 ** 6, 10 ** 4, 10 ** 2)

def getTimestamp(date=None, period=None):
	"""Returns a number that is like: 'YYYYMMDDhhmmssuuuuuu' representing the
	current date in UTC timezone. This number preserves the ordering an allows
	to easily identify the date (or at least easier than a timestamp
	since EPOCH)."""
	# FIXME: Should return UTC datetime
	if date is None:
		date = datetime.datetime.utcnow()
	if isinstance(date, datetime.datetime):
		date = (
			date.year,
			date.month,
			date.day,
			date.hour,
			date.minute,
			date.second,
			date.microsecond
		)
	if type(date) in (tuple, list):
		if len(date) == 9:
			# This is a timetuple
			year, month, day, hour, mn, sec, _, _, _ = date
			msec = 0
		else:
			year, month, day, hour, mn, sec, msec = date
		date = (
			msec             + \
			sec     * 10**6  + \
			mn      * 10**8  + \
			hour    * 10**10 + \
			day     * 10**12 + \
			month   * 10**14 + \
			year    * 10**16
		)
	if   period is None:
		return date
	else:
		return int(date/period) * period

def parseTimestamp( t ):
	"""Returns the timestamp as an UTC time-tuple"""
	year  = t / 10**16 ; t -= year  * 10 ** 16
	month = t / 10**14 ; t -= month * 10 ** 14
	day   = t / 10**12 ; t -= day   * 10 ** 12
	hour  = t / 10**10 ; t -= hour  * 10 ** 10
	mn    = t / 10**8  ; t -= mn    * 10 ** 8
	sec   = t / 10**6  ; t -= mn    * 10 ** 6
	return (year, month, day, hour, mn, sec, 0,0,0)

# -----------------------------------------------------------------------------
#
# IDENTIFIER
#
# -----------------------------------------------------------------------------

class Identifier(object):

	NODE_ID    = 0
	TIME_BASE  = calendar.timegm(datetime.datetime(2000, 1, 1, 0, 0, 0, 0).utctimetuple())

	@classmethod
	def UUID( cls ):
		"""Returns a UUI4"""
		return str(uuid.uuid4())

	@classmethod
	def Stamp( cls, rand=3, nodes=4 ):
		"""Returns an 64-bit integer timestamp that is made like this.

		> TTTTTTTTTTTTTTT RRR NNNN
		> |               |   |
		> |               |   Node ID [0-9999]
		> |               Random [0-999]
		> Timestamp since TIMEBASE

		The `rand` and `nodes` parameters tell how many `R` and `N` there
		wil be in the numbers.
		"""
		base = long((time.time() - cls.TIME_BASE)) * (10 ** (nodes + rand))
		r    = random.randint(0,(10**rand)-1)      * (10 ** (nodes))
		return base + r + cls.NODE_ID

	@classmethod
	def Timestamp( cls, rand=3, nodes=4 ):
		"""A version of the `stamp` that's more readable but slightly longer.

		> YYYYMMDDHHMMSSMMMMMM RRR NNNN
		> |                    |   |
		> |                    |   Node ID [0-9999]
		> |                    Random [0-999]
		> Timestamp since TIMEBASE

		"""
		date = getTimestamp ()
		return (
			cls.NODE_ID                                               + \
			(random.randint(0,(10**rand)-1) * (10**nodes))            + \
			(date                           * (10 ** (nodes + rand)))
		)

# -----------------------------------------------------------------------------
#
# OPERATIONS
#
# -----------------------------------------------------------------------------

class Operations(object):
	ADD            = "="
	REMOVE         = "-"
	UPDATE         = "+"
	SAVE_RAW       = "+R"

# -----------------------------------------------------------------------------
#
# TYPES
#
# -----------------------------------------------------------------------------

class MapType(object):

	def __init__(self, kwargs ):
		self.kwargs = kwargs

	def __str__( self ):
		return "{%s}" % (",".join(map(lambda _:"%s=%s" % _, self.kwargs.items())))

	def __call__(self, **kwargs ):
		res = {}
		for key, value in kwargs.items():
			assert key in self.kwargs, "Key not part of type: %s not in %s" % (key, self)
			# TODO: Should also validate type
			res[key] = value
		return res

class Types(object):

	BOOL     = "bool"
	DATE     = "date"
	TIME     = "time"
	NUMBER   = "number"
	INTEGER  = "int"
	POSITIVE = "int>0"
	FLOAT    = "float"
	STRING   = "string"
	I18NSTRING = "string:i18n"
	EMAIL    = "string:email"
	URL      = "string:url"
	HTML     = "string:html"
	BINARY   = "bin"
	ANY      = "any"
	THIS     = "¤"

	@staticmethod
	def RANGE(start,end,type=INTEGER):
		return "%s:%s-%s" % (type, start, end)

	@staticmethod
	def LIST(_=ANY):
		return "[%s]" % (_)

	@staticmethod
	def TUPLE(*_):
		return "(%s)" % (",".join(map(str,_)))

	@staticmethod
	def ONE_OF(*_):
		return "(%s)" % ("|".join(map(str,_)))

	@staticmethod
	def MAP(**kwargs):
		return MapType(kwargs)

	@staticmethod
	def ENUM(*args):
		return "(%s)" % ("|".join(map(lambda _:"%s" % _, args)))

	@staticmethod
	def REFERENCE(clss):
		return clss.__name__

# -----------------------------------------------------------------------------
#
# STORABLE
#
# -----------------------------------------------------------------------------

# NOTE: We have to use new-style classes as we're using descriptors
class Storable(object):

	DECLARED_CLASSES = {}
	STORAGE          = None

	# FIXME: Should have the following attributes
	# created: UTC timestamp (int)
	# updated: UTC timestamp (int)
	# revision: int
	@classmethod
	def DeclareClass( cls, *classes ):
		"""This allows to declare classes and then resovle them. This is used
		by unJSON so that storable objects are properly restored."""
		for c in classes:
			name = getCanonicalName(c)
			assert name not in cls.DECLARED_CLASSES or cls.DECLARED_CLASSES[name] is c, "Conflict with class: %s" % (name)
			if name not in cls.DECLARED_CLASSES:
				cls.DECLARED_CLASSES[name] = c
		return cls

	@classmethod
	def Recognizes( self, data ):
		raise NotImplementedError

	@classmethod
	def Import( self, data ):
		raise NotImplementedError

	@classmethod
	def Get( self, sid ):
		raise NotImplementedError

	def __init__( self ):
		pass
		# self._revision = 0
		# self._history  = []
		# self._mtime    = None

	def update( self, data ):
		raise NotImplementedError

	def save( self ):
		raise NotImplementedError

	def export( self ):
		raise NotImplementedError

	def remove( self ):
		raise NotImplementedError

	def getID( self ):
		"""Returns the identifier of this object. The identifier will be
		unique amongst objects of the same class. You should use `getStorageKey`
		to have a globally unique ID"""
		raise NotImplementedError

	def getRevision( self ):
		raise NotImplementedError

	def getHistory( self ):
		pass

	def commit( self, items=None, names=None ):
		self._mtime     = getTimestamp()
		self._revision += 1
		self._history.append((self._mtime, self._revision, names, export(items)))

	def getStorageKey( self ):
		"""Returns the key with which this object will be stored in an underlying
		backend"""
		raise NotImplementedError

# -----------------------------------------------------------------------------
#
# BACKEND
#
# -----------------------------------------------------------------------------

# FIXME: Backend should support primitive data only, and do the serialization
# deserialization to string depending on the capacity of the backend

# FIXME: We should update the storage to backend support storing specific
# formats of data:
# - set()      -- sets the data as string
# - setRaw()   -- sets the data as raw file (accepts file-like interface)
# - setJSON()  -- sets the data as a JSON
# - get()      -- gets the data as string
# - getRaw()   -- would return a file-like interface
# - getJSON()  -- would return a deserialized object
# - findJSON() -- optimized for JSON
# - findRaw()  -- optimized for raw
# - find()     -- generic
# By default, all the specific functions would fallback to the generic
# version. The  alternative with hints:

# set( key, format=AS_JSON )
# set( key, format=AS_RAW|AS_STREAM )
# set( key, format=AS_STRING )

# and the storage would define the capacities

# backend.CAPACITIES = AS_JSON | AS_RAW | AS_STRING

# Or this could alternatively be done by using hints:
# Hint:
#  format=
#  size=

# FIXME: Maybe add a notification system so that storages can be notified
# of changes to specific keys.
class Backend(object):
	"""Backends are key-value stores, where you can ADD, UPDATE and REMOVE
	data given a specific key. Keys and values are combination of primitive
	values (ie. anything that you can convert to JSON).

	Backends also provide querying and searching operations to list/search
	for specific keys.

	The `_serialize` and `_deserialize` define how the keys and values get
	serialized, which is JSON by default (but you could use something else,
	like msgpack).

	The `sync` method allows to request an explicit (blocking) synchronization
	of the data. This should be used if you really want to make sure that
	the data is commited to the underlying storage."""

	HAS_READ    = True
	HAS_WRITE   = True
	HAS_STREAM  = False
	HAS_FILE    = False
	HAS_PUBLISH = True
	HAS_RAW     = False

	def __init__( self ):
		self._onPublish   = []
		self._subscribers = {}

	def onPublish( self, callback ):
		"""Adds a callback that will be invoked when the `publish` method
		is invoked."""
		if callback not in self._onPublish:
			self._onPublish.append(callback)
		return self

	def subscribe( self, key, callback ):
		"""Adds the given callback to subscribe to add/update/remove events
		on the given key."""
		callbacks = self._subscribers.setdefault(key, [])
		if callback not in callbacks: callbacks.append(callback)
		return self

	def unsubscribe( self, key, callback ):
		"""Unsubscribes the given callback from the given key."""
		callbacks = self._subscribers.setdefault(key, [])
		if callback in callbacks: callbacks.remove(callback)
		return self

	def notify( self, key, operation, data=None ):
		"""Notify the subscribers to the given key of the given operation
		and data."""
		callbacks = self._subscribers.get(key, [])
		for c in callbacks:
			try:
				c(key, operation, data)
			except Exception as e:
				logging.error("Backend.notify: Exception in callback {0}: {1}".format(c, e))
		return self

	def publish( self, operation, key, data=None ):
		"""The publish method allows to publish a change to the backend."""
		for callback in self._onPublish: callback(operation, key, data)

	def process( operation, key, data=None ):
		"""Processes the given operation (from `Operations`) with the given key
		and data. This is useful for processing a journal of transactions."""
		if self.operation is Operations.ADD:
			return self.add(key, data)
		elif self.operation is Operations.UPDATE:
			return self.udpate(key, data)
		elif self.operation is Operations.REMOVE:
			return self.remove(key)
		else:
			# The operation is not supported
			raise NotImplementedError

	def add( self, key, data ):
		"""Adds the given data to the storage."""
		raise Exception("Backend.add not implemented")

	def update( self, key, data ):
		"""Updates the given data in the storage."""
		raise Exception("Backend.update not implemented")

	def remove( self, key ):
		"""Removes the given data to the storage. In most cases, the
		metric won't be actually removed, but just invalidated."""
		raise Exception("Backend.remove not implemented")

	def clear( self ):
		"""Removes all the data from this backend."""
		raise Exception("Backend.clear not implemented")

	def sync( self ):
		"""Explicitly ask the back-end to synchronize. Depending on the
		back-end this might be a long or short, blocking or async
		operation."""
		raise Exception("Backend.sync not implemented")

	def has( self, key ):
		raise Exception("Backend.has not implemented")

	def get( self, key ):
		raise Exception("Backend.get not implemented")

	def list( self, key=None ):
		raise Exception("Backend.list not implemented")

	def count( self, key=None ):
		raise Exception("Backend.count not implemented")

	def keys( self, collection=None ):
		raise Exception("Backend.keys not implemented")

	def path( self, key):
		"""Returns the physical path of the file used to store
		the key, if any."""
		raise Exception("Backend.path not implemented")

	def stream( self, key, size=None ):
		"""Streams the data at the given key by chunks of given `size`"""
		raise Exception("Backend.stream not implemented")

	def hasRawData( self, key, ext=None ):
		"""Tells if the backend has raw data assocaited with the given key and extension"""
		raise NotImplementedError

	def saveRawData( self, key, data, ext=None ):
		"""Saved the raw data associated with the given key and extension"""
		raise NotImplementedError

	def streamRawData( self, key, size=None, ext=None ):
		"""Loads the raw data associated with the given key and extension. Returns
		a generator that will load the data."""
		raise NotImplementedError

	def getRawDataPath( self, key, ext=None ):
		"""Returns the physical path to the. This only works if bakcend has HAS_FILE"""
		raise NotImplementedError

	def _serialize( self, key=NOTHING, data=NOTHING ):
		if   key  is NOTHING:
			return asJSON(data)
		elif data is NOTHING:
			return asJSON(key)
		else:
			return asJSON(key), asJSON(data)

	def _deserialize( self, key=NOTHING, data=NOTHING ):
		# NOTE: We use restore=False as we want the backends to store
		# primitives, it's up to the parent storages to use restore to restore
		# data.
		if   key  is NOTHING:
			return unJSON(data, useRestore=False)
		elif data is NOTHING:
			return unJSON(key, useRestore=False)
		else:
			return unJSON(key, useRestore=False), unJSON(data, useRestore=False)

# -----------------------------------------------------------------------------
#
# MULTI BACKEND
#
# -----------------------------------------------------------------------------

class MultiBackend( Backend ):
	"""A backend that allows to multiplex different back-end together, which
	is especially useful for development (you can mix Journal, Memory and File
	for instance)."""

	def __init__( self, *backends ):
		self.backends       = backends
		self._readBackend   = None
		self._fileBackend   = None
		self._streamBackend = None
		for b in self.backends:
			if b.HAS_READ:
				self._readBackend = b
			if b.HAS_FILE:
				self._fileBackend = b
			if b.HAS_STREAM:
				self._streamBackend = b
			b.onPublish(lambda op, key, data=None, source=b:self._onBackendPublish(op, key, data, source))

	def _onBackendPublish( self, operation, key, data, source ):
		"""When a backend publishes an operation, the other backends will process
		the operation."""
		for b in self.backends:
			if b != source:
				b.process(operation, key, data)

	def add( self, key, data ):
		for backend in self.backends:
			backend.add(key, data )

	def update( self, key, data):
		for backend in self.backends:
			backend.update(key, data )

	def remove( self, key ):
		for backend in self.backends:
			backend.remove(key)

	def sync( self ):
		for backend in self.backends:
			backend.sync()

	def has( self, key ):
		return self._readBackend.has(key)

	def list( self, key ):
		return self._readBackend.list(key)

	def has( self, key ):
		return self._readBackend.has(key)
		raise Exception("Backend.has not implemented")

	def get( self, key ):
		return self._readBackend.get(key)

	def list( self, key=None ):
		return self._readBackend.list(key)

	def count( self, key=None ):
		return self._readBackend.count(key)

	def keys( self, collection=None ):
		return self._readBackend.keys(key)

	def path( self, key):
		"""Returns the physical path of the file used to store
		the key, if any."""
		return self._fileBackend.path(key)
		raise Exception("Backend.path not implemented")

	def stream( self, key, size=None ):
		"""Streams the data at the given key by chunks of given `size`"""
		return self._streamBackend.path(key, size)
		raise Exception("Backend.stream not implemented")

# -----------------------------------------------------------------------------
#
# MEMORY BACKEND
#
# -----------------------------------------------------------------------------

class MemoryBackend(Backend):
	"""A really simple backend that wraps Python's dictionary. Keys are converted
	to JSON while values are kept as-is."""

	def __init__( self ):
		Backend.__init__(self)
		self.values = {}

	def add( self, key, data ):
		key = self._serialize(key)
		self.values[key] = data

	def update( self, key, data ):
		key = self._serialize(key)
		self.values[key] = data

	def remove( self, key ):
		key = self._serialize(key)
		del self.values[key]

	def sync( self ):
		pass

	def has( self, key ):
		key = self._serialize(key)
		return self.values.has_key(key)

	def get( self, key ):
		key = self._serialize(key)
		return self.values.get(key)

	def list( self, key=None ):
		assert key is None, "Not implemented"
		return self.values.values()

	def count( self, key=None ):
		assert key is None, "Not implemented"
		return len(self.values)

	def keys( self, collection=None ):
		for key in self.values.keys():
			yield self._deserialize(key=key)

	def clear( self ):
		self.values = {}

	def export( self, **options ):
		return self.values

# -----------------------------------------------------------------------------
#
# DBM BACKEND
#
# -----------------------------------------------------------------------------

class DBMBackend(Backend):
	"""A really simple backend that wraps Python's DBM module. Key and value
	data are converted to JSON strings on the fly."""

	def __init__( self, path, autoSync=False ):
		Backend.__init__(self)
		# FIMXE: We should get away from the DBM backend as it seems to have
		# numerous problems -- I got a lot of "cannot write..". Maybe I'm
		# using it wrong?
		import dbm
		self._dbm     = dbm
		self.path     = path
		self.autoSync = autoSync
		self.values   = None
		self._open()

	def _open( self, mode="c"):
		try:
			self.values   = self._dbm.open(self.path, "c")
		except self._dbm.error, e:
			raise Exception("Cannot open DBM at path {}:{}".format(self.path, e))

	def _tryAdd( self, key, data ):
		# SEE: http://stackoverflow.com/questions/4995162/python-shelve-dbm-error/12167172#12167172
		# NOTE: I've encountered a lot of problems with DBM, it does not
		# seem to be very reliable for that kind of application
		retries = 5
		if key:
			while True:
				try:
					self.values[key] = data
					return True
				except self._dbm.error, e:
					time.sleep(0.100 * retries)
					if retries == 0:
						raise Exception("{0} in {1}.db with key={2} data={3}".format(e,self.path,key,data))
				retries -= 1

	def add( self, key, data ):
		key, data  = self._serialize(key, data)
		self._tryAdd( key, data)
		return self

	def update( self, key, data ):
		key, data  = self._serialize(key, data)
		self._tryAdd( key, data)
		return self

	def remove( self, key ):
		key = self._serialize(key=key)
		del self.values[key]

	def sync( self ):
		if not self.autoSync:
			# On some DBM implementations, we might need to close the file
			# to flush it... but this is not the default behaviour
			self.values.close()
			self._open()

	def has( self, key ):
		key = self._serialize(key=key)
		return self.values.has_key(key)

	def get( self, key ):
		key  = self._serialize(key=key)
		data =  self.values.get(key)
		if data is None: return data
		else: return self._deserialize(data=data)

	def keys( self, collection=None ):
		for key in self.values.keys():
			yield self._deserialize(key=key)

	def clear( self ):
		# TODO: Not very optimized
		for k in self.keys():
			self.remove(k)
		self.close()
		self._open()

	def list( self, key=None ):
		assert key is None, "Not implemented"
		for data in self.values.values():
			yield self._deserialize(data=data)

	def count( self, key=None ):
		assert key is None, "Not implemented"
		return len(self.values)

	def close( self ):
		self.values.close()

	def __del__( self ):
		self.close()

# -----------------------------------------------------------------------------
#
# DIRECTORY BACKEND
#
# -----------------------------------------------------------------------------

# TODO: Should add a backend that caches,
class DirectoryBackend(Backend):
	"""A backend that stores the values as files with the given `DATA_EXTENSION`
	the `keyToPath` and `pathToKey` functions take care of translating the
	keys to specific file system paths, allowing to write custom path-mapping
	schemes."""

	HAS_FILE            = True
	HAS_STREAM          = True
	HAS_RAW             = True
	DATA_EXTENSION      = ".json"
	RAW_EXTENSION       = ".raw"
	DEFAULT_STREAM_SIZE = 1024 * 100

	def __init__( self, root, pathToKey=None, keyToPath=None, writer=None, reader=None, extension=None):
		Backend.__init__(self)
		if not root.endswith("/"): root += "/"
		self.root         = root
		self.keyToPath    = keyToPath    or self._defaultKeyToPath
		self.pathToKey    = pathToKey    or self._defaultPathToKey
		self.writer       = writer       or self._defaultWriter
		self.reader       = reader       or self._defaultReader
		if extension != None: self.DATA_EXTENSION = extension
		parent_dir  = os.path.dirname(os.path.abspath(self.root))
		assert os.path.isdir(parent_dir), "DirectoryBacked root parent does not exists: %s" % (parent_dir)
		if not os.path.isdir(self.root):
			os.mkdir(self.root)

	# =========================================================================
	# BACKEND METHODS
	# =========================================================================

	def keys( self, prefix=None):
		"""Iterates through all (or the given subset) of keys in this storage."""
		assert not prefix or type(prefix) in (str,unicode) or len(prefix) == 1, "Multiple prefixes not supported yet: {0}".format(prefix)
		if prefix and type(prefix) in (tuple, list): prefix = prefix[0]
		ext_len = len(self.DATA_EXTENSION)
		if not prefix:
			prefix_path = self.root
		else:
			prefix_path = self.path(prefix or "")
			if ext_len: prefix_path = prefix_path[:-ext_len]
		for root, dirnames, filenames in os.walk(self.root):
			for f in filenames:
				if not f.endswith(self.DATA_EXTENSION): continue
				path = root + os.sep + f
				key  = self.pathToKey( self, path )
				if prefix and not key.startswith(prefix): continue
				yield key

	def count( self, prefix=None):
		"""Returns the numbers of keys that match the given prefix(es)"""
		return len(tuple(self.keys(prefix)))

	def add( self, key, data ):
		"""Adds the given data to the storage."""
		self.writer(self, Operations.ADD, self.path(key), self._serialize(data=data))

	def update( self, key, data ):
		"""Updates the given data to the storage."""
		self.writer(self, Operations.UPDATE, self.path(key), self._serialize(data=data))

	def get( self, key ):
		"""Gets the value associated with the given key in the storage."""
		data = self.reader(self, self.path(key=key))
		return self._deserialize(data=data) if data is not None else None

	def has( self, key ):
		return os.path.exists(self.path(key))

	def remove( self, key ):
		"""Removes the given value from the storage. This will remove the
		given file and remove the parent directory if it's empty."""
		# FIXME: This works for objects and raw, not so much for metrics
		path = self.keyToPath(self, key)
		if os.path.exists(path):
			os.unlink(path)
		parent = os.path.dirname(path)
		if parent != self.root:
			if os.path.exists(parent):
				if not os.listdir(parent): os.rmdir(parent)
		return self

	def sync( self ):
		"""This backend sync at each operation, so if you want to
		buffer operation, use a cached backend."""

	def path( self, key, ext=None):
		return self.keyToPath(self, key, ext)

	def stream( self, key, size=None ):
		# FIXME: Hope this does not leak
		with file(self.path(key),"rb") as f:
			while True:
				d = f.read(size or self.DEFAULT_STREAM_SIZE)
				if d: yield d
				else: break

	# FIXME: Not sure if this should be merges as get/set/stream/path
	def hasRawData( self, key, ext=RAW_EXTENSION ):
		return os.path.exists(self.path(key, ext=ext))

	def saveRawData( self, key, data, ext=RAW_EXTENSION ):
		self.writer(self, Operations.SAVE_RAW, self.path(key, ext=ext), data)

	def loadRawData( self, key, data, ext=RAW_EXTENSION ):
		return self.reader(self, self.path(key=key, ext=ext))

	def streamRawData( self, key, size=None, ext=RAW_EXTENSION ):
		# FIXME: Hope this does not leak
		with file(self.path(key, ext=ext),"rb") as f:
			while True:
				d = f.read(size or self.DEFAULT_STREAM_SIZE)
				if d: yield d
				else: break

	def getRawDataPath( self, key, ext=RAW_EXTENSION ):
		return self.path(key, ext=ext)

	def queryMetrics( self, name=None, timestamp=None ):
		return []

	def listMetrics( self ):
		"""Lists the metrics available in this backend"""
		return []

	def _serialize( self, key=NOTHING, data=NOTHING ):
		"""Serializing the key means converting the key to a path."""
		if key is NOTHING:
			return Backend._serialize(self, data=data)
		elif data is NOTHING:
			raise Exception("Serialize key should not be used, use `path()` instead.")
		else:
			raise Exception("Serialize key should not be used, use `path()` instead.")

	# =========================================================================
	# FILE I/O
	# =========================================================================

	def appendFile( self, path, data ):
		handle = self._getWriteFileHandle(path, mode="ab")
		handle.write(data)
		self._closeFileHandle(handle)
		return True

	def writeFile( self, path, data ):
		handle = self._getWriteFileHandle(path, mode="wb")
		if type(data) is types.FileType:
			try:
				shutil.copyfileobj(data, handle)
				self._closeFileHandle(handle)
			except Exception, e:
				self._closeFileHandle(handle)
				os.unlink(path)
				raise e
			return True
		elif data is None:
			# NOTE: In case we're given None as data, we don't create the file
			return True
		else:
			try:
				handle.write(data)
				self._closeFileHandle(handle)
			except Exception, e:
				self._closeFileHandle(handle)
				os.unlink(path)
				raise e
		return True

	def readFile( self, path ):
		handle = self._getReadFileHandle(path, mode="rb")
		if handle:
			data = handle.read()
			self._closeFileHandle(handle)
			return data
		else:
			return None

	# =========================================================================
	# INTERNALS (FILE MANIPULATION)
	# =========================================================================

	def _defaultKeyToPath( self, backend, key, ext=None ):
		"""Converts the given key to the given path."""
		return self.root + key.replace(".", "/") + (ext or self.DATA_EXTENSION)

	def _defaultPathToKey( self, backend, path, ext=None ):
		res = path.replace("/",".")
		ext = ext or self.DATA_EXTENSION
		if ext:
			return res[len(self.root):-len(ext)]
		else:
			return res[len(self.root):]

	def _defaultWriter( self, backend, operation, path, data ):
		"""Writes the given operation on the storable with the given key and data"""
		return self.writeFile(path, data)

	def _defaultReader( self, backend, path ):
		"""Returns the value that is stored in the given backend at the given
		key."""
		return self.readFile(path)

	def _getWriteFileHandle( self, path, mode="ab" ):
		parent = os.path.dirname(path)
		if parent and not os.path.exists(parent): os.makedirs(parent)
		return file(path, mode)

	def _getReadFileHandle( self, path, mode="rb" ):
		if os.path.exists(path):
			return file(path, mode)
		else:
			return None

	def _closeFileHandle( self, handle ):
		handle.close()

# EOF - vim: tw=80 ts=4 sw=4 noet
