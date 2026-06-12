import json
import os
import sys
import time
from calendar import timegm
from datetime import UTC, datetime
from enum import Enum
from random import randint
from typing import Any, ClassVar
from uuid import uuid4

from .utils import TPrimitive, numcode


# -----------------------------------------------------------------------------
#
# SERIALIZATION
#
# -----------------------------------------------------------------------------

NOTHING = object()


def asJSON(value: Any) -> str:
	"""Serializes `value` as JSON."""
	return json.dumps(value)


def asPrimitive(value: Any, **options) -> TPrimitive:
	"""Converts `value` into storage-friendly primitive data."""
	options.setdefault("depth", 1)
	if value in (None, True, False):
		return value
	if type(value) in (str, int, float):
		return value
	if type(value) in (tuple, list):
		options["depth"] -= 1
		res = [asPrimitive(_, **options) for _ in value]
		options["depth"] += 1
		return res
	if type(value) is dict:
		res = {}
		options["depth"] -= 1
		for key in value:
			res[key] = asPrimitive(value[key], **options)
		options["depth"] += 1
		return res
	if hasattr(value, "__class__") and value.__class__.__name__ == "datetime":
		return asPrimitive(tuple(value.timetuple()), **options)
	if hasattr(value, "__class__") and value.__class__.__name__ == "date":
		return asPrimitive(tuple(value.timetuple()), **options)
	if hasattr(value, "__class__") and value.__class__.__name__ == "struct_time":
		return asPrimitive(tuple(value), **options)
	if hasattr(value, "export"):
		return value.export(**options)
	raise Exception("Type not supported: %s %s" % (type(value), value))


def getCanonicalName(aClass):
	"""Returns the canonical `module.Class` name for `aClass`."""
	return aClass.__module__ + "." + aClass.__name__


def restore(value):
	"""Restores a primitive export into a storable instance when possible."""
	if isinstance(value, Storable):
		return value
	if type(value) is dict and "type" in value and "id" in value:
		value_type = value["type"]
		i = value_type.rfind(".")
		assert i >= 0, "Object type should be `module.Class`, got {0}".format(
			value_type
		)
		module_name = value_type[:i]
		class_name = value_type[i + 1 :]
		declared_class = Storable.DECLARED_CLASSES.get(value_type)
		if declared_class:
			return declared_class.Import(value)
		if not sys.modules.get(module_name):
			__import__(module_name)
		module = sys.modules.get(module_name)
		a_class = getattr(module, class_name)
		return a_class.Import(value)
	if type(value) is dict:
		for key in value:
			bound_value = value[key]
			restored_value = restore(bound_value)
			if bound_value is not restored_value:
				value[key] = restored_value
		return value
	if type(value) in (tuple, list):
		return list(map(restore, value))
	return value


def unJSON(text, useRestore=True):
	"""Parses JSON text and optionally restores storable values."""
	value = json.loads(text)
	return restore(value) if useRestore else value


def isSame(a, b):
	"""Returns True when `a` and `b` refer to the same storable object."""
	a_type = None
	a_id = None
	b_type = None
	b_id = None
	if isinstance(a, Storable):
		a_type = getCanonicalName(a.__class__)
		a_id = a.id
	elif isinstance(a, dict):
		a_type = a.get("type")
		a_id = a.get("id")
	if isinstance(b, Storable):
		b_type = getCanonicalName(b.__class__)
		b_id = b.id
	elif isinstance(b, dict):
		b_type = b.get("type")
		b_id = b.get("id")
	if a_type is None or a_id is None or b_type is None or b_id is None:
		return False
	return a_type == b_type and str(a_id) == str(b_id)


# -----------------------------------------------------------------------------
#
# TIME AND IDENTIFIERS
#
# -----------------------------------------------------------------------------


class By(Enum):
	Year = 10**10
	Month = 10**8
	Day = 10**6
	Hour = 10**4
	Minute = 10**2


def getTimestamp(date=None, period=None):
	"""Returns a UTC Unix epoch timestamp in seconds."""
	if date is None:
		date = int(time.time())
	if isinstance(date, datetime):
		if date.tzinfo is None:
			date = date.replace(tzinfo=UTC)
		date = int(date.timestamp())
	if type(date) in (tuple, list):
		date = int(
			timegm(tuple(date[:9]) if len(date) >= 9 else tuple(date[:6]) + (0, 0, 0))
		)
	date = int(date)
	return date if period is None else int(date / period) * period


def parseTimestamp(t):
	"""Returns `t` as a UTC time tuple."""
	return time.gmtime(t)


class Identifier:
	NODE_ID: ClassVar[int] = 0
	DATE_BASE: ClassVar[datetime] = datetime(2000, 1, 1, 0, 0, 0, 0, tzinfo=UTC)
	TIME_BASE: ClassVar[int] = timegm(DATE_BASE.utctimetuple())

	@classmethod
	def ParseNodeID(cls, host=None):
		"""Parses a node id from `host`, `NODE_ID`, or `/etc/hostname`."""
		if host:
			name_suffix = host.split(".")[0].rsplit("-", 1)
			if len(name_suffix) != 2:
				return None
			try:
				return int(name_suffix[-1])
			except ValueError:
				return None
		if "NODE_ID" in os.environ:
			return int(os.environ["NODE_ID"])
		if os.path.exists("/etc/hostname"):
			with open("/etc/hostname") as f:
				for line in f.readlines():
					res = cls.ParseNodeID(line)
					if res is not None:
						return res
		return cls.NODE_ID

	@classmethod
	def UpdateNodeID(cls):
		"""Refreshes and returns the process node id."""
		cls.NODE_ID = cls.ParseNodeID()
		return cls.NODE_ID

	@classmethod
	def UUID(cls):
		"""Returns a UUID4 string."""
		return str(uuid4())

	@classmethod
	def Stamp(cls, rand=3, nodes=4) -> int:
		"""Returns a compact sortable integer stamp."""
		t = int((datetime.now(UTC) - cls.DATE_BASE).total_seconds() * 1000)
		base = t * (10 ** (nodes + rand))
		n = cls.NODE_ID * (10**rand)
		r = randint(0, (10**rand) - 1)
		return base + n + r

	@classmethod
	def ID(cls, node: int = 0, prefix: str | None = None) -> str:
		"""Returns a sortable object id with optional `prefix`."""
		t: str = numcode(time.clock_gettime_ns(time.CLOCK_TAI)).rjust(14, "0")[:14]
		n: str = numcode(node).rjust(4, "0")[:4]
		r = numcode(int.from_bytes(os.urandom(3))).rjust(4, "0")[:4]
		id = f"{t}-{n}-{r}"
		return f"{prefix}-{id}" if prefix else id

	@classmethod
	def Timestamp(cls, rand=3, nodes=4):
		"""Returns a sortable integer timestamp with node and random suffixes."""
		date = getTimestamp()
		return (
			(date * (10 ** (nodes + rand)))
			+ cls.NODE_ID * (10**rand)
			+ randint(0, (10**rand) - 1)
		)


# -----------------------------------------------------------------------------
#
# STORAGE OPERATIONS
#
# -----------------------------------------------------------------------------


class Operation(Enum):
	ADD = "="
	REMOVE = "-"
	UPDATE = "+"
	SAVE_RAW = "+R"


# -----------------------------------------------------------------------------
#
# STORABLE BASE TYPE
#
# -----------------------------------------------------------------------------


class Storable:
	DECLARED_CLASSES = {}
	STORAGE = None

	# FIXME: Should have the following attributes
	# created: UTC timestamp (int)
	# updated: UTC timestamp (int)
	# revision: int
	@classmethod
	def DeclareClass(cls, *classes):
		"""Registers concrete storable classes for restore-time lookup."""
		for c in classes:
			name = getCanonicalName(c)
			assert (
				name not in cls.DECLARED_CLASSES or cls.DECLARED_CLASSES[name] is c
			), "Conflict with class: %s" % (name)
			if name not in cls.DECLARED_CLASSES:
				cls.DECLARED_CLASSES[name] = c
		return cls

	@classmethod
	def Recognizes(self, data):
		raise NotImplementedError

	@classmethod
	def Import(self, data):
		raise NotImplementedError

	@classmethod
	def Get(self, sid):
		raise NotImplementedError

	def __init__(self):
		pass
		# self._revision = 0
		# self._history  = []
		# self._mtime    = None

	def update(self, data):
		raise NotImplementedError

	def save(self):
		raise NotImplementedError

	def export(self):
		raise NotImplementedError

	def remove(self):
		raise NotImplementedError

	def getRevision(self):
		raise NotImplementedError

	def getHistory(self):
		pass

	def commit(self, items=None, names=None):
		self._mtime = getTimestamp()
		self._revision += 1
		self._history.append((self._mtime, self._revision, names, asPrimitive(items)))

	def getStorageKey(self):
		"""Returns the backend key used to store this object."""
		raise NotImplementedError


# -----------------------------------------------------------------------------
#
# PUBLIC API
#
# -----------------------------------------------------------------------------

__all__ = [
	"By",
	"Identifier",
	"NOTHING",
	"Operation",
	"Storable",
	"asJSON",
	"asPrimitive",
	"getCanonicalName",
	"getTimestamp",
	"isSame",
	"parseTimestamp",
	"restore",
	"unJSON",
]


# FIXME: Why do we need that?
Identifier.UpdateNodeID()

# EOF
