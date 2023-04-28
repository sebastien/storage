from .utils import numcode
from typing import ClassVar, Any
from uuid import uuid4
from datetime import datetime
from calendar import timegm
from random import randint
from enum import Enum
import time
import os
import sys
import json

NOTHING = object()


def asJSON(value: Any) -> str:
    return json.dumps(value)


def asPrimitive(value: Any, **options):
    options.setdefault("depth", 1)
    if value in (None, True, False):
        return value
    elif type(value) in (str, str, int, int, float):
        return value
    elif type(value) in (tuple, list):
        options["depth"] -= 1
        res = [asPrimitive(_, **options) for _ in value]
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


def restore(value):
    """Takes a primitive value and tries to restore a stored value instance
    out of it. If the value is not restorable, will return the value itself"""
    if isinstance(value, Storable):
        return value
    elif type(value) is dict and "type" in value and "oid" in value:
        value_type = value["type"]
        i = value_type.rfind(".")
        assert i >= 0, "Object type should be `module.Class`, got {0}".format(
            value_type
        )
        module_name = value_type[:i]
        class_name = value_type[i + 1 :]
        # NOTE: An alternative would be to look at declared classes
        if not sys.modules.get(module_name):
            __import__(module_name)
        module = sys.modules.get(module_name)
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
        return list(map(restore, value))
    else:
        return value


def isSame(a, b):
    """Tells if the two values refer to the same storable object (this works
    with exported storables and actual storables)"""
    a_type = None
    a_oid = None
    b_type = None
    b_oid = None
    if isinstance(a, Storable):
        a_type = a.getTypeName()
        a_oid = a.oid
    elif isinstance(a, dict):
        a_type = a.get("type")
        a_oid = a.get("oid")
    if isinstance(b, Storable):
        b_type = b.getTypeName()
        b_oid = b.oid
    elif isinstance(b, dict):
        b_type = b.get("type")
        b_oid = b.get("oid")
    if a_type is None or a_oid is None or b_type is None or b_oid is None:
        return False
    # NOTE: We have to assume that IDs are string, or force them to, as
    # JavaScript does not supprot long integers
    return a_type == b_type and str(a_oid) == str(b_oid)


def unJSON(text, useRestore=True):
    """Parses the given text as JSON, and if the result is an object, will try
    to identify whether the object is serialization of a metric, object or
    raw data, and restore it."""
    value = json.loads(text)
    if useRestore:
        return restore(value)
    else:
        return value


def getCanonicalName(aClass):
    """Returns the canonical name for the class"""
    return aClass.__module__ + "." + aClass.__name__


class By(Enum):
    Year = 10**10
    Month = 10**8
    Day = 10**6
    Hour = 10**4
    Minute = 10**2


def getTimestamp(date=None, period=None):
    """Returns a number that is like: 'YYYYMMDDhhmmssuuuuuu' representing the
    current date in UTC timezone. This number preserves the ordering an allows
    to easily identify the date (or at least easier than a timestamp
    since EPOCH)."""
    # FIXME: Should return UTC datetime
    if date is None:
        date = datetime.utcnow()
    if isinstance(date, datetime):
        date = (
            date.year,
            date.month,
            date.day,
            date.hour,
            date.minute,
            date.second,
            date.microsecond,
        )
    if type(date) in (tuple, list):
        if len(date) == 9:
            # This is a timetuple
            year, month, day, hour, mn, sec, _, _, _ = date
            msec = 0
        else:
            year, month, day, hour, mn, sec, msec = date
        date = (
            msec
            + sec * 10**6
            + mn * 10**8
            + hour * 10**10
            + day * 10**12
            + month * 10**14
            + year * 10**16
        )
    if period is None:
        return date
    else:
        return int(date / period) * period


def parseTimestamp(t):
    """Returns the timestamp as an UTC time-tuple"""
    year = t / 10**16
    t -= year * 10**16
    month = t / 10**14
    t -= month * 10**14
    day = t / 10**12
    t -= day * 10**12
    hour = t / 10**10
    t -= hour * 10**10
    mn = t / 10**8
    t -= mn * 10**8
    sec = t / 10**6
    t -= mn * 10**6
    return (year, month, day, hour, mn, sec, 0, 0, 0)


class Identifier:

    NODE_ID: ClassVar[int] = 0
    DATE_BASE: ClassVar[datetime] = datetime(2000, 1, 1, 0, 0, 0, 0)
    TIME_BASE = timegm(DATE_BASE.utctimetuple())

    @classmethod
    def ParseNodeID(cls, host=None):
        """Parses the node ID based on the hostname (if it is suffixed by a dash
        and a number. If the NODE_ID is defined in the environment, it will take
        over."""
        if host:
            name_suffix = host.split(".")[0].rsplit("-", 1)
            if len(name_suffix) != 2:
                return None
            try:
                return int(name_suffix[-1])
            except ValueError:
                return None
        elif "NODE_ID" in os.environ:
            return int(os.environ["NODE_ID"])
        elif os.path.exists("/etc/hostname"):
            with open("/etc/hostname") as f:
                for line in f.readlines():
                    res = cls.ParseNodeID(line)
                    if res is not None:
                        return res
        return cls.NODE_ID

    @classmethod
    def UpdateNodeID(cls):
        cls.NODE_ID = cls.ParseNodeID()
        return cls.NODE_ID

    @classmethod
    def UUID(cls):
        """Returns a UUI4"""
        return str(uuid4())

    @classmethod
    def Stamp(cls, rand=3, nodes=4) -> int:
        """Returns an 64-bit integer timestamp that is made like this.

        > TTTTTTTTTTTTTTT NNNN RRR
        > |               |    |
        > |               |    Random [0-999]
        > |               Node ID [0-9999]
        > Timestamp since TIMEBASE

        The `rand` and `nodes` parameters tell how many `R` and `N` there
        wil be in the numbers.
        """
        t = int((datetime.utcnow() - cls.DATE_BASE).total_seconds() * 1000)
        base = t * (10 ** (nodes + rand))
        n = cls.NODE_ID * (10**rand)
        r = randint(0, (10**rand) - 1)
        return base + n + r

    @classmethod
    def OID(cls, node: int = 0) -> str:
        """Creates an id that contains a timestamp, a node id and some random
        factor, that should make the jobs ids largely sortable"""
        t: str = numcode(time.clock_gettime_ns(time.CLOCK_TAI)).rjust(14, "0")[:14]
        n: str = numcode(node).rjust(4, "0")[:4]
        # NOTE: math.log(math.pow(2,3 * 8), 62) ~ 3
        r = numcode(int.from_bytes(os.urandom(3))).rjust(4, "0")[:4]
        return f"{t}-{n}-{r}"

    @classmethod
    def Timestamp(cls, rand=3, nodes=4):
        """A version of the `stamp` that's more readable but slightly longer.

        > YYYYMMDDHHMMSSMMMMMM NNNNN RRR
        > |                    |   |
        > |                    |   Random [0-999]
        > |                    Node ID [0-9999]
        > Timestamp since TIMEBASE

        """
        date = getTimestamp()
        return (
            (date * (10 ** (nodes + rand)))
            + cls.NODE_ID * (10**rand)
            + (randint(0, (10**rand) - 1))
        )


# -----------------------------------------------------------------------------
#
# OPERATIONS
#
# -----------------------------------------------------------------------------


class Operation(Enum):
    ADD = "="
    REMOVE = "-"
    UPDATE = "+"
    SAVE_RAW = "+R"


# -----------------------------------------------------------------------------
#
# STORABLE
#
# -----------------------------------------------------------------------------

# NOTE: We have to use new-style classes as we're using descriptors
class Storable:

    DECLARED_CLASSES = {}
    STORAGE = None

    # FIXME: Should have the following attributes
    # created: UTC timestamp (int)
    # updated: UTC timestamp (int)
    # revision: int
    @classmethod
    def DeclareClass(cls, *classes):
        """This allows to declare classes and then resovle them. This is used
        by unJSON so that storable objects are properly restored."""
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

    def getID(self):
        """Returns the identifier of this object. The identifier will be
        unique amongst objects of the same class. You should use `getStorageKey`
        to have a globally unique ID"""
        raise NotImplementedError

    def getRevision(self):
        raise NotImplementedError

    def getHistory(self):
        pass

    def commit(self, items=None, names=None):
        self._mtime = getTimestamp()
        self._revision += 1
        self._history.append((self._mtime, self._revision, names, export(items)))

    def getStorageKey(self):
        """Returns the key with which this object will be stored in an underlying
        backend"""
        raise NotImplementedError


# FIXME: Why do we need that?
Identifier.UpdateNodeID()
# EOF
