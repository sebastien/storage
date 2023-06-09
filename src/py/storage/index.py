from . import Storable
from typing import Optional, Type
from .backends import StorageBackend
from .core import getCanonicalName, getTimestamp
import re, unicodedata

RE_SPACES = re.compile(r"[\s\t\n]+")
RE_NOALPHANUM = re.compile("[^A-Za-z0-9]+")

# -----------------------------------------------------------------------------
#
# INDEXING
#
# -----------------------------------------------------------------------------


class Indexing:
    """A collection of functions that transform the given value into a value
    or a list of values to be used with indexing."""

    @classmethod
    def Value(cls, value, object=None):
        """Transparent, just returns the value."""
        return value

    @classmethod
    def Normalize(cls, value: str, object=None) -> str:
        """Converts the word to UTF-8, lowercase, stripped and with single spaces."""
        return RE_SPACES.sub(" ", str(value or "").lower()).strip()

    # SEE: http://stackoverflow.com/questions/517923/what-is-the-best-way-to-remove-accents-in-a-python-unicode-string
    @classmethod
    def NoAccents(cls, value, object=None):
        value = str(value) if type(value) is not str else value
        nkfd_form = unicodedata.normalize("NFKD", value)
        return str(nkfd_form.encode("ASCII", "ignore"), "ascii")

    @classmethod
    def UpdateTime(cls, value, object: Storable = None) -> Optional[int]:
        """Returns the update time of the given object"""
        return object.getUpdateTime() if object else None

    @classmethod
    def Keyword(cls, value, object=None) -> str:
        """Normalizes the given value as a keyword. It will be filtered
        through `NoAccents`, then non-alphanumeric characters will be replaced
        by spaces, and the result  will be normalized."""
        text = str(value, "utf8") if not isinstance(value, str) else value
        text = cls.NoAccents(text)
        text = RE_NOALPHANUM.sub(" ", text)
        text = cls.Normalize(text)
        return text

    @classmethod
    def Keywords(cls, values, object=None, minLength=3):
        """Extracts keywords from the the given object. Returns them
        noramlized and without accents."""
        res = set()
        if isinstance(values, dict):
            value = list(values.values())
        if type(values) not in (tuple, list):
            values = (values,)
        for value in values:
            if not value:
                continue
            # We might have i18n fields that are like {en:XXX,fr:XXX}
            if isinstance(value, dict):
                words = []
                for _ in list(value.values()):
                    words.extend(_.split(" "))
            else:
                words = value.split(" ")
            for word in words:
                word = cls.Keyword(word)
                if word and len(word) >= minLength:
                    res.add(word)
        return list(res)

    @classmethod
    def Properties(cls, **properties):
        """Returns a function that will index the given `properties` using
        the given extractor. (for instance `Properties(name=Normalize)`)"""

        def indexer(cls, values, object=None):
            res = []
            scope = values
            for name, extractor in list(properties.items()):
                v = getattr(scope, name)
                v = extractor(v, scope)
                if type(v) in (tuple, list):
                    res.extend(v)
                else:
                    res.append(v)
            return res

        return indexer

    @classmethod
    def Paths(cls, separator):
        def f(value, object=None, sep=separator):
            res = []
            current = None
            if not value:
                return res
            for _ in value.split(sep):
                if not _:
                    continue
                if current is None:
                    current = _
                else:
                    current += sep + _
                res.append(current)
            return res

        return f


# -----------------------------------------------------------------------------
#
# INDEXES
#
# -----------------------------------------------------------------------------

# FROM: http://code.activestate.com/recipes/473786-dictionary-with-attribute-style-access/
class AttrDict(dict):
    """A dictionary with attribute-style access. It maps attribute access to
    the real dictionary."""

    def __init__(self, init={}):
        dict.__init__(self, init)

    def __getstate__(self):
        return list(self.__dict__.items())

    def __setstate__(self, items):
        for key, val in items:
            self.__dict__[key] = val

    def __repr__(self):
        return "%s(%s)" % (self.__class__.__name__, dict.__repr__(self))

    def __setitem__(self, key, value):
        return super(AttrDict, self).__setitem__(key, value)

    def __getitem__(self, name):
        return super(AttrDict, self).__getitem__(name)

    def __delitem__(self, name):
        return super(AttrDict, self).__delitem__(name)

    __getattr__ = __getitem__
    __setattr__ = __setitem__

    def copy(self):
        ch = AttrDict(self)
        return ch


class Indexes:
    """Manages a collection of indexes."""

    # TODO: Document shortcut usage
    def __init__(self, backendClass: Type[StorageBackend], prefix=""):
        self.backendClass = backendClass
        self.prefix = prefix
        self.indexes: list[tuple[int, Index]] = []

    def all(self):
        """Lists all the indexes registered in this index registry"""
        return [_[0] for _ in self.indexes]

    def rebuild(self, sync=False) -> int:
        """Lists all the indexes registered in this index registry"""
        count = 0
        for index, storable_class in self.indexes:
            count += index.rebuild(storable_class.All())
            if sync:
                index.save()
        return count

    def use(self, *storableClasses):
        for c in storableClasses:
            if type(c) in (list, tuple):
                self.use(*c)
                continue
            if not hasattr(c, "INDEX_BY"):
                continue
            path = self.prefix + getCanonicalName(c)
            for indexed_property, indexing_function in list(c.INDEX_BY.items()):
                index_path = path + "." + indexed_property
                # FIXME: Should provide a single backend for both forward and backward, no?
                storage = IndexStorage(
                    self.backendClass(index_path + "-fwd"),
                    self.backendClass(index_path + "-bwd"),
                )
                extractor, restorer = self._createIndexFunctions(
                    indexed_property, indexing_function, c
                )
                index = Index(storage, extractor=extractor, restorer=restorer)
                c.AddIndex(index)
                self.indexes.append((index, c))
                # We register shortcuts so that <StorableClass>.by.<property> will
                # give access to the index
                name = c.__name__.split(".")[-1]
                if not hasattr(self, name):
                    setattr(self, name, AttrDict())
                d = getattr(self, name)
                d.setdefault("by", AttrDict())
                d.by[indexed_property] = index
        # We build indexes that need to be built
        for index, storable_class in self.indexes:
            if not index.STORAGE.getLastUpdate():
                index.rebuild(storable_class.All())
                index.save()
        return self

    def _createIndexFunctions(self, name, extractor, storableClass):
        def r(value, storableClass=storableClass):
            return storableClass.STORAGE.get(value)

        if name == "_":

            def e1(value, name=name, extractor=extractor):
                return extractor(None, value)

            return e1, r
        else:

            def e2(value, name=name, extractor=extractor):
                return extractor(
                    hasattr(value, name) and getattr(value, name) or None, value
                )

            return e2, r


# -----------------------------------------------------------------------------
#
# INDEX
#
# -----------------------------------------------------------------------------


class Index:

    STORAGE = None

    def __init__(self, storage, extractor=None, restorer=None):
        self.extractor = extractor
        self.restorer = restorer
        self.STORAGE = storage

    def getSignature(self, value):
        """Returns the signature of the value, which will allow to find
        it in the reverse index. This will typically be the StoredObject
        `getStorageKey` method."""
        if isinstance(value, Storable):
            return value.getStorageKey()
        else:
            raise Exception("Index only support stored object for now")

    def _restoreValue(self, value):
        if self.restorer:
            return self.restorer(value)
        else:
            return value

    def getIndexKey(self, value):
        """Returns the key used to find the value"""
        return self.extractor(value)

    def add(self, value):
        self.STORAGE.add(self.getSignature(value), self.getIndexKey(value))

    def update(self, value):
        self.STORAGE.update(self.getSignature(value), self.getIndexKey(value))

    def get(self, key, restore=True):
        for _ in self.STORAGE.get(key) or ():
            if _ is not None:
                yield self._restoreValue(_) if restore else _

    def one(self, key, index=0, restore=True):
        i = 0
        for _ in self.STORAGE.get(key) or ():
            if _ is not None:
                if i == index:
                    return self._restoreValue(_) if restore else _
                else:
                    i += 1

    def has(self, key):
        try:
            next(self(key))
            return True
        except StopIteration as e:
            return False

    def count(self, key):
        return len(list(self.STORAGE.get(key) or ()))

    def getKeys(self, sig):
        return self.STORAGE.getKeys(sig)

    def keys(self, start=0, end=None, count=None, order=0):
        return self.STORAGE.keys(start=start, end=end, count=count, order=order)

    def list(self, start=0, end=None, count=None, order=0, restore=True):
        for _ in self.STORAGE.list(start=start, end=end, count=count, order=order):
            yield self._restoreValue(_) if restore else _

    def remove(self, value):
        self.STORAGE.remove(self.getSignature(value))

    def clear(self):
        self.STORAGE.clear()

    def rebuild(self, values):
        self.clear()
        count = 0
        for _ in values:
            self.add(_)
            count += 1
        return count

    def save(self):
        self.STORAGE.sync()

    def __call__(self, key, restore=True):
        return self.get(key, restore=restore)


# -----------------------------------------------------------------------------
#
# INDEX STORAGE
#
# -----------------------------------------------------------------------------

# TODO: Add cached ordered keys


class IndexStorage(object):
    """An index storage stores indexing information using two key-value
    backends. An Index produces a couple (key, signature) from a given value.
    The `key` represents its indexing key, the `signature` represents a
    way to uniquely identify the value."""

    KEY_LASTUPDATE = "__index__.lastUpdate"

    def __init__(self, forwardBackend, backwardBackend, metaBackend=None):
        """The forward backend maps the computed value index key to the value
        signature (for instance, a user email to a user id), while the
        backward backend does just the opposite."""
        self.forwardBackend = forwardBackend
        self.backwardBackend = backwardBackend
        self.metaBackend = metaBackend or backwardBackend

    def getLastUpdate(self):
        """Returns the timestamp of the last update"""
        return self.metaBackend.get(self.KEY_LASTUPDATE)

    def add(self, sig, keys):
        # We convert to multiple keys by default
        if type(keys) not in (tuple, list):
            keys = (keys,)
        # If the object was already there, we remove its entries in both
        # backends and add new ones
        has_backward = self.backwardBackend.has(sig)
        if has_backward:
            previous_keys = self.backwardBackend.get(sig)
            for previous_key in previous_keys:
                if self.forwardBackend.has(previous_key):
                    values = [
                        _ for _ in self.forwardBackend.get(previous_key) if _ != sig
                    ]
                    if not values:
                        self.forwardBackend.remove(previous_key)
                    else:
                        self.forwardBackend.update(previous_key, values)
                else:
                    # FIXME: There should be a warning here, as if there is a
                    # backward value, there should be a forward value!
                    pass
            self.backwardBackend.update(sig, keys)
        else:
            self.backwardBackend.add(sig, keys)
        # We update the forward backend, ensuring that the value is registered
        for key in keys:
            if self.forwardBackend.has(key):
                values = self.forwardBackend.get(key)
                values.append(sig)
                self.forwardBackend.update(key, values)
            else:
                self.forwardBackend.add(key, [sig])

    def get(self, key):
        return self.forwardBackend.get(key)

    def getKeys(self, sig):
        return self.backwardBackend.get(sig)

    def update(self, sig, key):
        self.add(sig, key)

    def keys(self, start=0, end=None, count=None, order=0):
        """Returns the given keys in database order (default), ascending order (order > 0)
        or descending order (order < 0)"""
        keys = self.forwardBackend.keys(order=order)
        i = 0
        if count is not None:
            end = start + count
        for k in keys:
            if end is not None and i >= end:
                break
            if i >= start:
                yield k
            i += 1

    def list(self, start=0, end=None, count=None, order=0):
        i = 0
        if count is not None:
            end = start + count
        for k in self.keys(order=order):
            for v in self.get(k):
                if end is not None and i >= end:
                    break
                if i >= start:
                    yield v
                i += 1

    def remove(self, sig):
        if self.backwardBackend.has(sig):
            previous_keys = self.backwardBackend.get(sig)
            for previous_key in previous_keys:
                # NOTE: We've seen some cases where forward_mapping can be done
                # this most likely happens when the extractor fails
                forward_mapping = self.forwardBackend.get(previous_key)
                values = [_ for _ in forward_mapping or () if _ != sig]
                if not values:
                    if forward_mapping != None:
                        self.forwardBackend.remove(previous_key)
                else:
                    self.forwardBackend.update(previous_key, values)
            self.backwardBackend.remove(sig)

    def clear(self):
        self.forwardBackend.clear()
        self.backwardBackend.clear()

    def sync(self):
        self.metaBackend.add(self.KEY_LASTUPDATE, getTimestamp())
        self.forwardBackend.sync()
        self.backwardBackend.sync()
        if self.metaBackend != self.backwardBackend:
            self.metaBackend.sync()


# EOF
