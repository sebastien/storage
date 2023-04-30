from ..core import Operation, asJSON, unJSON, NOTHING
import logging

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
class StorageBackend:
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

    ORDER_NONE = 0
    ORDER_ASCENDING = 1
    ORDER_DESCENDING = -1

    HAS_READ = True
    HAS_WRITE = True
    HAS_STREAM = False
    HAS_FILE = False
    HAS_PUBLISH = True
    HAS_RAW = False
    HAS_ORDERING = False

    def __init__(self):
        self._onPublish = []
        self._subscribers = {}

    def onPublish(self, callback):
        """Adds a callback that will be invoked when the `publish` method
        is invoked."""
        if callback not in self._onPublish:
            self._onPublish.append(callback)
        return self

    def subscribe(self, key, callback):
        """Adds the given callback to subscribe to add/update/remove events
        on the given key."""
        callbacks = self._subscribers.setdefault(key, [])
        if callback not in callbacks:
            callbacks.append(callback)
        return self

    def unsubscribe(self, key, callback):
        """Unsubscribes the given callback from the given key."""
        callbacks = self._subscribers.setdefault(key, [])
        if callback in callbacks:
            callbacks.remove(callback)
        return self

    def notify(self, key, operation, data=None):
        """Notify the subscribers to the given key of the given operation
        and data."""
        callbacks = self._subscribers.get(key, [])
        for c in callbacks:
            try:
                c(key, operation, data)
            except Exception as e:
                logging.error(
                    "StorageBackend.notify: Exception in callback {0}: {1}".format(c, e)
                )
        return self

    def publish(self, operation, key, data=None):
        """The publish method allows to publish a change to the backend."""
        for callback in self._onPublish:
            callback(operation, key, data)

    def process(self, operation: Operation, key: str, data=None):
        """Processes the given operation (from `Operation`) with the given key
        and data. This is useful for processing a journal of transactions."""
        if operation is Operation.ADD:
            return self.add(key, data)
        elif operation is Operation.UPDATE:
            return self.udpate(key, data)
        elif operation is Operation.REMOVE:
            return self.remove(key)
        else:
            # The operation is not supported
            raise NotImplementedError

    def add(self, key, data):
        """Adds the given data to the storage."""
        raise NotImplementedError

    def update(self, key, data):
        """Updates the given data in the storage."""
        raise NotImplementedError

    def remove(self, key):
        """Removes the given data to the storage. In most cases, the
        metric won't be actually removed, but just invalidated."""
        raise NotImplementedError

    def clear(self):
        """Removes all the data from this backend."""
        raise NotImplementedError

    def sync(self):
        """Explicitly ask the back-end to synchronize. Depending on the
        back-end this might be a long or short, blocking or async
        operation."""
        raise Exception("Backend.sync not implemented")

    def has(self, key):
        raise Exception("Backend.has not implemented")

    def get(self, key):
        raise Exception("Backend.get not implemented")

    def list(self, key=None):
        raise Exception("Backend.list not implemented")

    def count(self, key=None):
        raise Exception("Backend.count not implemented")

    def keys(self, collection=None, order=ORDER_NONE):
        raise Exception("Backend.keys not implemented")

    def path(self, key):
        """Returns the physical path of the file used to store
        the key, if any."""
        raise Exception("Backend.path not implemented")

    def stream(self, key, size=None):
        """Streams the data at the given key by chunks of given `size`"""
        raise Exception("Backend.stream not implemented")

    def hasRawData(self, key, ext=None):
        """Tells if the backend has raw data assocaited with the given key and extension"""
        raise NotImplementedError

    def saveRawData(self, key, data, ext=None):
        """Saved the raw data associated with the given key and extension"""
        raise NotImplementedError

    def streamRawData(self, key, size=None, ext=None):
        """Loads the raw data associated with the given key and extension. Returns
        a generator that will load the data."""
        raise NotImplementedError

    def getRawDataPath(self, key, ext=None):
        """Returns the physical path to the. This only works if bakcend has HAS_FILE"""
        raise NotImplementedError

    def _serialize(self, key=NOTHING, data=NOTHING):
        if key is NOTHING:
            return asJSON(data)
        elif data is NOTHING:
            return asJSON(key)
        else:
            return asJSON(key), asJSON(data)

    def _deserialize(self, key=NOTHING, data=NOTHING):
        # NOTE: We use restore=False as we want the backends to store
        # primitives, it's up to the parent storages to use restore to restore
        # data.
        if key is NOTHING:
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


class MultiBackend(StorageBackend):
    """A backend that allows to multiplex different back-end together, which
    is especially useful for development (you can mix Journal, Memory and File
    for instance)."""

    def __init__(self, *backends):
        self.backends = backends
        self._readBackend = None
        self._fileBackend = None
        self._streamBackend = None
        for b in self.backends:
            if b.HAS_READ:
                self._readBackend = b
            if b.HAS_FILE:
                self._fileBackend = b
            if b.HAS_STREAM:
                self._streamBackend = b
            b.onPublish(
                lambda op, key, data=None, source=b: self._onBackendPublish(
                    op, key, data, source
                )
            )

    def _onBackendPublish(self, operation, key, data, source):
        """When a backend publishes an operation, the other backends will process
        the operation."""
        for b in self.backends:
            if b != source:
                b.process(operation, key, data)

    def add(self, key, data):
        for backend in self.backends:
            backend.add(key, data)

    def update(self, key, data):
        for backend in self.backends:
            backend.update(key, data)

    def remove(self, key):
        for backend in self.backends:
            backend.remove(key)

    def sync(self):
        for backend in self.backends:
            backend.sync()

    def has(self, key):
        if not self._readBackend:
            raise RuntimeError(f"Undefined read backend: {self}")
        return self._readBackend.has(key)

    def get(self, key):
        if not self._readBackend:
            raise RuntimeError(f"Undefined read backend: {self}")
        return self._readBackend.get(key)

    def list(self, key=None):
        if not self._readBackend:
            raise RuntimeError(f"Undefined read backend: {self}")
        return self._readBackend.list(key)

    def count(self, key=None):
        if not self._readBackend:
            raise RuntimeError(f"Undefined read backend: {self}")
        return self._readBackend.count(key)

    def keys(self, collection=None, order=StorageBackend.ORDER_NONE):
        if not self._readBackend:
            raise RuntimeError(f"Undefined read backend: {self}")
        return self._readBackend.keys(collection, order)

    def path(self, key):
        """Returns the physical path of the file used to store
        the key, if any."""
        if not self._fileBackend:
            raise RuntimeError(f"Undefined file backend: {self}")
        return self._fileBackend.path(key)

    def stream(self, key, size=None):
        """Streams the data at the given key by chunks of given `size`"""
        raise NotImplementedError


# EOF
