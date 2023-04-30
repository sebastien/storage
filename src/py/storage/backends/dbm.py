from . import StorageBackend

# FIMXE: We should get away from the DBM backend as it seems to have
# numerous problems -- I got a lot of "cannot write..". Maybe I'm
# using it wrong?
import dbm.ndbm
import time

# FIXME: This implementation is a bit shit. The changes should be queued and
# a worker should work on that queue.


class DBMBackend(StorageBackend):
    """A really simple backend that wraps Python's DBM module. Key and value
    data are converted to JSON strings on the fly."""

    def __init__(self, path, autoSync=False):
        super().__init__()
        self._dbm = dbm
        self.path = f"{path}.dbm"
        self.autoSync = autoSync
        self.values = None
        self._open()

    def _open(self, mode="a") -> True:
        if self.values is None:
            try:
                self.values = self._dbm.open(self.path, "c")
                return True
            except self._dbm.error as e:
                raise RuntimeError("Cannot open DBM at path {}:{}".format(self.path, e))
        else:
            return False

    def _tryAdd(self, key, data):
        # SEE: http://stackoverflow.com/questions/4995162/python-shelve-dbm-error/12167172#12167172
        # NOTE: I've encountered a lot of problems with DBM, it does not
        # seem to be very reliable for that kind of application
        retries = 5
        if not self.values:
            self._open()
        if not self.values:
            raise RuntimeError(f"Could not open DBM database at: {self.path}")
        if key:
            while True:
                try:
                    self.values[key] = data
                    return True
                except self._dbm.error as e:
                    # FIXME: This is not cool, this should be done in a worker.
                    time.sleep(0.100 * retries)
                    if retries == 0:
                        raise Exception(
                            "{0} in {1}.db with key={2} data={3}".format(
                                e, self.path, key, data
                            )
                        )
                retries -= 1

    def add(self, key, data):
        key, data = self._serialize(key, data)
        self._tryAdd(key, data)
        return self

    def update(self, key, data):
        key, data = self._serialize(key, data)
        self._tryAdd(key, data)
        return self

    def remove(self, key):
        key = self._serialize(key=key)
        del self.values[key]

    def sync(self):
        # FIXME: Sync is an expensive operation, so it should really not be done on every operation.
        # self.values.sync()
        pass

    def has(self, key):
        key = self._serialize(key=key)
        return key in self.values

    def get(self, key):
        key = self._serialize(key=key)
        data = self.values.get(key)
        if data is None:
            return data
        else:
            return self._deserialize(data=data)

    def keys(self, collection=None, order=StorageBackend.ORDER_NONE):
        keys = list(self.values.keys())
        if order == StorageBackend.ORDER_ASCENDING:
            keys = sorted(keys)
        elif order == StorageBackend.ORDER_DESCENDING:
            keys = sorted(keys, reverse=True)
        for key in keys:
            yield self._deserialize(key=key)

    def clear(self):
        # TODO: Not very optimized
        for k in list(self.keys()):
            self.remove(k)
        self.close()
        self._open()

    def list(self, key=None):
        assert key is None, "Not implemented"
        for data in list(self.values.values()):
            yield self._deserialize(data=data)

    def count(self, key=None) -> int:
        assert key is None, "Not implemented"
        return len(self.values) if self.values else 0

    def close(self) -> bool:
        if self.values:
            self.sync()
            self.values.close()
            self.values = None
            return True
        else:
            return False

    def __del__(self):
        self.close()


# EOF
