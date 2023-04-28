from . import Backend


class DBMBackend(Backend):
    """A really simple backend that wraps Python's DBM module. Key and value
    data are converted to JSON strings on the fly."""

    def __init__(self, path, autoSync=False):
        Backend.__init__(self)
        # FIMXE: We should get away from the DBM backend as it seems to have
        # numerous problems -- I got a lot of "cannot write..". Maybe I'm
        # using it wrong?
        import dbm.ndbm

        self._dbm = dbm
        self.path = path
        self.autoSync = autoSync
        self.values = None
        self._open()

    def _open(self, mode="c"):
        try:
            self.values = self._dbm.open(self.path, "c")
        except self._dbm.error as e:
            raise Exception("Cannot open DBM at path {}:{}".format(self.path, e))

    def _tryAdd(self, key, data):
        # SEE: http://stackoverflow.com/questions/4995162/python-shelve-dbm-error/12167172#12167172
        # NOTE: I've encountered a lot of problems with DBM, it does not
        # seem to be very reliable for that kind of application
        retries = 5
        if key:
            while True:
                try:
                    self.values[key] = data
                    return True
                except self._dbm.error as e:
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
        if not self.autoSync:
            # On some DBM implementations, we might need to close the file
            # to flush it... but this is not the default behaviour
            self.values.close()
            self._open()

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

    def keys(self, collection=None, order=Backend.ORDER_NONE):
        keys = list(self.values.keys())
        if order == Backend.ORDER_ASCENDING:
            keys = sorted(keys)
        elif order == Backend.ORDER_DESCENDING:
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

    def count(self, key=None):
        assert key is None, "Not implemented"
        return len(self.values)

    def close(self):
        self.values.close()

    def __del__(self):
        self.close()


# EOF
