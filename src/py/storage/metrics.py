from . import (
    Operation,
    DirectoryBackend,
    getCanonicalName,
    getTimestamp,
    Storable,
    NOTHING,
)
import json

__doc__ = """\
This module provides a model and persistenc backends to store monotone metrics
data (ie, metrics that do no change, but are just sampled at specific moments)
"""

# -----------------------------------------------------------------------------
#
# STORED METRIC
#
# -----------------------------------------------------------------------------


class StoredMetric(Storable):
    """An interface/abstract class that specifies the methods that should
    be implemented by metric values to be stored by storage providers
    defined in this module."""

    @classmethod
    def Recognizes(cls, data):
        if type(data) == dict:
            for key in ("name", "value", "timestamp"):
                if key not in data:
                    return False
            return True
        else:
            return False

    @classmethod
    def Import(cls, data):
        return cls(
            name=data["name"],
            value=data["value"],
            timestamp=data["timestamp"],
            meta=data.get("meta"),
        )

    def __init__(self, name, value, meta=None, timestamp=None):
        if timestamp is None:
            timestamp = getTimestamp()
        self.name = name
        self.value = value
        self.meta = meta
        self.timestamp = timestamp

    def isBefore(self, timestamp):
        return timestamp is None or self.timestamp < timestamp

    def isAfter(self, timestamp):
        return timestamp is None or self.timestamp >= timestamp

    def getName(self):
        """Returns the name for this metric, as a string. The recommended
        notation is dot separated camelCase."""
        return self.name

    def getValue(self):
        """Returns the actual value for this metric. This is expected to
        be either a number or a string, but in some cases, it might be a
        simple list or map of these elements. In general, values should
        be kept as simple as possible."""
        return self.value

    def getTimestamp(self):
        """Returns the timestamp corresponding to the moment where the value
        was sampled. The recommended format here is an integer number
        following 'YYYYMMDDhhmmss' -- so that they are easy to read when
        view and still preserve ordering."""
        return self.timestamp

    def get(self, key=NOTHING):
        """Alias for getMeta"""
        return self.getMeta(key)

    def getMeta(self, key=NOTHING):
        """Returns a dictionary of meta-informations attached to the event."""
        if key is NOTHING:
            return self.meta
        else:
            return self.meta.get(key) if self.meta else None

    def hasMeta(self, key):
        return key in self.meta if self.meta else False

    def export(self, **options):
        return dict(
            name=self.name,
            value=self.value,
            timestamp=self.timestamp,
            meta=self.meta,
            type=getCanonicalName(self.__class__),
        )


# -----------------------------------------------------------------------------
#
# METRIC STORAGE
#
# -----------------------------------------------------------------------------


class MetricStorage:
    """A metric storage is typically a monotone (the past doesn't change),
    append-only storage. Values stored in metric are composed of (key, timestamp,
    value, meta), as opposed as just (key, value) as it would be the case with
    object storage.

    A metric storage doesn't need caching as an object does, as the typical
    use it to query the metric by key and time range, usually computing
    aggregates on the fly.

    As with object storage, the metric storage has a sync policy, where
    you should call sync explicitely -- although some backends might
    sync automatically.
    """

    def __init__(self, backend, metricClass=StoredMetric):
        """Creates a new metric storage with the given backend"""
        self.backend = backend
        self.metricClass = metricClass

    def _ensureMetric(self, metric):
        if not isinstance(metric, self.metricClass):
            metric = self.metricClass.Import(metric)
        return metric

    def add(self, metric):
        """Adds the given metric to the storage."""
        metric = self._ensureMetric(metric)
        self.backend.add(metric.getName(), self._export(metric, Operation.ADD))

    def get(self, name, after=None, before=None):
        """Gets the metric with the given name from the storage."""
        for _ in self.backend.get(name):
            m = self._ensureMetric(_)
            if after is not None and not m.isAfter(after):
                continue
            if before is not None and not m.isBefore(before):
                continue
            yield m

    def remove(self, metric):
        """Removes the given metric to the storage. In most cases, the
        metric won't be actually removed, but just invalidated."""
        metric = self._ensureMetric(metric)
        self.backend.remove(metric.getName(), self._export(metric, Operation.REMOVE))

    def update(self, metric):
        """Updates the value for the given metric in the storage."""
        metric = self._ensureMetric(metric)
        self.backend.update(metric.getName(), self._export(metric, Operation.UPDATE))

    def sync(self):
        """Explicitely ask the back-end to synchronize. Depending on the
        back-end this might be a long or short, blocking or async
        operation."""
        self.backend.sync()

    def keys(self, prefix=None):
        for k in self.backend.keys(prefix=prefix):
            yield k

    def query(self, name=None, timestamp=None):
        """Returns a list of metric with the given name and timestamp.
        If name is a string, then only the metrics with the given names
        will be returned. If name is a list, only the metrics within the
        given list will be returned, if name is a function it will be used
        as a predicate to filter the name.

        If timestamp is a number, it will return only the metrics with the
        actual timestamp. If it's a couple `(a,b)`, then it will return all the
        metrics where `a <= timestamp  < b`. If it is a function, it will
        return only the metrics where the timestamp acts as a predicate."""
        return self.backend.queryMetrics(name=name, timestamp=timestamp)

    def list(self):
        """Lists the metrics available in this backend"""
        return self.backend.listMetrics()

    def _export(self, metric, operation):
        """Serializes the given metric to a string."""
        return metric.export()

    def _import(self, data):
        """Creates a metric instance out of the given (previously serialized)
        value."""
        return self.metricClass.Import(data)


# -----------------------------------------------------------------------------
#
# METRIC DIRECTORY BACKEND
#
# -----------------------------------------------------------------------------


class MetricsDirectoryBackend(DirectoryBackend):
    """A specialized directory back-end designed to store metrics and allows
    for fast-searching."""

    FILE_EXTENSION = ".metric"

    def _defaultWriter(self, backend, operation, path, data):
        line = operation + "\t" + data
        return self.appendFile(path, line)

    def get(self, key, after=None, before=None):
        # FIXME: Should be smart when it comes to finding the offset of
        # after/before
        with open(self.path(key), "rb") as f:
            for line in f.readlines():
                data = self._deserialize(data=line)
                yield data

    def _serialize(self, key=NOTHING, data=NOTHING):
        """Serializes the given metric to a string."""
        # NOTE: Data is expected to be a metric-export
        if data is not NOTHING:
            data = (
                "%d\t%s\t%s\t%s\n"
                % (
                    data["timestamp"],
                    data["name"],
                    json.dumps(data["value"]),
                    json.dumps(data.get("meta")),
                )
                if type(data) not in (str, str)
                else data
            )
        if key is not NOTHING:
            assert type(key) in (str, str), (
                self.__class__.__name__ + "._serialize only accepts strings as key."
            )
            key = str(key)
        if key is NOTHING:
            return data
        elif data is NOTHING:
            return key
        else:
            return key, data

    def _deserialize(self, key=NOTHING, data=NOTHING):
        """Creates a metric instance out of the given (previously serialized)
        value."""
        if data is not NOTHING:
            operation, timestamp, name, value, meta = data.split("\t", 5)
            data = dict(
                name=name,
                timestamp=int(timestamp),
                value=json.loads(value),
                meta=json.loads(meta),
            )
        if key is not NOTHING:
            return key
        if key is NOTHING:
            return data
        elif data is NOTHING:
            return key
        else:
            return key, data


# EOF - vim: tw=80 ts=4 sw=4 noet
