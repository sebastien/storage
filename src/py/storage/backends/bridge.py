"""Serialization bridges for composing logical storage with physical backends."""

import base64
from typing import Any

from .base import StorageBackend
from ..formats.base import Codec


class SerializationBridge:
	"""Converts logical keys, values, and metadata for a backend representation."""

	def encodeKey(self, key: Any) -> Any:
		return key

	def decodeKey(self, key: Any) -> Any:
		return key

	def encodeValue(self, value: Any) -> Any:
		raise NotImplementedError

	def decodeValue(self, value: Any) -> Any:
		raise NotImplementedError

	def encodeMetadata(self, value: Any) -> Any:
		return self.encodeValue(value)

	def decodeMetadata(self, value: Any) -> Any:
		return self.decodeValue(value)


class JSONBridge(SerializationBridge):
	"""Passes JSON-compatible values through to JSON-capable backends."""

	def encodeValue(self, value: Any) -> Any:
		return value

	def decodeValue(self, value: Any) -> Any:
		return value


class IdentityBridge(SerializationBridge):
	"""Passes values through after validating their physical representation."""

	def __init__(self, valueType: type | tuple[type, ...] = (str, bytes)):
		self.valueType = valueType

	def encodeValue(self, value: Any) -> Any:
		if not isinstance(value, self.valueType):
			raise TypeError("Unsupported backend value type: %s" % type(value))
		return value

	def decodeValue(self, value: Any) -> Any:
		return value


class CodecBridge(SerializationBridge):
	"""Adapts a value codec to text, primitive, or byte backend storage."""

	PREFIX = "storage.codec:"

	def __init__(self, codec: Codec, representation: str = "str"):
		if representation not in ("bytes", "str", "primitive"):
			raise ValueError("Unsupported backend representation: %s" % representation)
		self.codec = codec
		self.representation = representation

	def encodeValue(self, value: Any) -> bytes | str:
		data = self.codec.encode(value)
		if self.representation == "bytes":
			return data
		return self.PREFIX + base64.b64encode(data).decode("ascii")

	def decodeValue(self, value: bytes | str) -> Any:
		if isinstance(value, bytes):
			return self.codec.decode(value)
		if not isinstance(value, str) or not value.startswith(self.PREFIX):
			raise ValueError("Encoded value does not use the configured codec bridge")
		return self.codec.decode(base64.b64decode(value[len(self.PREFIX) :]))

	def encodeMetadata(self, value: Any) -> str:
		"""Keep metadata text-safe even when values use a byte backend."""
		data = self.codec.encode(value)
		return self.PREFIX + base64.b64encode(data).decode("ascii")


class SerializationBackend(StorageBackend):
	"""Wraps a physical backend with a configurable serialization bridge."""

	def __init__(self, backend: StorageBackend, bridge: SerializationBridge):
		super().__init__()
		self.backend = backend
		self.bridge = bridge
		self.HAS_READ = backend.HAS_READ
		self.HAS_WRITE = backend.HAS_WRITE
		# Streaming a serialized record would expose its physical representation.
		self.HAS_STREAM = False
		self.HAS_FILE = backend.HAS_FILE
		self.HAS_PUBLISH = backend.HAS_PUBLISH
		self.HAS_RAW = backend.HAS_RAW
		self.HAS_ORDERING = backend.HAS_ORDERING

	def add(self, key, data):
		return self.backend.add(self.bridge.encodeKey(key), self.bridge.encodeValue(data))

	def update(self, key, data):
		return self.backend.update(self.bridge.encodeKey(key), self.bridge.encodeValue(data))

	def remove(self, key):
		return self.backend.remove(self.bridge.encodeKey(key))

	def clear(self):
		return self.backend.clear()

	def sync(self):
		return self.backend.sync()

	def has(self, key):
		return self.backend.has(self.bridge.encodeKey(key))

	def get(self, key):
		value = self.backend.get(self.bridge.encodeKey(key))
		return None if value is None else self.bridge.decodeValue(value)

	def list(self, key=None):
		for value in self.backend.list(self.bridge.encodeKey(key) if key is not None else None):
			yield self.bridge.decodeValue(value)

	def count(self, key=None):
		return self.backend.count(self.bridge.encodeKey(key) if key is not None else None)

	def keys(self, collection=None, order=StorageBackend.ORDER_NONE):
		for key in self.backend.keys(collection, order):
			yield self.bridge.decodeKey(key)

	def getMetadata(self, key=None, default=None):
		return self.backend.getMetadata(self.bridge.encodeKey(key) if key is not None else None, default)

	def setMetadata(self, key, value):
		return self.backend.setMetadata(self.bridge.encodeKey(key), value)

	def removeMetadata(self, key):
		return self.backend.removeMetadata(self.bridge.encodeKey(key) if key is not None else None)

	def path(self, key):
		return self.backend.path(self.bridge.encodeKey(key))

	def stream(self, key, size=None):
		raise NotImplementedError("SerializationBackend does not stream serialized records")

	def hasRawData(self, key, ext=None):
		return self.backend.hasRawData(self.bridge.encodeKey(key), ext)

	def saveRawData(self, key, data, ext=None):
		return self.backend.saveRawData(self.bridge.encodeKey(key), data, ext)

	def streamRawData(self, key, size=None, ext=None):
		return self.backend.streamRawData(self.bridge.encodeKey(key), size, ext)

	def getRawDataPath(self, key, ext=None):
		return self.backend.getRawDataPath(self.bridge.encodeKey(key), ext)


__all__ = [
	"CodecBridge",
	"IdentityBridge",
	"JSONBridge",
	"SerializationBackend",
	"SerializationBridge",
]
