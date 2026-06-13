"""Generic key-value storage with plugable backends, key types, and codecs.

The architecture has two layers:

	- `StorageBackend` — low-level shared backend API
	- `KVKeyNormalizer` — converts user keys (str, list[str], tuple) to storage strings
	- `KVStorage` — user-facing facade combining backend + normalizer + codec

Usage::

	from storage.kv import KVStorage, StringKVKeyNormalizer
	from storage.formats import JSONCodec
	from storage.backends.memory import KVMemoryBackend

	b = KVMemoryBackend()
	kv = KVStorage(b, normalizer=StringKVKeyNormalizer(), codec=JSONCodec())
	kv.set("hello", {"world": 42})
"""

from typing import (
	ClassVar,
	Dict,
	Generic,
	Iterator,
	List,
	Optional,
	Tuple,
	TypeVar,
	Union,
)

from .backends import StorageBackend
from .formats.base import Codec


# -----------------------------------------------------------------------------
#
# ERRORS
#
# -----------------------------------------------------------------------------


class KVError(Exception):
	"""Base exception for KV storage failures."""

	def __init__(self, message: str):
		super().__init__(message)


class KVFull(KVError):
	"""Raised when the KV store is full (quota exceeded, disk full, etc.)."""

	def __init__(self, message: str = "KV storage is full"):
		super().__init__(message)


class KVFailure(KVError):
	"""Raised when an operation cannot complete due to internal failure."""

	def __init__(self, message: str = "KV storage operation failed"):
		super().__init__(message)


# -----------------------------------------------------------------------------
#
# KEY NORMALIZERS
#
# -----------------------------------------------------------------------------


K = TypeVar("K")


class KVKeyNormalizer(Generic[K]):
	"""Converts between user-facing keys and storage-level string keys.

	Subclasses define the `K` type (e.g. ``str``, ``List[str]``, ``Tuple[str, ...]``)
	and provide five operations:

	- ``normalize``   — accept ``str | K``, return canonical ``K``
	- ``serialize``   — ``K -> str``
	- ``parse``       — ``str -> K``
	- ``join``        — ``prefix: str, K -> str``
	- ``matches``     — ``prefix: str, K -> bool``
	"""

	def normalize(self, key: Union[str, K]) -> K:
		raise NotImplementedError

	def serialize(self, key: K) -> str:
		raise NotImplementedError

	def parse(self, s: str) -> K:
		raise NotImplementedError

	def join(self, prefix: str, key: K) -> str:
		raise NotImplementedError

	def unjoin(self, prefix: str, storageKey: str) -> K:
		raise NotImplementedError

	def matches(self, prefix: str, key: K) -> bool:
		raise NotImplementedError


class StringKVKeyNormalizer(KVKeyNormalizer[str]):
	"""Normalizer for simple string keys.

	Keys are stored as-is. Prefix is concatenated: ``prefix + key``.
	"""

	def normalize(self, key: Union[str, str]) -> str:
		return key if isinstance(key, str) else "/".join(key)

	def serialize(self, key: str) -> str:
		return key

	def parse(self, s: str) -> str:
		return s

	def join(self, prefix: str, key: str) -> str:
		return prefix + key

	def unjoin(self, prefix: str, storageKey: str) -> str:
		if prefix and storageKey.startswith(prefix):
			return storageKey[len(prefix):]
		return storageKey

	def matches(self, prefix: str, key: str) -> bool:
		return key.startswith(prefix)


class PathKVKeyNormalizer(KVKeyNormalizer[List[str]]):
	"""Normalizer for path-based keys ``[...dirs, filename]``.

	Keys are stored as ``"/".join(components)``. Prefix is prepended as
	the first path component when joining.
	"""

	SEPARATOR: ClassVar[str] = "/"

	def normalize(self, key: Union[str, List[str]]) -> List[str]:
		return key if isinstance(key, list) else key.split(self.SEPARATOR)

	def serialize(self, key: List[str]) -> str:
		return self.SEPARATOR.join(key)

	def parse(self, s: str) -> List[str]:
		return s.split(self.SEPARATOR)

	def join(self, prefix: str, key: List[str]) -> str:
		if prefix:
			return self.SEPARATOR.join([prefix] + key)
		else:
			return self.SEPARATOR.join(key)

	def unjoin(self, prefix: str, storageKey: str) -> List[str]:
		if prefix and storageKey.startswith(prefix):
			remainder = storageKey[len(prefix):]
			if remainder.startswith(self.SEPARATOR):
				remainder = remainder[len(self.SEPARATOR):]
			return self.parse(remainder)
		return self.parse(storageKey)

	def matches(self, prefix: str, key: List[str]) -> bool:
		return len(key) > 0 and key[0] == prefix


class TupleKVKeyNormalizer(KVKeyNormalizer[Tuple[str, ...]]):
	"""Normalizer for tuple keys (e.g. ``["bucket", "key"]``).

	When joining, the prefix is prepended to the first tuple element.
	"""

	def __init__(self, separator: str = ":"):
		self.separator = separator

	def normalize(self, key: Union[str, Tuple[str, ...]]) -> Tuple[str, ...]:
		return key if isinstance(key, tuple) else tuple(key.split(self.separator))

	def serialize(self, key: Tuple[str, ...]) -> str:
		return self.separator.join(key)

	def parse(self, s: str) -> Tuple[str, ...]:
		return tuple(s.split(self.separator))

	def join(self, prefix: str, key: Tuple[str, ...]) -> str:
		return self.separator.join((prefix + key[0],) + key[1:])

	def unjoin(self, prefix: str, storageKey: str) -> Tuple[str, ...]:
		if prefix and storageKey.startswith(prefix):
			storageKey = storageKey[len(prefix):]
		return self.parse(storageKey)

	def matches(self, prefix: str, key: Tuple[str, ...]) -> bool:
		return len(key) > 0 and key[0].startswith(prefix)


# -----------------------------------------------------------------------------
#
# USER-FACING KV STORAGE
#
# -----------------------------------------------------------------------------


K = TypeVar("K")
V = TypeVar("V")

KVStorageBackend = StorageBackend


class KVStorage(Generic[K, V]):
	"""Generic key-value store wrapping a backend, normalizer, and codec.

	``K`` is the user-facing key type (``str``, ``List[str]``, ``Tuple[str, ...]``).
	``V`` is the value type (serialized as bytes by the ``codec``).

	A prefix is applied transparently to all storage keys via the normalizer's
	``join`` method. Batch operations are best-effort and non-atomic.
	"""

	def __init__(
		self,
		backend: StorageBackend,
		*,
		prefix: str = "",
		normalizer: KVKeyNormalizer[K],
		codec: Codec[V],
	):
		self.backend = backend
		self.prefix = prefix
		self.normalizer = normalizer
		self.codec = codec

	def _key(self, key: Union[str, K]) -> str:
		"""Normalize user key and prefix it."""
		k = self.normalizer.normalize(key)
		return self.normalizer.join(self.prefix, k)

	def _unkey(self, storage_key: str) -> K:
		"""Remove prefix and parse back to user key."""
		s = storage_key
		if self.prefix:
			assert s.startswith(self.prefix), (
				"Storage key does not start with prefix: %s" % (s)
			)
			s = s[len(self.prefix):]
		return self.normalizer.parse(s)

	def _encode(self, value: V) -> bytes:
		return self.codec.encode(value)

	def _decode(self, data: bytes) -> V:
		return self.codec.decode(data)

	# --- Single operations -----------------------------------------------

	def set(self, key: Union[str, K], value: V) -> V:
		"""Set ``key`` to ``value``. Returns ``value``."""
		sk = self._key(key)
		try:
			self.backend.set(sk, self._encode(value))
		except KVError:
			raise
		except Exception as e:
			raise KVFailure("set(%s) failed: %s" % (key, e)) from e
		return value

	def get(self, key: Union[str, K]) -> Optional[V]:
		"""Return the value at ``key``, or ``None`` if missing."""
		sk = self._key(key)
		try:
			data = self.backend.get(sk)
		except KVError:
			raise
		except Exception as e:
			raise KVFailure("get(%s) failed: %s" % (key, e)) from e
		return self._decode(data) if data is not None else None

	def has(self, key: Union[str, K]) -> bool:
		"""Return ``True`` if ``key`` exists in the store."""
		sk = self._key(key)
		try:
			return self.backend.has(sk)
		except KVError:
			raise
		except Exception as e:
			raise KVFailure("has(%s) failed: %s" % (key, e)) from e

	def delete(self, key: Union[str, K]) -> None:
		"""Remove ``key`` from the store. Idempotent."""
		sk = self._key(key)
		try:
			self.backend.delete(sk)
		except KVError:
			raise
		except Exception as e:
			raise KVFailure("del(%s) failed: %s" % (key, e)) from e

	# --- Batch operations ------------------------------------------------

	def setm(self, entries: Dict[Union[str, K], V]) -> Dict[K, V]:
		"""Set multiple entries. Best-effort, non-atomic.

		Returns a dict of successfully stored keys to values.
		"""
		result: Dict[K, V] = {}
		for key, value in entries.items():
			k = self.normalizer.normalize(key)
			try:
				self.set(k, value)
				result[k] = value
			except (KVError, Exception):
				pass
		return result

	def getm(self, keys: List[Union[str, K]]) -> Dict[K, V]:
		"""Get multiple keys. Best-effort, non-atomic.

		Returns a dict mapping each found key to its value.
		"""
		result: Dict[K, V] = {}
		for key in keys:
			k = self.normalizer.normalize(key)
			try:
				value = self.get(k)
				if value is not None:
					result[k] = value
			except (KVError, Exception):
				pass
		return result

	def hasm(self, keys: List[Union[str, K]]) -> Dict[K, bool]:
		"""Check multiple keys. Best-effort, non-atomic.

		Returns a dict mapping each requested key to its presence.
		"""
		result: Dict[K, bool] = {}
		for key in keys:
			k = self.normalizer.normalize(key)
			try:
				result[k] = self.has(k)
			except (KVError, Exception):
				result[k] = False
		return result

	def deletem(self, keys: List[Union[str, K]]) -> None:
		"""Delete multiple keys. Best-effort, non-atomic."""
		for key in keys:
			k = self.normalizer.normalize(key)
			try:
				self.delete(k)
			except (KVError, Exception):
				pass

	# --- Iteration -------------------------------------------------------

	def ilist(self, prefix: Optional[Union[str, K]] = None) -> Iterator[K]:
		"""Iterate over all stored keys, optionally filtered by ``prefix``.

		The prefix is normalized and matched against storage keys. Matching
		keys are returned as user-facing keys (with prefix stripped).
		"""
		storage_prefix = self.prefix
		if prefix not in (None, ""):
			storage_prefix = self.normalizer.join(self.prefix, self.normalizer.normalize(prefix))
		for sk in self.backend.keys(storage_prefix):
			if not sk.startswith(storage_prefix):
				continue
			yield self.normalizer.unjoin(self.prefix, sk)

	def iitems(self, prefix: Optional[Union[str, K]] = None) -> Iterator[Tuple[K, V]]:
		"""Iterate over all ``(key, value)`` pairs, optionally filtered by prefix."""
		for k in self.ilist(prefix):
			value = self.get(k)
			yield (k, value)

	# --- Maintenance ------------------------------------------------------

	def size(self) -> int:
		"""Return the number of entries in the store."""
		try:
			return self.backend.size()
		except KVError:
			raise
		except Exception as e:
			raise KVFailure("size() failed: %s" % (e)) from e

	def clear(self) -> None:
		"""Remove all entries from the store."""
		try:
			self.backend.clear()
		except KVError:
			raise
		except Exception as e:
			raise KVFailure("clear() failed: %s" % (e)) from e


# -----------------------------------------------------------------------------
#
# PUBLIC API
#
# -----------------------------------------------------------------------------

__all__ = [
	# Errors
	"KVError",
	"KVFull",
	"KVFailure",
	# Normalizers
	"KVKeyNormalizer",
	"StringKVKeyNormalizer",
	"PathKVKeyNormalizer",
	"TupleKVKeyNormalizer",
	# User-facing
	"KVStorageBackend",
	"KVStorage",
]
