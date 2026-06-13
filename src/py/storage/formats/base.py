"""Codec base class for value encode/decode."""

from typing import ClassVar, Generic, TypeVar


V = TypeVar("V")


class Codec(Generic[V]):
	"""Base class for value encoders/decoders.

	Subclasses define `encode` and `decode` to convert between
	Python values and raw bytes for backend storage.
	"""

	NAME: ClassVar[str] = "base"

	def encode(self, value: V) -> bytes:
		raise NotImplementedError

	def decode(self, data: bytes) -> V:
		raise NotImplementedError


__all__ = [
	"Codec",
]
