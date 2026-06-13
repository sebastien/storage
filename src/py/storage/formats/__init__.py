"""Plugable codec (encode/decode) interfaces and format implementations.

Codecs convert Python values to/from bytes for storage in KV backends.
"""

from .base import Codec as Codec
from .json import JSONCodec as JSONCodec


__all__ = [
	"Codec",
	"JSONCodec",
]
