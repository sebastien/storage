"""JSON-based value codec."""

import json
from typing import Any, ClassVar

from .base import Codec


class JSONCodec(Codec[Any]):
	"""Encodes Python values as JSON bytes."""

	NAME: ClassVar[str] = "json"

	def encode(self, value: Any) -> bytes:
		return json.dumps(value, ensure_ascii=False, default=str).encode("utf-8")

	def decode(self, data: bytes) -> Any:
		if data is None:
			return None
		return json.loads(data.decode("utf-8"))


__all__ = [
	"JSONCodec",
]
