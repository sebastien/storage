"""Backend contracts and composition helpers."""

from .base import StorageBackend as StorageBackend
from .composite import MultiBackend as MultiBackend

__all__ = [
	"MultiBackend",
	"StorageBackend",
]
