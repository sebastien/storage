"""Concrete HTTP service for storage APIs."""

from .channels import StorageChannel
from .decorators import StorageDecoration, http
from .errors import StorageWebError
from .objects import ObjectWebMixin
from .relations import RelationWebMixin

__all__ = [
	"ObjectWebMixin",
	"RelationWebMixin",
	"StorageChannel",
	"StorageDecoration",
	"StorageServer",
	"StorageWebError",
	"http",
]


class StorageServer(RelationWebMixin, ObjectWebMixin):
	"""An Extra service that exposes storables through a REST-like API."""

	pass
