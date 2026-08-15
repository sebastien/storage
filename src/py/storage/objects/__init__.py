"""Stored object model, descriptors, and runtime storage."""

from .descriptors import (
	Property as Property,
	PropertyDescriptor as PropertyDescriptor,
	Relation as Relation,
	RelationDescriptor as RelationDescriptor,
)
from .model import Ownership as Ownership, PublicID as PublicID, StoredObject as StoredObject
from .storage import ObjectStorage as ObjectStorage

# These names are part of the persisted object format and remain rooted at the
# public package rather than at an implementation module.
for _class in (Ownership, PublicID, StoredObject):
	_class.__module__ = __name__

__all__ = [
	"ObjectStorage",
	"Ownership",
	"PublicID",
	"Property",
	"PropertyDescriptor",
	"Relation",
	"RelationDescriptor",
	"StoredObject",
]
