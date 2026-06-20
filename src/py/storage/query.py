"""Transient stored query helpers.

The first implementation intentionally stays narrow: owner-scoped object
queries that can be resolved from the existing storage key layout and fed by
object journal entries.
"""

from typing import Any, Optional, Type

from .objects import StoredObject


# TODO: Support index-backed predicates such as `where(status="active")`.
# TODO: Support relation-backed queries and compound predicates.
# TODO: Maintain stable ordered result sets and pagination cursors.
# TODO: Support resumable/durable query subscriptions across processes.
# TODO: Support membership transition detection for non-owner predicates.


class StoredQuery:
	"""A transient owner-scoped query over a stored object type."""

	def __init__(
		self,
		storableClass: Type[StoredObject],
		owner: Optional[Any] = None,
		target: Optional[dict] = None,
		export: Optional[dict] = None,
	):
		self.storableClass = storableClass
		self.owner = owner
		self.target = dict(target or {})
		self.exportOptions = dict(export or {})

	def ownerID(self):
		owner = self.owner
		if isinstance(owner, StoredObject):
			return owner.id
		elif isinstance(owner, dict):
			return owner.get("id")
		elif isinstance(owner, (tuple, list)) and owner:
			return owner[0]
		else:
			return owner

	def prefix(self) -> str:
		owner_id = self.ownerID()
		if owner_id is None:
			return self.storableClass.StoragePrefix()
		return "%s.%s." % (
			self.storableClass.StoragePrefix(),
			self.storableClass.OwnerBucket(owner_id),
		)

	def list(self, start: int = 0, end: Optional[int] = None, count: Optional[int] = None):
		if self.owner is None:
			items = list(self.storableClass.List())
		else:
			items = list(self.storableClass.OwnedBy(self.owner))
		if count is not None:
			end = start + count
		elif end is None:
			end = len(items)
		return items[start:end]

	def snapshot(self, cursor=None, start: int = 0, end: Optional[int] = None, count: Optional[int] = None):
		values = [_.export(**self.exportOptions) for _ in self.list(start=start, end=end, count=count)]
		return {
			"event": "snapshot",
			"cursor": cursor,
			"target": dict(self.target),
			"count": len(values),
			"values": values,
		}

	def eventFor(self, entry: dict, backend=None):
		operation = entry.get("operation")
		key = entry.get("key")
		if key is None or not str(key).startswith(self.prefix()):
			return None
		change = {
			"=": "added",
			"+": "updated",
			"-": "removed",
		}.get(operation)
		if change is None:
			return None
		meta = entry.get("meta") or {}
		res = {
			"event": "query",
			"change": change,
			"seq": entry.get("seq"),
			"operation": operation,
			"key": key,
			"type": meta.get("objectType"),
			"id": meta.get("objectID"),
			"revision": meta.get("revision"),
			"patch": entry.get("patch") or [],
			"target": dict(self.target),
			"entry": entry,
		}
		if change != "removed" and backend and backend.has(key):
			res["value"] = backend.get(key)
		return res


__all__ = ["StoredQuery"]


# EOF
