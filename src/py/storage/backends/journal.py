"""Journal backend wrapper and in-memory journal persistence."""

from __future__ import annotations

import hashlib
import json

from . import StorageBackend
from ..core import Operation, getTimestamp


class JournalPersistence:
	"""Stores journal entries without deciding compaction policy."""

	def nextSeq(self) -> int:
		raise NotImplementedError

	def append(self, entry: dict) -> dict:
		raise NotImplementedError

	def getCursor(self) -> int:
		raise NotImplementedError

	def getEntries(self, since=None, limit=None, keys=None, prefix=None):
		raise NotImplementedError

	def getChangedKeys(self, since=None, prefix=None):
		raise NotImplementedError

	def countEntries(self, key=None) -> int:
		raise NotImplementedError

	def getSnapshot(self, key):
		raise NotImplementedError

	def getSnapshots(self, since=None, keys=None, prefix=None):
		raise NotImplementedError

	def setSnapshot(self, key, seq, value):
		raise NotImplementedError

	def removeEntries(self, before=None, key=None):
		raise NotImplementedError

	def clear(self):
		raise NotImplementedError


class MemoryJournalPersistence(JournalPersistence):
	"""In-memory journal persistence used by default."""

	def __init__(self):
		self.cursor = 0
		self.entries = []
		self.snapshots = {}

	def nextSeq(self) -> int:
		self.cursor += 1
		return self.cursor

	def append(self, entry: dict) -> dict:
		self.entries.append(entry)
		self.cursor = max(self.cursor, entry.get("seq", 0))
		return entry

	def getCursor(self) -> int:
		return self.cursor

	def getEntries(self, since=None, limit=None, keys=None, prefix=None):
		keys = self._normalizeKeys(keys)
		res = []
		for entry in self.entries:
			if since is not None and entry.get("seq", 0) <= since:
				continue
			if keys is not None and entry.get("key") not in keys:
				continue
			if prefix is not None and not str(entry.get("key", "")).startswith(prefix):
				continue
			res.append(entry)
			if limit is not None and len(res) >= limit:
				break
		return res

	def getChangedKeys(self, since=None, prefix=None):
		res = []
		seen = set()
		for entry in self.getEntries(since=since, prefix=prefix):
			key = entry.get("key")
			if key not in seen:
				seen.add(key)
				res.append(key)
		return res

	def countEntries(self, key=None) -> int:
		if key is None:
			return len(self.entries)
		return len([_ for _ in self.entries if _.get("key") == key])

	def getSnapshot(self, key):
		return self.snapshots.get(key)

	def getSnapshots(self, since=None, keys=None, prefix=None):
		keys = self._normalizeKeys(keys)
		res = []
		for key, snapshot in self.snapshots.items():
			if since is not None and snapshot.get("seq", 0) <= since:
				continue
			if keys is not None and key not in keys:
				continue
			if prefix is not None and not str(key).startswith(prefix):
				continue
			res.append(snapshot)
		return sorted(res, key=lambda _: _.get("seq", 0))

	def setSnapshot(self, key, seq, value):
		snapshot = {"key": key, "seq": seq, "value": value}
		self.snapshots[key] = snapshot
		return snapshot

	def removeEntries(self, before=None, key=None):
		removed = []
		kept = []
		for entry in self.entries:
			if key is not None and entry.get("key") != key:
				kept.append(entry)
				continue
			if before is not None and entry.get("seq", 0) >= before:
				kept.append(entry)
				continue
			removed.append(entry)
		self.entries = kept
		return removed

	def clear(self):
		self.cursor = 0
		self.entries = []
		self.snapshots = {}
		return self

	def _normalizeKeys(self, keys):
		if keys is None:
			return None
		elif isinstance(keys, (tuple, list, set)):
			return set(keys)
		else:
			return {keys}


class JournalBackend(StorageBackend):
	"""Backend wrapper that records JSON-encodable changes."""

	def __init__(
		self,
		backend: StorageBackend,
		persistence: JournalPersistence | None = None,
		maxEntries: int | None = 10000,
		maxEntriesPerKey: int | None = 100,
		maxApproxBytes: int | None = None,
		snapshotEvery: int | None = None,
	):
		super().__init__()
		self.backend = backend
		self.persistence = persistence or MemoryJournalPersistence()
		self.maxEntries = maxEntries
		self.maxEntriesPerKey = maxEntriesPerKey
		self.maxApproxBytes = maxApproxBytes
		self.snapshotEvery = snapshotEvery
		self.compactedBefore = 0
		self.compactedKeys = {}
		self.HAS_READ = backend.HAS_READ
		self.HAS_WRITE = backend.HAS_WRITE
		self.HAS_STREAM = backend.HAS_STREAM
		self.HAS_FILE = backend.HAS_FILE
		self.HAS_RAW = backend.HAS_RAW
		self.HAS_ORDERING = backend.HAS_ORDERING
		self._deferredBatches = []

	def beginBatch(self):
		batch = dict(entries=[])
		self._deferredBatches.append(batch)
		return batch

	def endBatch(self, batch=None):
		if not self._deferredBatches:
			return None
		current = self._deferredBatches.pop()
		if batch is not None and current is not batch:
			raise ValueError("Journal batch mismatch")
		if self._deferredBatches:
			self._deferredBatches[-1]["entries"].extend(current.get("entries") or ())
			return current
		self._notifyBatch(current)
		return current

	def add(self, key, data):
		old = self.backend.get(key) if self.backend.has(key) else None
		self.backend.add(key, data)
		self._record(Operation.ADD if old is None else Operation.UPDATE, key, old, data)
		return self

	def update(self, key, data):
		old = self.backend.get(key) if self.backend.has(key) else None
		self.backend.update(key, data)
		self._record(Operation.UPDATE if old is not None else Operation.ADD, key, old, data)
		return self

	def remove(self, key):
		old = self.backend.get(key) if self.backend.has(key) else None
		self.backend.remove(key)
		self._record(Operation.REMOVE, key, old, None)
		return self

	def process(self, operation: Operation, key: str, data=None):
		if operation is Operation.ADD:
			return self.add(key, data)
		elif operation is Operation.UPDATE:
			return self.update(key, data)
		elif operation is Operation.REMOVE:
			return self.remove(key)
		elif operation is Operation.SAVE_RAW:
			return self.saveRawData(key, data)
		else:
			raise NotImplementedError

	def clear(self):
		self.backend.clear()
		self.persistence.clear()
		self.compactedBefore = 0
		self.compactedKeys = {}
		return self

	def sync(self):
		return self.backend.sync()

	def has(self, key):
		return self.backend.has(key)

	def get(self, key):
		return self.backend.get(key)

	def list(self, key=None):
		return self.backend.list(key)

	def count(self, key=None):
		return self.backend.count(key)

	def keys(self, collection=None, order=StorageBackend.ORDER_NONE):
		return self.backend.keys(collection, order)

	def path(self, key):
		return self.backend.path(key)

	def getMetadata(self, key=None, default=None):
		return self.backend.getMetadata(key, default)

	def setMetadata(self, key, value):
		return self.backend.setMetadata(key, value)

	def removeMetadata(self, key):
		return self.backend.removeMetadata(key)

	def stream(self, key, size=None):
		return self.backend.stream(key, size)

	def hasRawData(self, key, ext=None):
		return self.backend.hasRawData(key, ext)

	def saveRawData(self, key, data, ext=None):
		meta = self._rawMeta(data, ext)
		self.backend.saveRawData(key, data, ext)
		self._recordRaw(key, meta)
		return self

	def streamRawData(self, key, size=None, ext=None):
		return self.backend.streamRawData(key, size, ext)

	def getRawDataPath(self, key, ext=None):
		return self.backend.getRawDataPath(key, ext)

	def getCursor(self) -> int:
		return self.persistence.getCursor()

	def getChanges(self, since=None, limit=None, keys=None, prefix=None):
		return self.persistence.getEntries(since, limit, keys, prefix)

	def getChangedKeys(self, since=None, prefix=None):
		return self.persistence.getChangedKeys(since, prefix)

	def getUpdate(self, since=None, keys=None, prefix=None, limit=None):
		changes = self.getChanges(since=since, limit=limit, keys=keys, prefix=prefix)
		compacted = since is not None and since < self.compactedBefore
		snapshots = self.persistence.getSnapshots(since, keys, prefix) if compacted else []
		changed = self._changedKeys(changes, snapshots)
		return {
			"from": since,
			"cursor": self.getCursor(),
			"compacted": compacted,
			"snapshots": snapshots,
			"changes": changes,
			"changed": changed,
		}

	def compact(self, key=None, force=False):
		if key is not None:
			return self._compactKey(key, force=force)
		keys = set(_.get("key") for _ in self.persistence.getEntries())
		return [self._compactKey(_, force=force) for _ in keys if _ is not None]

	def _record(self, operation: Operation, key, old, new):
		seq = self.persistence.nextSeq()
		entry = {
			"seq": seq,
			"time": getTimestamp(),
			"operation": operation.value,
			"key": key,
			"kind": self._kind(new if new is not None else old),
			"patch": self._patch(old, new),
			"meta": self._meta(new if new is not None else old),
		}
		relations = self._relations(old, new)
		if relations:
			entry["relations"] = relations
		self.persistence.append(entry)
		self._deferOrNotify(entry)
		self._compactIfNeeded(key)
		return entry

	def _recordRaw(self, key, meta):
		seq = self.persistence.nextSeq()
		entry = {
			"seq": seq,
			"time": getTimestamp(),
			"operation": Operation.SAVE_RAW.value,
			"key": key,
			"kind": "raw",
			"patch": [],
			"meta": meta,
		}
		self.persistence.append(entry)
		self._deferOrNotify(entry)
		self._compactIfNeeded(key)
		return entry

	def _deferOrNotify(self, entry):
		if self._deferredBatches:
			self._deferredBatches[-1]["entries"].append(entry)
		else:
			self._notifyJournal(entry)

	def _notifyJournal(self, entry):
		operation = entry.get("operation")
		key = entry.get("key")
		for sub_key, callbacks in list(self._subscribers.items()):
			if self._subscriptionMatches(sub_key, key):
				for callback in list(callbacks):
					callback(key, operation, entry)
		self.publish(operation, key, entry)

	def _notifyBatch(self, batch):
		entries = [entry for entry in batch.get("entries") or [] if entry]
		if not entries:
			return batch
		payload = {
			"entries": entries,
			"count": len(entries),
			"changed": self._changedKeys(entries, []),
			"from": entries[0].get("seq"),
			"to": entries[-1].get("seq"),
		}
		for sub_key, callbacks in list(self._subscribers.items()):
			matches = [entry for entry in entries if self._subscriptionMatches(sub_key, entry.get("key"))]
			if not matches:
				continue
			sub_payload = dict(payload)
			sub_payload["entries"] = matches
			sub_payload["count"] = len(matches)
			sub_payload["changed"] = self._changedKeys(matches, [])
			sub_payload["from"] = matches[0].get("seq")
			sub_payload["to"] = matches[-1].get("seq")
			for callback in list(callbacks):
				callback(None, "batch", sub_payload)
		self.publish("batch", None, payload)
		return payload

	def _subscriptionMatches(self, sub_key, key):
		return sub_key in (None, "*") or sub_key == key or str(key).startswith(str(sub_key))

	def _compactIfNeeded(self, key):
		if self.snapshotEvery and self.persistence.countEntries(key) % self.snapshotEvery == 0:
			self._compactKey(key)
		if self.maxEntriesPerKey is not None and self.persistence.countEntries(key) > self.maxEntriesPerKey:
			self._compactKey(key)
		if self.maxEntries is not None and self.persistence.countEntries() > self.maxEntries:
			entries = self.persistence.getEntries()
			if entries:
				self._compactKey(entries[0].get("key"))
		if self.maxApproxBytes is not None:
			while self._approxBytes() > self.maxApproxBytes:
				entries = self.persistence.getEntries()
				if not entries:
					break
				self._compactKey(entries[0].get("key"))

	def _compactKey(self, key, force=False):
		entries = self.persistence.getEntries(keys=key)
		if not force and not entries:
			return None
		seq = self.getCursor()
		value = self.backend.get(key) if self.backend.has(key) else None
		snapshot = self.persistence.setSnapshot(key, seq, value)
		removed = self.persistence.removeEntries(before=seq, key=key)
		if removed:
			before = max(_.get("seq", 0) for _ in removed) + 1
			self.compactedKeys[key] = before
			self.compactedBefore = max(self.compactedBefore, before)
		return snapshot

	def _approxBytes(self):
		return sum(len(json.dumps(_, sort_keys=True)) for _ in self.persistence.getEntries())

	def _changedKeys(self, changes, snapshots):
		res = []
		seen = set()
		for entry in list(snapshots) + list(changes):
			key = entry.get("key")
			if key not in seen:
				seen.add(key)
				res.append(key)
		return res

	def _patch(self, old, new, path=""):
		if old == new:
			return []
		if old is None:
			return [{"op": "add", "path": path, "value": new}]
		if new is None:
			return [{"op": "remove", "path": path}]
		if isinstance(old, dict) and isinstance(new, dict):
			patch = []
			for key in sorted(set(old.keys()) | set(new.keys())):
				child = path + "/" + self._escapePath(key)
				if key not in old:
					patch.append({"op": "add", "path": child, "value": new[key]})
				elif key not in new:
					patch.append({"op": "remove", "path": child})
				else:
					patch.extend(self._patch(old[key], new[key], child))
			return patch
		if isinstance(old, list) and isinstance(new, list):
			return [{"op": "replace", "path": path, "value": new}]
		return [{"op": "replace", "path": path, "value": new}]

	def _escapePath(self, value):
		return str(value).replace("~", "~0").replace("/", "~1")

	def _kind(self, value):
		if isinstance(value, dict) and "id" in value and "type" in value:
			return "object"
		else:
			return "value"

	def _meta(self, value):
		if isinstance(value, dict):
			return {
				"objectID": value.get("id"),
				"objectType": value.get("type"),
				"revision": value.get("revision") or value.get("updates"),
			}
		else:
			return {}

	def _relations(self, old, new):
		if not isinstance(old, dict) or not isinstance(new, dict):
			return None
		res = {}
		for key in set(old.keys()) | set(new.keys()):
			old_refs = self._refs(old.get(key))
			new_refs = self._refs(new.get(key))
			if old_refs is None or new_refs is None:
				continue
			old_ids = set(old_refs.keys())
			new_ids = set(new_refs.keys())
			added = [new_refs[_] for _ in sorted(new_ids - old_ids)]
			removed = [old_refs[_] for _ in sorted(old_ids - new_ids)]
			if added or removed:
				res[key] = {"added": added, "removed": removed}
		return res or None

	def _refs(self, value):
		if value is None:
			return {}
		if not isinstance(value, list):
			return None
		res = {}
		for item in value:
			if not (isinstance(item, dict) and "id" in item and "type" in item):
				return None
			ref = {"id": item["id"], "type": item["type"]}
			res[(str(ref["type"]), str(ref["id"]))] = ref
		return res

	def _rawMeta(self, data, ext):
		content = self._rawContent(data)
		if content is None:
			return {"ext": ext, "size": None, "sha256": None, "changed": True}
		if isinstance(content, str):
			content = content.encode("utf-8")
		return {
			"ext": ext,
			"size": len(content),
			"sha256": hashlib.sha256(content).hexdigest(),
			"changed": True,
		}

	def _rawContent(self, data):
		if data is None:
			return b""
		if isinstance(data, (bytes, str)):
			return data
		if hasattr(data, "tell") and hasattr(data, "seek") and hasattr(data, "read"):
			try:
				pos = data.tell()
				content = data.read()
				data.seek(pos)
				return content
			except Exception:
				return None
		return None


__all__ = [
	"JournalBackend",
	"JournalPersistence",
	"MemoryJournalPersistence",
]


# EOF
