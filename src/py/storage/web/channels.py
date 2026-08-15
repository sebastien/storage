"""In-memory storage channels for server-sent events."""

import asyncio
import json
import time
from uuid import uuid4

from .errors import StorageWebError

__all__ = ["StorageChannel"]


class StorageChannel:
	"""In-memory SSE channel backed by journal subscriptions."""

	def __init__(self, server):
		self.server = server
		self.id = uuid4().hex
		self.queue = asyncio.Queue(maxsize=server.CHANNEL_QUEUE_SIZE)
		self.subscriptions = {}
		self.closed = False
		self.attached = False
		self.lastSeen = time.time()
		self.blocked = False
		self.pendingEvents = []
		try:
			self.loop = asyncio.get_running_loop()
		except RuntimeError:
			self.loop = None

	def touch(self):
		self.lastSeen = time.time()
		return self

	def attach(self):
		self.attached = True
		self.touch()
		return self

	def detach(self):
		self.attached = False
		self.touch()
		return self

	def command(self, command):
		if not isinstance(command, dict):
			return StorageWebError(
				"BADITEM",
				"Invalid storage channel command.",
				"Each storage channel command must be an object.",
				received=self.server.describeValue(command),
				expected='A channel command object such as {"op":"subscribe","target":{...}}.',
			).payload(dict(operation="channel"))
		op = command.get("op")
		if op == "subscribe":
			return self.subscribe(command.get("target"), snapshot=bool(command.get("snapshot")))
		elif op == "unsubscribe":
			return self.unsubscribe(command.get("target"))
		elif op == "heartbeat":
			self.touch()
			return dict(ok=True, op=op)
		elif op == "block":
			self.blocked = True
			self.touch()
			return dict(ok=True, op=op, blocked=True)
		elif op == "flush":
			self.touch()
			return dict(ok=True, op=op, blocked=True, batch=self.flushPending())
		elif op == "unblock":
			batch = self.flushPending()
			self.blocked = False
			self.touch()
			return dict(ok=True, op=op, blocked=False, batch=batch)
		elif op == "close":
			self.close()
			return dict(ok=True, op=op)
		else:
			return StorageWebError(
				"BADOP",
				"Unsupported storage channel command.",
				"The storage channel command operation is not supported: %s." % op,
				received=dict(op=op),
				expected='Supported channel operations: "subscribe", "unsubscribe", "heartbeat", "block", "flush", "unblock", "close".',
			).payload(dict(operation="channel"))

	def subscribe(self, target, snapshot=False):
		resolved, error = self.server.resolveJournalTarget(target)
		if error:
			return error.payload(dict(operation="channel", op="subscribe"))
		sub_id = self.subscriptionID(resolved)
		if sub_id in self.subscriptions:
			if snapshot:
				self.enqueueSnapshot(self.subscriptions[sub_id])
			return dict(ok=True, op="subscribe", target=target, duplicate=True)

		def callback(key, operation, entry):
			self.notify(resolved, key, operation, entry)

		resolved["callback"] = callback
		resolved["backend"].subscribe(resolved["key"], callback)
		self.subscriptions[sub_id] = resolved
		if snapshot:
			self.enqueueSnapshot(resolved)
		return dict(ok=True, op="subscribe", target=target)

	def unsubscribe(self, target):
		resolved, error = self.server.resolveJournalTarget(target)
		if error:
			return error.payload(dict(operation="channel", op="unsubscribe"))
		sub_id = self.subscriptionID(resolved)
		stored = self.subscriptions.pop(sub_id, None)
		if stored:
			stored["backend"].unsubscribe(stored["key"], stored["callback"])
		return dict(ok=True, op="unsubscribe", target=target)

	def subscriptionID(self, resolved):
		target = resolved.get("target") or {}
		return "%s:%s:%s" % (id(resolved.get("backend")), resolved.get("key"), json.dumps(target, sort_keys=True))

	def notify(self, resolved, key, operation, entry):
		if operation == "batch":
			events = self.batchEntries(resolved, entry.get("entries") or [])
			if not events:
				return
			if self.blocked:
				self.pendingEvents.extend(events)
			else:
				self.enqueueBatch(events)
			return
		target = resolved.get("target") or {}
		if target.get("kind") == "query":
			query = resolved.get("query")
			data = query.eventFor(entry, resolved.get("backend")) if query else None
			if not data:
				return
			if self.blocked:
				self.pendingEvents.append(data)
			else:
				self.enqueue(data.get("event", "query"), data, id=data.get("seq"))
			return
		if target.get("kind") == "relation":
			relations = entry.get("relations") or {}
			if target.get("name") not in relations:
				return
		data = self.server.journalEvent(entry)
		backend = resolved.get("backend")
		if operation != "-" and backend and backend.has(key):
			data["value"] = backend.get(key)
		data["target"] = target
		if self.blocked:
			self.pendingEvents.append(data)
		else:
			self.enqueue(data.get("event", "update"), data, id=data.get("seq"))

	def batchEntries(self, resolved, entries):
		res = []
		query = resolved.get("query")
		for entry in entries:
			key = entry.get("key")
			operation = entry.get("operation")
			target = resolved.get("target") or {}
			if target.get("kind") == "query":
				data = query.eventFor(entry, resolved.get("backend")) if query else None
				if data:
					res.append(data)
				continue
			if target.get("kind") == "relation":
				relations = entry.get("relations") or {}
				if target.get("name") not in relations:
					continue
			data = self.server.journalEvent(entry)
			backend = resolved.get("backend")
			if operation != "-" and backend and key is not None and backend.has(key):
				data["value"] = backend.get(key)
			data["target"] = target
			res.append(data)
		return res

	def enqueueSnapshot(self, resolved):
		query = resolved.get("query")
		backend = resolved.get("backend")
		if not query or not backend or not hasattr(backend, "getCursor"):
			return self
		payload = query.snapshot(cursor=backend.getCursor())
		self.enqueue("snapshot", payload, id=payload.get("cursor"))
		return self

	def flushPending(self):
		if not self.pendingEvents:
			return dict(count=0, changed=[], events=[])
		events = self.pendingEvents
		self.pendingEvents = []
		self.enqueueBatch(events)
		return self.batchPayload(events)

	def enqueueBatch(self, events):
		if not events:
			return self
		payload = self.batchPayload(events)
		self.enqueue("batch", payload, id=payload.get("to"))
		return self

	def batchPayload(self, events):
		changed = []
		seen = set()
		for event in events:
			key = event.get("key")
			if key and key not in seen:
				seen.add(key)
				changed.append(key)
		seqs = [event.get("seq") for event in events if event.get("seq") is not None]
		return {
			"count": len(events),
			"changed": changed,
			"from": seqs[0] if seqs else None,
			"to": seqs[-1] if seqs else None,
			"events": events,
		}

	def enqueue(self, event, data=None, id=None):
		if self.closed:
			return self
		item = (event, data or {}, id)
		def put():
			if self.closed:
				return
			if self.queue.full():
				try:
					self.queue.get_nowait()
				except asyncio.QueueEmpty:
					pass
			self.queue.put_nowait(item)
		if self.loop and self.loop.is_running():
			self.loop.call_soon_threadsafe(put)
		else:
			put()
		return self

	async def stream(self):
		self.attach()
		yield self.server.formatSSE("ready", dict(id=self.id))
		try:
			while not self.closed:
				try:
					event, data, id = await asyncio.wait_for(
						self.queue.get(), timeout=self.server.CHANNEL_HEARTBEAT
					)
				except asyncio.TimeoutError:
					self.touch()
					yield self.server.formatSSE("ping", dict(time=time.time()))
					continue
				yield self.server.formatSSE(event, data, id=id)
		finally:
			self.detach()

	def close(self):
		if self.closed:
			return self
		if self.pendingEvents:
			self.flushPending()
		self.closed = True
		for sub in list(self.subscriptions.values()):
			sub["backend"].unsubscribe(sub["key"], sub["callback"])
		self.subscriptions.clear()
		self.enqueue("close", dict(id=self.id))
		return self
