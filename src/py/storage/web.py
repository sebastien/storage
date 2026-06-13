import asyncio
import json
import time
import types
import sys
from pathlib import Path
from typing import Iterable
from urllib.parse import unquote
from uuid import uuid4

try:
	from extra import HTTPRequest, HTTPResponse, Service
	from extra.routing import Handler
except ImportError:
	extra_path = Path(__file__).resolve().parents[3] / "deps" / "extra" / "src" / "py"
	if str(extra_path) not in sys.path:
		sys.path.insert(0, str(extra_path))
	from extra import HTTPRequest, HTTPResponse, Service
	from extra.routing import Handler

from .core import Storable, getCanonicalName, isSame, restore
from .raw import StoredRaw


# FIXME: It seems that sometimes when one element is sent as a field value
# (like shootingback.model.Clip.tags=["Youth"], only "Youth" is stored
# instead of ["Youth"]. Might be in objects or JSON conversion.


def http(url, restrict=None, methods=None, contentType=None, export=None):
	"""Adds HTTP exposure information to a storable class or method."""

	def wrapper(storableClassOrFunction):
		if type(storableClassOrFunction) is types.FunctionType:
			setattr(
				storableClassOrFunction,
				StorageDecoration.KEY_FUNCTION,
				(url, restrict, methods, contentType),
			)
		else:
			storable = storableClassOrFunction
			s = StorageDecoration(storable, url, restrict, methods, export)
			setattr(storable, StorageDecoration.KEY, s)
		return storableClassOrFunction

	return wrapper


class StorageDecoration:
	"""Stores HTTP exposure metadata on storable classes."""

	KEY = "_storage_web_StorageDecoration"
	KEY_FUNCTION = "_storage_web_StorageDecoration_Function"

	@classmethod
	def Get(cls, storableClass):
		return getattr(storableClass, cls.KEY)

	@classmethod
	def Has(cls, storableClass):
		return hasattr(storableClass, cls.KEY)

	def __init__(self, storableClass, url, restrict=None, methods=None, export=None):
		assert issubclass(storableClass, Storable), (
			"Storable class requires a Storable object"
		)
		self.storable = storableClass
		self.url = url
		self.restrict = restrict
		self.httpMethods = [methods] if isinstance(methods, str) else methods
		if type(export) in (str, str):
			export = dict(profile=export)
		self.export = export or {}
		self.export["target"] = "web"

	def listInvocables(self, storable=None):
		storable = storable or self.storable
		for name in dir(storable):
			value = getattr(storable, name)
			if hasattr(value, self.KEY_FUNCTION):
				meta_data = getattr(value, self.KEY_FUNCTION)
				yield (name, meta_data)

	def getName(self):
		return self.storable.__name__.split(".")[-1].lower()

	def getExportOptions(self):
		return self.export

	def __repr__(self):
		return "@storage.web:%s(url=%s,storable=%s,methods=%s,restrict=%s)" % (
			self.getName(),
			self.url,
			self.storable,
			self.httpMethods,
			self.restrict,
		)


class StorageWebError(Exception):
	"""Structured web API error with a compact reportable code."""

	def __init__(self, errno, error, problem, received=None, expected=None, status=400):
		Exception.__init__(self, error)
		self.errno = errno
		self.error = error
		self.problem = problem
		self.received = received
		self.expected = expected
		self.status = status

	def payload(self, context=None):
		res = dict(
			ok=False,
			errno=self.errno,
			error=self.error,
			problem=self.problem,
			expected=self.expected,
			status=self.status,
		)
		if self.received is not None:
			res["received"] = self.received
		if context:
			res["context"] = context
		return res


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
			return self.subscribe(command.get("target"))
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

	def subscribe(self, target):
		resolved, error = self.server.resolveJournalTarget(target)
		if error:
			return error.payload(dict(operation="channel", op="subscribe"))
		sub_id = self.subscriptionID(resolved)
		if sub_id in self.subscriptions:
			return dict(ok=True, op="subscribe", target=target, duplicate=True)

		def callback(key, operation, entry):
			self.notify(resolved, key, operation, entry)

		resolved["callback"] = callback
		resolved["backend"].subscribe(resolved["key"], callback)
		self.subscriptions[sub_id] = resolved
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
		for entry in entries:
			key = entry.get("key")
			operation = entry.get("operation")
			target = resolved.get("target") or {}
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


class StorageServer(Service):
	"""An Extra service that exposes storables through a REST-like API."""

	LIST_COUNT = 20
	CHANNEL_HEARTBEAT = 15
	CHANNEL_TTL = 45
	CHANNEL_QUEUE_SIZE = 1000

	def __init__(self, prefix="/api", classes=None, readonly=False):
		prefix = prefix.strip("/")
		prefix = prefix + "/" if prefix else ""
		Service.__init__(self, prefix=prefix)
		self.storableClasses = []
		self.readonly = readonly
		self.channels = {}
		if classes:
			self.add(*classes)

	def use(self, *storableClasses):
		"""Alias for `add`."""
		return self.add(*storableClasses)

	def add(self, *storableClasses):
		"""Registers decorated storable classes for HTTP exposure."""
		for s in storableClasses:
			info = getattr(s, StorageDecoration.KEY)
			assert info, "Storable class must be decorated with StorageDecoration"
			assert isinstance(info, StorageDecoration), (
				"Storable information should be StorageDecoration"
			)
			self.storableClasses.append(s)
		self._handlers = None
		return self

	def iterHandlers(self) -> Iterable[Handler]:
		async def handler_commands(request: HTTPRequest) -> HTTPResponse:
			return await self.onCommands(request)

		async def handler_channel_create(request: HTTPRequest) -> HTTPResponse:
			return await self.onChannelCreate(request)

		def handler_channel_events(request: HTTPRequest, cid: str) -> HTTPResponse:
			return self.onChannelEvents(request, cid)

		async def handler_channel_commands(request: HTTPRequest, cid: str) -> HTTPResponse:
			return await self.onChannelCommands(request, cid)

		def handler_channel_heartbeat(request: HTTPRequest, cid: str) -> HTTPResponse:
			return self.onChannelHeartbeat(request, cid)

		def handler_channel_close(request: HTTPRequest, cid: str) -> HTTPResponse:
			return self.onChannelClose(request, cid)

		yield self._handler(handler_commands, ("POST", "commands"))
		yield self._handler(handler_channel_create, ("POST", "channel"))
		yield self._handler(handler_channel_events, ("GET", "channel/{cid:segment}/events"))
		yield self._handler(handler_channel_commands, ("POST", "channel/{cid:segment}/commands"))
		yield self._handler(handler_channel_heartbeat, ("POST", "channel/{cid:segment}/heartbeat"))
		yield self._handler(handler_channel_close, ("POST", "channel/{cid:segment}/close"))
		for storableClass in self.storableClasses:
			yield from self._iterHandlers(storableClass)

	async def create(self, request, storableClass):
		if self.readonly:
			return request.notAuthorized()
		info = StorageDecoration.Get(storableClass)
		return await self.onStorableCreate(storableClass, info, request)

	def remove(self, request, storableClass, sid):
		if self.readonly:
			return request.notAuthorized()
		info = StorageDecoration.Get(storableClass)
		return self.onStorableRemove(storableClass, info, request, sid)

	async def update(self, request, storableClass, sid):
		if self.readonly:
			return request.notAuthorized()
		info = StorageDecoration.Get(storableClass)
		return await self.onStorableUpdate(storableClass, info, request, sid)

	def get(self, request, storableClass, sid):
		info = StorageDecoration.Get(storableClass)
		return self.onStorableGet(storableClass, info, request, sid)

	async def onStorableCreate(self, storableClass, info, request):
		if self.readonly:
			return request.notAuthorized()
		data = await request.loadData()
		if data is not None:
			storable = storableClass.Import(data).save()
		else:
			storable = storableClass()
		return request.returns(storable.export(**info.getExportOptions()))

	async def onStorableUpdate(self, storableClass, info, request, sid):
		sid = unquote(sid)
		if self.readonly:
			return request.notAuthorized()
		try:
			data = await request.loadParams()
			self.validateUpdatePayload(data, dict(type=info.getName(), id=sid))
			storable = self.applyStorableUpdate(storableClass, sid, data)
		except StorageWebError as error:
			return self.storageError(request, error, dict(type=info.getName(), id=sid))
		except ValueError as error:
			return self.storageError(
				request,
				StorageWebError(
					"BADPAYLOAD",
					"Invalid storage update payload.",
					str(error),
					received=self.describeRequest(request),
					expected="A JSON or form object with non-empty storage field names.",
				),
				dict(type=info.getName(), id=sid),
			)
		return request.returns(storable.export(**info.getExportOptions()))

	def applyStorableUpdate(self, storableClass, sid, data):
		data = dict(data or {})
		storable = storableClass.Get(sid)
		if not storable:
			if "id" not in data:
				data["id"] = sid
			storable = storableClass.Import(data)
			storable.save()
		else:
			storable.update(data)
			storable.save()
		return storable

	async def onCommands(self, request):
		if self.readonly:
			return request.notAuthorized()
		try:
			data = await request.loadData()
		except ValueError as error:
			return self.storageError(
				request,
				StorageWebError(
					"BADPAYLOAD",
					"Invalid storage commands payload.",
					str(error),
					received=self.describeRequest(request),
					expected='A JSON object with a "commands" list.',
				),
				dict(operation="commands"),
			)
		commands = data.get("commands") if isinstance(data, dict) else None
		transaction = bool(data.get("transaction")) if isinstance(data, dict) else False
		if not isinstance(commands, list):
			return self.storageError(
				request,
				StorageWebError(
					"BADLIST",
					"Invalid storage commands list.",
					'The storage commands payload must contain a "commands" list.',
					received=self.describeValue(data),
					expected='A JSON object such as {"commands":[{"op":"update",...}]}.',
				),
				dict(operation="commands"),
			)
		results = []
		batches = []
		try:
			if transaction:
				for backend in self.iterJournalBackends():
					batches.append((backend, backend.beginBatch()))
			for index, command in enumerate(commands):
				result = await self.onCommand(command, index=index)
				results.append(result)
		finally:
			for backend, batch in reversed(batches):
				backend.endBatch(batch)
		res = dict(results=results)
		if transaction:
			res["transaction"] = True
		return request.returns(res)

	async def onCommand(self, command, index=None):
		if not isinstance(command, dict):
			return StorageWebError(
				"BADITEM",
				"Invalid storage command.",
				"Each storage command must be an object.",
				received=self.describeValue(command),
				expected='A command object such as {"op":"update","type":"items","id":"...","fields":{...}}.',
			).payload(dict(operation="commands", index=index))
		op = command.get("op")
		try:
			if op == "create":
				return self.onCreateCommand(command, index=index)
			elif op == "update":
				return self.onUpdateCommand(command, index=index)
			elif op == "remove":
				return self.onRemoveCommand(command, index=index)
			elif isinstance(op, str) and op.startswith("relation."):
				return self.onRelationCommand(command, index=index)
			elif op == "invoke":
				return self.onInvokeCommand(command, index=index)
			else:
				return StorageWebError(
					"BADOP",
					"Unsupported storage command.",
					"The storage command operation is not supported: %s." % op,
					received=dict(op=op),
					expected='Supported command operations: "create", "update", "remove", "relation.*", "invoke".',
				).payload(dict(operation="commands", index=index))
		except StorageWebError as error:
			return error.payload(dict(operation="commands", index=index, op=op))
		except Exception as error:
			return StorageWebError(
				"INTERNAL",
				"Unexpected storage command error.",
				str(error),
				received=dict(op=op, type=command.get("type"), id=command.get("id")),
				expected="The command should complete without an internal server error.",
				status=500,
			).payload(dict(operation="commands", index=index, op=op))

	def onCreateCommand(self, command, index=None):
		match = self.commandMatch(command.get("type"))
		storableClass, info = match
		fields = command.get("fields")
		if fields is not None:
			self.validateUpdatePayload(fields, dict(type=info.getName(), index=index))
			storable = storableClass.Import(fields).save()
		else:
			storable = storableClass()
		return dict(
			ok=True,
			op="create",
			type=info.getName(),
			id=str(storable.id),
			value=storable.export(**info.getExportOptions()),
		)

	def onUpdateCommand(self, command, index=None):
		command_type = command.get("type")
		sid = command.get("id")
		fields = command.get("fields", {})
		match = self.commandMatch(command_type)
		if sid is None:
			raise StorageWebError(
				"NOID",
				"Storage command id is required.",
				"The update command does not identify which object to update.",
				received=dict(id=sid),
				expected='An "id" string in the update command.',
			)
		self.validateUpdatePayload(fields, dict(type=command_type, id=sid, index=index))
		storableClass, info = match
		storable = self.applyStorableUpdate(storableClass, str(sid), fields)
		return dict(
			ok=True,
			op="update",
			type=command_type,
			id=str(sid),
			value=storable.export(**info.getExportOptions()),
		)

	def onRemoveCommand(self, command, index=None):
		match = self.commandMatch(command.get("type"))
		sid = command.get("id")
		if sid is None:
			raise StorageWebError(
				"NOID",
				"Storage command id is required.",
				"The remove command does not identify which object to remove.",
				received=dict(id=sid),
				expected='An "id" string in the remove command.',
			)
		storableClass, info = match
		storable = storableClass.Get(str(sid))
		if not storable:
			raise StorageWebError(
				"NOTFOUND",
				"Storage object not found.",
				"The remove command does not reference an existing object.",
				received=dict(type=info.getName(), id=str(sid)),
				expected="An existing storage object.",
				status=404,
			)
		storable.remove()
		return dict(ok=True, op="remove", type=info.getName(), id=str(sid), value=True)

	def onRelationCommand(self, command, index=None):
		op = str(command.get("op") or "")
		operation = op.split(".", 1)[1] if "." in op else ""
		match = self.commandMatch(command.get("type"))
		sid = command.get("id")
		name = command.get("relation")
		if sid is None:
			raise StorageWebError(
				"NOID",
				"Storage command id is required.",
				"The relation command does not identify which object to update.",
				received=dict(id=sid),
				expected='An "id" string in the relation command.',
			)
		if not isinstance(name, str) or not name:
			raise StorageWebError(
				"BADRELATION",
				"Storage relation not found.",
				"The relation command must identify which relation to mutate.",
				received=dict(relation=name),
				expected='A relation name in the "relation" field.',
				status=404,
			)
		storableClass, info = match
		storable = storableClass.Get(str(sid))
		if not storable:
			raise StorageWebError(
				"NOTFOUND",
				"Storage object not found.",
				"The relation command does not reference an existing object.",
				received=dict(type=info.getName(), id=str(sid)),
				expected="An existing storage object.",
				status=404,
			)
		relation = self.getStorableRelation(storable, name)
		data = self.commandPayload(command, exclude=("op", "type", "id", "relation", "return"))
		self.validateRelationRevision(storable, name, data)
		changed = self.applyRelationOperation(relation, operation, data)
		if changed:
			storable.save()
		return_mode = command.get("return")
		if return_mode == "none":
			return dict(ok=True, operation=operation)
			
		if return_mode == "object":
			return dict(
				ok=True,
				op=op,
				type=info.getName(),
				id=str(sid),
				value=storable.export(**info.getExportOptions()),
			)
		page = self.relationPageData(storable, info, relation, name, command, operation=operation)
		page["op"] = op
		return page

	def onInvokeCommand(self, command, index=None):
		match = self.commandMatch(command.get("type"))
		sid = command.get("id")
		method_name = command.get("method")
		if sid is None:
			raise StorageWebError(
				"NOID",
				"Storage command id is required.",
				"The invoke command does not identify which object to invoke.",
				received=dict(id=sid),
				expected='An "id" string in the invoke command.',
			)
		if not isinstance(method_name, str) or not method_name:
			raise StorageWebError(
				"BADMETHOD",
				"Storage method not found.",
				"The invoke command must identify which POST method to invoke.",
				received=dict(method=method_name),
				expected='A method name or route in the "method" field.',
				status=404,
			)
		storableClass, info = match
		storable = storableClass.Get(str(sid))
		if not storable:
			raise StorageWebError(
				"NOTFOUND",
				"Storage object not found.",
				"The invoke command does not reference an existing object.",
				received=dict(type=info.getName(), id=str(sid)),
				expected="An existing storage object.",
				status=404,
			)
		handler_name, _content_type = self.resolveInvocable(info, method_name, method="POST")
		args, kwargs = self.invokeCommandArguments(command)
		result = getattr(storable, handler_name)(*args, **kwargs)
		return dict(
			ok=True,
			op="invoke",
			type=info.getName(),
			id=str(sid),
			method=method_name,
			value=result,
		)

	def commandMatch(self, command_type):
		match = self.resolveStorable(command_type)
		if not match:
			raise StorageWebError(
				"BADTYPE",
				"Storage type not found.",
				"No web storage type is registered for this command type.",
				received=dict(type=command_type),
				expected="A registered storage type or route name.",
				status=404,
			)
		return match

	def commandPayload(self, command, exclude=()):
		fields = command.get("fields")
		if fields is None:
			fields = {k: v for k, v in command.items() if k not in exclude}
		if not isinstance(fields, dict):
			raise StorageWebError(
				"BADPAYLOAD",
				"Invalid storage command payload.",
				"The storage command payload must be an object.",
				received=self.describeValue(fields),
				expected='A JSON object in "fields" or as top-level command fields.',
			)
		return dict(fields)

	def iterJournalBackends(self):
		seen = set()
		for storableClass in self.storableClasses:
			backend = self.journalBackend(storableClass)
			if backend and id(backend) not in seen and hasattr(backend, "beginBatch"):
				seen.add(id(backend))
				yield backend

	def resolveInvocable(self, info, method_name, method="POST"):
		for name, meta in info.listInvocables():
			invoke_url, _restrict, methods, contentType = meta
			invoke_url = invoke_url[1:] if invoke_url.startswith("/") else invoke_url
			allowed = (methods,) if isinstance(methods, str) else tuple(methods or ("GET", "POST"))
			allowed = tuple(_.upper() for _ in allowed)
			if method.upper() in allowed and method_name in (name, invoke_url):
				return name, contentType
		raise StorageWebError(
			"BADMETHOD",
			"Storage method not found.",
			"The requested storage method is not exposed for this command.",
			received=dict(method=method_name),
			expected="A POST-exposed storage method.",
			status=404,
		)

	def invokeCommandArguments(self, command):
		if "args" in command:
			args = command.get("args") or []
			if not isinstance(args, list):
				raise StorageWebError(
					"BADPAYLOAD",
					"Invalid storage method payload.",
					"The invoke command args payload must be a list.",
					received=self.describeValue(args),
					expected='A JSON list in the "args" field.',
				)
			kwargs = command.get("kwargs") or {}
		elif "kwargs" in command:
			args = []
			kwargs = command.get("kwargs") or {}
		else:
			body = command.get("body")
			if isinstance(body, list):
				args = body
				kwargs = {}
			elif isinstance(body, dict):
				args = []
				kwargs = body
			elif body is None:
				args = []
				kwargs = {}
			else:
				raise StorageWebError(
					"BADPAYLOAD",
					"Invalid storage method payload.",
					"The invoke command body must be a JSON object, list, or omitted.",
					received=self.describeValue(body),
					expected='A JSON object in "body", a JSON list in "body", or explicit "args"/"kwargs".',
				)
		if not isinstance(kwargs, dict):
			raise StorageWebError(
				"BADPAYLOAD",
				"Invalid storage method payload.",
				"The invoke command kwargs payload must be an object.",
				received=self.describeValue(kwargs),
				expected='A JSON object in the "kwargs" field.',
			)
		return [restore(_) for _ in args], dict((k, restore(v)) for k, v in kwargs.items())

	def validateUpdatePayload(self, data, context=None):
		if not isinstance(data, dict):
			raise StorageWebError(
				"BADPAYLOAD",
				"Invalid storage update payload.",
				"The storage update payload must be an object.",
				received=self.describeValue(data),
				expected='A JSON object such as {"title":"..."}.',
			)
		for name in data.keys():
			if not isinstance(name, str):
				raise StorageWebError(
					"BADKEY",
					"Invalid update field name.",
					"Every update field name must be a string.",
					received=dict(field=name, fieldType=type(name).__name__, context=context),
					expected="A string field name.",
				)
			if not name.strip():
				raise StorageWebError(
					"EMPTYKEY",
					"Empty update field name.",
					"The update payload contains an empty field name.",
					received=dict(field=name, keys=list(data.keys()), context=context),
					expected='A non-empty storage field name, for example "title".',
				)
		return data

	def storageError(self, request, error, context=None):
		return request.returns(
			error.payload(self.requestContext(request, context)),
			status=error.status,
		)

	def requestContext(self, request, context=None):
		res = self.describeRequest(request)
		if context:
			res.update(context)
		return res

	def describeRequest(self, request):
		return dict(
			method=getattr(request, "method", None),
			path=getattr(request, "path", None),
			contentType=getattr(request, "contentType", None),
		)

	def describeValue(self, value):
		if isinstance(value, dict):
			return dict(type="object", keys=list(value.keys()))
		elif isinstance(value, list):
			return dict(type="list", length=len(value))
		else:
			return dict(type=type(value).__name__, value=value)

	def resolveStorable(self, name):
		if name is None:
			return None
		name = str(name).strip("/")
		for storableClass in self.storableClasses:
			info = StorageDecoration.Get(storableClass)
			url = info.url or info.getName()
			url = url[1:] if url.startswith("/") else url
			if name in (url, info.getName(), storableClass.__name__):
				return storableClass, info
		return None

	async def onChannelCreate(self, request):
		self.expireChannels()
		channel = StorageChannel(self)
		self.channels[channel.id] = channel
		return request.returns(
			dict(
				id=channel.id,
				events="channel/%s/events" % channel.id,
				commands="channel/%s/commands" % channel.id,
				heartbeat="channel/%s/heartbeat" % channel.id,
				close="channel/%s/close" % channel.id,
			)
		)

	def onChannelEvents(self, request, cid):
		channel = self.getChannel(cid)
		if not channel:
			return request.notFound()

		def onClientClose(_):
			channel.detach()

		return request.onClose(onClientClose).respond(
			channel.stream(),
			contentType="text/event-stream",
			headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
		)

	async def onChannelCommands(self, request, cid):
		channel = self.getChannel(cid)
		if not channel:
			return request.notFound()
		try:
			data = await request.loadData()
		except ValueError as error:
			return self.storageError(
				request,
				StorageWebError(
					"BADPAYLOAD",
					"Invalid storage channel payload.",
					str(error),
					received=self.describeRequest(request),
					expected='A JSON object with a "commands" list.',
				),
				dict(operation="channel", id=cid),
			)
		commands = data.get("commands") if isinstance(data, dict) else None
		if not isinstance(commands, list):
			return self.storageError(
				request,
				StorageWebError(
					"BADLIST",
					"Invalid storage channel commands list.",
					'The storage channel payload must contain a "commands" list.',
					received=self.describeValue(data),
					expected='A JSON object such as {"commands":[{"op":"subscribe","target":{...}}]}.',
				),
				dict(operation="channel", id=cid),
			)
		results = []
		for command in commands:
			results.append(channel.command(command))
		return request.returns(dict(results=results))

	def onChannelHeartbeat(self, request, cid):
		channel = self.getChannel(cid)
		if not channel:
			return request.notFound()
		channel.touch()
		return request.returns(dict(ok=True, id=channel.id))

	def onChannelClose(self, request, cid):
		channel = self.channels.pop(cid, None)
		if channel:
			channel.close()
		return request.returns(dict(ok=True, id=cid))

	def getChannel(self, cid):
		self.expireChannels()
		channel = self.channels.get(cid)
		if channel:
			channel.touch()
		return channel

	def expireChannels(self):
		now = time.time()
		for cid, channel in list(self.channels.items()):
			if now - channel.lastSeen > self.CHANNEL_TTL:
				self.channels.pop(cid, None)
				channel.close()
		return self

	def resolveJournalTarget(self, target):
		if not isinstance(target, dict):
			return None, StorageWebError(
				"BADTARGET",
				"Invalid storage channel target.",
				"The storage channel target must be an object.",
				received=self.describeValue(target),
				expected='A target object such as {"kind":"object","type":"items","id":"..."}.',
			)
		kind = target.get("kind")
		match = self.resolveStorable(target.get("type"))
		if not match:
			return None, StorageWebError(
				"BADTYPE",
				"Storage type not found.",
				"No web storage type is registered for this channel target.",
				received=dict(type=target.get("type"), target=target),
				expected="A registered storage type or route name.",
				status=404,
			)
		storableClass, info = match
		backend = self.journalBackend(storableClass)
		if not backend:
			return None, StorageWebError(
				"UNSUPPORTED",
				"Storage backend does not support channel subscriptions.",
				"The selected storage backend does not expose journal subscribe/unsubscribe support.",
				received=dict(type=target.get("type"), target=target),
				expected="A journal-capable storage backend.",
			)
		if kind == "object":
			if target.get("id") is None:
				return None, StorageWebError(
					"NOID",
					"Storage channel target id is required.",
					"An object channel target must identify which object to subscribe to.",
					received=dict(target=target),
					expected='An "id" string in the channel target.',
				)
			return dict(
				backend=backend,
				key=storableClass.StorageKey(str(target.get("id"))),
				target=target,
				info=info,
			), None
		elif kind == "type":
			return dict(
				backend=backend,
				key=storableClass.StoragePrefix(),
				target=target,
				info=info,
			), None
		elif kind == "relation":
			if target.get("id") is None or target.get("name") is None:
				return None, StorageWebError(
					"BADTARGET",
					"Invalid storage channel relation target.",
					"A relation channel target must include both object id and relation name.",
					received=dict(target=target),
					expected='A target such as {"kind":"relation","type":"items","id":"...","name":"tags"}.',
				)
			return dict(
				backend=backend,
				key=storableClass.StorageKey(str(target.get("id"))),
				target=target,
				info=info,
			), None
		else:
			return None, StorageWebError(
				"BADTARGET",
				"Unsupported storage channel target.",
				"The storage channel target kind is not supported: %s." % kind,
				received=dict(kind=kind, target=target),
				expected='Supported target kinds: "object", "type", "relation".',
			)

	def journalBackend(self, storableClass):
		storage = getattr(storableClass, "STORAGE", None)
		backend = getattr(storage, "backend", None)
		if backend and all(hasattr(backend, _) for _ in ("subscribe", "unsubscribe", "getUpdate")):
			return backend
		return None

	def journalEvent(self, entry):
		meta = entry.get("meta") or {}
		operation = entry.get("operation")
		name = "update"
		if operation == "-":
			name = "remove"
		elif operation == "+":
			name = "create"
		return dict(
			event=name,
			seq=entry.get("seq"),
			operation=operation,
			key=entry.get("key"),
			type=meta.get("objectType"),
			id=meta.get("objectID"),
			revision=meta.get("revision"),
			patch=entry.get("patch") or [],
			relations=entry.get("relations") or {},
			entry=entry,
		)

	def formatSSE(self, event, data=None, id=None):
		lines = []
		if event:
			lines.append("event: %s" % event)
		if id is not None:
			lines.append("id: %s" % id)
		text = json.dumps(data or {}, separators=(",", ":"))
		for line in text.splitlines() or [""]:
			lines.append("data: %s" % line)
		return "\n".join(lines) + "\n\n"

	def onStorableRemove(self, storableClass, info, request, sid):
		sid = unquote(sid)
		if self.readonly:
			return request.notAuthorized()
		storable = storableClass.Get(sid)
		if storable:
			storable.remove()
			return request.returns(True)
		else:
			return request.notFound()

	def onStorableGet(self, storableClass, info, request, sid):
		sid = unquote(sid)
		storable = storableClass.Get(sid)
		if not storable:
			if request.param("strict") is not None:
				return request.notFound()
			else:
				return request.returns(storableClass.Export(sid, **info.getExportOptions()))
		return request.returns(storable.export(**info.getExportOptions()))

	async def onStorableInvokeMethod(
		self, storableClass, name, contentType, request, sid, *args, **kwargs
	):
		sid = unquote(sid)
		storable = storableClass.Get(sid)
		if not storable:
			return request.notFound()
		method = getattr(storable, name)
		args = list(args) if args else []
		if request.method == "POST":
			data = await request.loadData()
			if isinstance(data, list):
				args = data
			elif isinstance(data, dict):
				body_kwargs = dict(data)
				body_kwargs.update(kwargs)
				kwargs = body_kwargs
			elif data is not None:
				return self.storageError(
					request,
					StorageWebError(
						"BADPAYLOAD",
						"Invalid storage method payload.",
						"Storage method POST payloads must be a JSON object, JSON list, or empty body.",
						received=self.describeValue(data),
						expected="A JSON object for keyword arguments, a JSON list for positional arguments, or no body.",
					),
					dict(operation="invoke", method=name, id=sid),
				)
		else:
			query = dict(request.query or {})
			query.update(kwargs)
			kwargs = query
		args = [restore(_) for _ in args] if args else []
		kwargs = dict((k, restore(v)) for k, v in list(kwargs.items())) if kwargs else {}
		result = method(*args, **kwargs)
		if not contentType:
			return request.returns(result)
		if isinstance(contentType, types.FunctionType):
			contentType = contentType(storable)
		return request.respond(result, contentType=contentType)

	def onStorableInvokeOperation(self, storableClass, name, request, *args, **kwargs):
		method = getattr(storableClass, name)
		return request.returns(method(*args, **kwargs))

	def onStorableList(self, storableClass, info, request, start=0, end=None):
		options = info.getExportOptions()
		if end is None:
			end = start + self.LIST_COUNT
		res = [_.export(**options) for _ in storableClass.List(start=start, end=end)]
		return request.returns(dict(start=start, end=end, count=len(res), values=res))

	def onStorableRelations(self, storableClass, info, request, sid):
		sid = unquote(sid)
		storable = storableClass.Get(sid)
		if not storable:
			return request.notFound()
		res = {}
		for name, relation in storable.iterRelations() if hasattr(storable, "iterRelations") else ():
			relationClass = relation.getRelationClass()
			res[name] = dict(
				many=relation.isMany(),
				type=getCanonicalName(relationClass),
				count=len(relation),
				revision=storable.getUpdateTime(name),
			)
		return request.returns(dict(type=info.getName(), id=sid, relations=res))

	def onStorableRelationCount(self, storableClass, info, request, sid, name):
		sid = unquote(sid)
		storable = storableClass.Get(sid)
		if not storable:
			return request.notFound()
		try:
			relation = self.getStorableRelation(storable, name)
		except StorageWebError as error:
			return self.storageError(request, error, dict(type=info.getName(), id=sid))
		return request.returns(
			dict(
				type=info.getName(),
				id=sid,
				relation=name,
				count=len(relation),
				revision=storable.getUpdateTime(name),
			)
		)

	def onStorableRelationGet(self, storableClass, info, request, sid, name, start=0, end=None):
		sid = unquote(sid)
		storable = storableClass.Get(sid)
		if not storable:
			return request.notFound()
		try:
			relation = self.getStorableRelation(storable, name)
		except StorageWebError as error:
			return self.storageError(request, error, dict(type=info.getName(), id=sid))
		return request.returns(
			self.relationPage(storable, info, relation, name, request, start=start, end=end)
		)

	async def onStorableRelationOperation(
		self, storableClass, info, request, sid, name, operation
	):
		if self.readonly:
			return request.notAuthorized()
		sid = unquote(sid)
		storable = storableClass.Get(sid)
		if not storable:
			return request.notFound()
		try:
			data = await request.loadParams()
			relation = self.getStorableRelation(storable, name)
			self.validateRelationRevision(storable, name, data)
			changed = self.applyRelationOperation(relation, operation, data)
			if changed:
				storable.save()
		except StorageWebError as error:
			return self.storageError(
				request,
				error,
				dict(type=info.getName(), id=sid, relation=name, operation=operation),
			)
		except ValueError as error:
			return self.storageError(
				request,
				StorageWebError(
					"BADPAYLOAD",
					"Invalid storage relation payload.",
					str(error),
					received=self.describeRequest(request),
					expected="A JSON or form object with valid relation operation arguments.",
				),
				dict(type=info.getName(), id=sid, relation=name, operation=operation),
			)
		if request.param("return") == "none":
			return request.returns(dict(ok=True, operation=operation))
		if request.param("return") == "object":
			return request.returns(storable.export(**info.getExportOptions()))
		return request.returns(
			self.relationPage(storable, info, relation, name, request, operation=operation)
		)

	def getStorableRelation(self, storable, name):
		relations = getattr(storable.__class__, "RELATIONS", {}) or {}
		if name not in relations or not hasattr(storable, "getRelation"):
			raise StorageWebError(
				"BADRELATION",
				"Storage relation not found.",
				"The requested relation is not declared on this storage type.",
				received=dict(relation=name),
				expected="One of: %s" % sorted(relations.keys()),
				status=404,
			)
		return storable.getRelation(name)

	def relationPage(self, storable, info, relation, name, request, start=0, end=None, operation=None):
		return self.relationPageData(
			storable,
			info,
			relation,
			name,
			dict(
				start=self.intParam(request, "start", start or 0),
				end=end if end is not None else self.intParam(request, "end", None),
				count=self.intParam(request, "count", None),
				resolve=self.boolParam(request, "resolve", False),
				depth=self.intParam(request, "depth", 1),
			),
			operation=operation,
		)

	def relationPageData(self, storable, info, relation, name, options=None, operation=None):
		options = dict(options or {})
		start = self.intValue(options.get("start"), 0)
		end = self.intValue(options.get("end"), None)
		count = self.intValue(options.get("count"), None)
		if end is None:
			end = start + (count if count is not None else self.LIST_COUNT)
		resolve = self.boolValue(options.get("resolve"), False)
		depth = self.intValue(options.get("depth"), 1)
		values = list(relation.get(start=start, limit=end, resolve=resolve))
		info_options = info.getExportOptions()
		info_options.update(dict(depth=depth))
		res = [_.export(**info_options) if hasattr(_, "export") else _ for _ in values]
		return dict(
			ok=True,
			type=info.getName(),
			id=storable.id,
			relation=name,
			operation=operation,
			start=start,
			end=end,
			count=len(res),
			total=len(relation),
			revision=storable.getUpdateTime(name),
			values=res,
		)

	def applyRelationOperation(self, relation, operation, data):
		data = dict(data or {})
		operation = str(operation or "")
		many_ops = {"append", "prepend", "insert", "delete", "remove", "swap", "move"}
		if not relation.isMany() and operation in many_ops:
			raise StorageWebError(
				"BADOP",
				"Unsupported relation operation.",
				"The requested operation requires a many-valued relation.",
				received=dict(operation=operation),
				expected='Use "set" or "clear" on single-valued relations.',
			)
		values = list(relation.get(resolve=False))
		if operation == "set":
			items = self.relationValues(relation, data)
			if not relation.isMany() and len(items) > 1:
				raise StorageWebError(
					"TOOMANY",
					"Too many relation values.",
					"A single-valued relation cannot contain more than one value.",
					received=dict(count=len(items)),
					expected="Zero or one relation value.",
				)
			values = items
		elif operation == "append":
			values.extend(self.relationValues(relation, data))
		elif operation == "prepend":
			values = self.relationValues(relation, data) + values
		elif operation == "insert":
			index = self.relationIndex(data, "index", allowEnd=True, length=len(values))
			values[index:index] = self.relationValues(relation, data)
		elif operation == "delete":
			start, end = self.relationRange(data, len(values), single=True)
			del values[start:end]
		elif operation == "remove":
			items = self.relationValues(relation, data)
			values = [_ for _ in values if not any(isSame(_, item) for item in items)]
		elif operation == "swap":
			a = self.relationIndex(data, "a", length=len(values))
			b = self.relationIndex(data, "b", length=len(values))
			values[a], values[b] = values[b], values[a]
		elif operation == "move":
			start, end = self.relationRange(data, len(values), names=("from", "end"), single=True)
			to = self.relationIndex(data, "to", allowEnd=True, length=len(values))
			chunk = values[start:end]
			del values[start:end]
			if to > start:
				to -= len(chunk)
			values[to:to] = chunk
		elif operation == "clear":
			values = []
		else:
			raise StorageWebError(
				"BADOP",
				"Unsupported relation operation.",
				"The requested relation operation is not supported: %s." % operation,
				received=dict(operation=operation),
				expected='Supported operations: "set", "append", "prepend", "insert", "delete", "remove", "swap", "move", "clear".',
			)
		relation.set(values)
		return True

	def relationValues(self, relation, data):
		values = data.get("values")
		if values is None and "value" in data:
			values = data.get("value")
		if values is None:
			values = []
		elif not isinstance(values, (list, tuple)):
			values = [values]
		relationClass = relation.getRelationClass()
		res = []
		for value in values:
			if isinstance(value, dict) and "id" in value:
				value = dict(value)
				value["type"] = getCanonicalName(relationClass)
			res.append(value)
		return res

	def validateRelationRevision(self, storable, name, data):
		if not isinstance(data, dict) or "revision" not in data:
			return True
		received = int(data.get("revision") or 0)
		current = int(storable.getUpdateTime(name) or 0)
		if received != current:
			raise StorageWebError(
				"CONFLICT",
				"Relation was modified.",
				"The relation revision does not match the current stored revision.",
				received=dict(revision=received),
				expected=dict(revision=current),
				status=409,
			)
		return True

	def relationRange(self, data, length, names=("start", "end"), single=False):
		startName, endName = names
		if single and "index" in data:
			start = self.relationIndex(data, "index", length=length)
			return start, start + 1
		if single and startName in data and endName not in data:
			start = self.relationIndex(data, startName, length=length)
			return start, start + 1
		start = self.relationIndex(data, startName, length=length)
		end = self.relationIndex(data, endName, allowEnd=True, length=length)
		if end < start:
			raise StorageWebError(
				"BADRANGE",
				"Invalid relation range.",
				"The relation range end must be greater than or equal to start.",
				received=dict(start=start, end=end),
				expected="A valid half-open range [start, end).",
			)
		return start, end

	def relationIndex(self, data, name, allowEnd=False, length=0):
		if name not in data:
			raise StorageWebError(
				"NOINDEX",
				"Relation index is required.",
				"The relation operation requires an integer index.",
				received=dict(keys=list(data.keys())),
				expected='An integer field named "%s".' % name,
			)
		try:
			index = int(data.get(name))
		except (TypeError, ValueError):
			raise StorageWebError(
				"BADINDEX",
				"Invalid relation index.",
				"Relation indexes must be integers.",
				received=dict(index=data.get(name)),
				expected="An integer index.",
			)
		maximum = length if allowEnd else length - 1
		if index < 0 or index > maximum:
			raise StorageWebError(
				"INDEXRANGE",
				"Relation index out of range.",
				"The relation index is outside the current relation bounds.",
				received=dict(index=index, length=length),
				expected="An index between 0 and %s." % maximum,
			)
		return index

	def intParam(self, request, name, default=None):
		value = request.param(name)
		if value is None or value == "":
			return default
		return int(value)

	def intValue(self, value, default=None):
		if value is None or value == "":
			return default
		return int(value)

	def boolParam(self, request, name, default=False):
		value = request.param(name)
		if value is None:
			return default
		if isinstance(value, str):
			return value.lower() not in ("0", "false", "no", "off")
		return bool(value)

	def boolValue(self, value, default=False):
		if value is None:
			return default
		if isinstance(value, str):
			return value.lower() not in ("0", "false", "no", "off")
		return bool(value)

	def onRawGetData(self, storableClass, request, sid):
		sid = unquote(sid)
		storable = storableClass.Get(sid)
		assert isinstance(storable, StoredRaw)
		return request.respondFile(
			storable.path(),
			contentType=storable.meta("contentType")
			or storable.meta("mimeType")
			or "application/x-binary",
		)

	def _handler(self, functor, *methods):
		return Handler(functor=functor, methods=list(methods))

	def _iterHandlers(self, storableClass):
		info = StorageDecoration.Get(storableClass)
		url = info.url or info.getName()
		url = url[1:] if url.startswith("/") else url

		async def handler_create(request: HTTPRequest) -> HTTPResponse:
			return await self.onStorableCreate(storableClass, info, request)

		async def handler_update(request: HTTPRequest, sid: str) -> HTTPResponse:
			return await self.onStorableUpdate(storableClass, info, request, sid)

		def handler_get(request: HTTPRequest, sid: str) -> HTTPResponse:
			return self.onStorableGet(storableClass, info, request, sid)

		def handler_remove(request: HTTPRequest, sid: str) -> HTTPResponse:
			return self.onStorableRemove(storableClass, info, request, sid)

		def handler_list(
			request: HTTPRequest, start: int = 0, end: int | None = None
		) -> HTTPResponse:
			return self.onStorableList(storableClass, info, request, start, end)

		def handler_relations(request: HTTPRequest, sid: str) -> HTTPResponse:
			return self.onStorableRelations(storableClass, info, request, sid)

		def handler_relation_count(request: HTTPRequest, sid: str, name: str) -> HTTPResponse:
			return self.onStorableRelationCount(storableClass, info, request, sid, name)

		def handler_relation_get(
			request: HTTPRequest,
			sid: str,
			name: str,
			start: int = 0,
			end: int | None = None,
		) -> HTTPResponse:
			return self.onStorableRelationGet(
				storableClass, info, request, sid, name, start, end
			)

		async def handler_relation_operation(
			request: HTTPRequest, sid: str, name: str, operation: str
		) -> HTTPResponse:
			return await self.onStorableRelationOperation(
				storableClass, info, request, sid, name, operation
			)

		yield self._handler(handler_create, ("GET", url), ("POST", url))
		yield self._handler(handler_update, ("POST", url + "/{sid:segment}"))
		yield self._handler(handler_remove, ("POST", url + "/{sid:segment}/remove"))
		yield self._handler(handler_get, ("GET", url + "/{sid:segment}"))
		yield self._handler(handler_list, ("GET", url + "/list"))
		yield self._handler(handler_list, ("GET", url + "/list/{start:int}"))
		yield self._handler(handler_list, ("GET", url + "/list/{start:int}:"))
		yield self._handler(handler_list, ("GET", url + "/list/{start:int}:{end:int}"))
		yield self._handler(handler_relations, ("GET", url + "/{sid:segment}/relations"))
		yield self._handler(handler_relation_get, ("GET", url + "/{sid:segment}/relations/{name:segment}"))
		yield self._handler(handler_relation_count, ("GET", url + "/{sid:segment}/relations/{name:segment}/count"))
		yield self._handler(handler_relation_get, ("GET", url + "/{sid:segment}/relations/{name:segment}/list"))
		yield self._handler(handler_relation_get, ("GET", url + "/{sid:segment}/relations/{name:segment}/list/{start:int}"))
		yield self._handler(handler_relation_get, ("GET", url + "/{sid:segment}/relations/{name:segment}/list/{start:int}:"))
		yield self._handler(handler_relation_get, ("GET", url + "/{sid:segment}/relations/{name:segment}/list/{start:int}:{end:int}"))
		yield self._handler(handler_relation_operation, ("POST", url + "/{sid:segment}/relations/{name:segment}/{operation:segment}"))

		for name, meta in info.listInvocables():
			invoke_url, restrict, methods, contentType = meta
			invoke_url = invoke_url[1:] if invoke_url.startswith("/") else invoke_url

			async def handler_invoke(
				request: HTTPRequest,
				sid: str,
				_handlerName: str = name,
				_contentType=contentType,
				**kwargs,
			) -> HTTPResponse:
				return await self.onStorableInvokeMethod(
					storableClass,
					_handlerName,
					_contentType,
					request,
					sid,
					**kwargs,
				)

			urls = []
			methods = (methods,) if isinstance(methods, str) else methods
			for method in methods or ("GET", "POST"):
				urls.append((method.upper(), url + "/{sid:segment}/" + invoke_url))
			yield self._handler(handler_invoke, *urls)

		if issubclass(storableClass, StoredRaw):
			def handler_raw(request: HTTPRequest, sid: str) -> HTTPResponse:
				return self.onRawGetData(storableClass, request, sid)

			yield self._handler(handler_raw, ("GET", url + "/{sid:segment}/data"))


# EOF
