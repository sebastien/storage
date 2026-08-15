import time
import types
import sys
from pathlib import Path
from typing import Iterable
from urllib.parse import unquote

try:
	from extra import HTTPRequest, HTTPResponse, Service
	from extra.routing import Handler
except ImportError:
	extra_path = Path(__file__).resolve().parents[4] / "deps" / "extra" / "src" / "py"
	if str(extra_path) not in sys.path:
		sys.path.insert(0, str(extra_path))
	from extra import HTTPRequest, HTTPResponse, Service
	from extra.routing import Handler

from ..core import restore
from ..raw import StoredRaw
from ..query import StoredQuery
from .channels import StorageChannel
from .decorators import StorageDecoration
from .errors import StorageWebError
from .formatting import StorageFormatting
from .kv import KVWebMixin

__all__ = ["ObjectWebMixin"]


# FIXME: It seems that sometimes when one element is sent as a field value
# (like shootingback.model.Clip.tags=["Youth"], only "Youth" is stored
# instead of ["Youth"]. Might be in objects or JSON conversion.


class ObjectWebMixin(KVWebMixin, StorageFormatting, Service):
	"""HTTP handlers for stored objects and their object-oriented routes."""

	UNSCOPED = object()

	LIST_COUNT = 20
	CHANNEL_HEARTBEAT = 15
	CHANNEL_TTL = 45
	CHANNEL_QUEUE_SIZE = 1000
	FORMAT_CONTENT_TYPES = {
		"md": "text/markdown; charset=utf-8",
		"xml": "application/xml; charset=utf-8",
	}

	def __init__(self, prefix="/api", classes=None, readonly=False):
		prefix = prefix.strip("/")
		prefix = prefix + "/" if prefix else ""
		Service.__init__(self, prefix=prefix)
		self.storableClasses = []
		self.kvStores = {}
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

	def inferOwner(self, request, storableClass):
		return self.UNSCOPED

	def resolvedOwner(self, request, storableClass):
		ownership = (
			storableClass.GetOwnership()
			if hasattr(storableClass, "GetOwnership")
			else None
		)
		return self.inferOwner(request, storableClass) if ownership else self.UNSCOPED

	def isScoped(self, owner):
		return owner is not self.UNSCOPED

	def scopedGet(self, request, storableClass, sid):
		owner = self.resolvedOwner(request, storableClass)
		return (
			storableClass.Get(sid, owner=owner)
			if self.isScoped(owner)
			else storableClass.Get(sid)
		)

	def scopedExport(self, request, storableClass, sid, **options):
		owner = self.resolvedOwner(request, storableClass)
		value = (
			storableClass.Export(sid, owner=owner, **options)
			if self.isScoped(owner)
			else storableClass.Export(sid, **options)
		)
		return self.toPublicValue(value, request)

	def applyScopedOwner(self, request, storableClass, data):
		owner = self.resolvedOwner(request, storableClass)
		if not self.isScoped(owner):
			return data, owner
		if data is None:
			data = {}
		if not isinstance(data, dict):
			raise StorageWebError(
				"BADPAYLOAD",
				"Invalid storage create payload.",
				"Owned storage create payloads must be a JSON object or empty body.",
				received=self.describeValue(data),
				expected="A JSON object or empty body.",
			)
		data = dict(data)
		data["owner"] = owner
		return data, owner

	def scopedList(self, request, storableClass, start=0, end=None):
		owner = self.resolvedOwner(request, storableClass)
		values = (
			storableClass.OwnedBy(owner)
			if self.isScoped(owner)
			else storableClass.List(start=start, end=end)
		)
		for index, value in enumerate(values):
			if self.isScoped(owner):
				if index < start:
					continue
				if end is not None and index >= end:
					break
			yield value

	def publicID(self, storableClass, value, request):
		return value

	def toPublicValue(self, value, request):
		if isinstance(value, list):
			return [self.toPublicValue(_, request) for _ in value]
		if isinstance(value, tuple):
			return [self.toPublicValue(_, request) for _ in value]
		if not isinstance(value, dict):
			return value
		result = {
			key: self.toPublicValue(item, request)
			for key, item in value.items()
		}
		if "type" in result and "id" in result:
			match = self.resolveStorable(result.get("type"))
			if match:
				storableClass, _info = match
				result["id"] = self.publicID(storableClass, result["id"], request)
				result.pop("partition", None)
		return result

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
		for name in self.kvStores:
			yield from self._iterKVHandlers(name)
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
		data, owner = self.applyScopedOwner(
			request, storableClass, await request.loadData()
		)
		if data is not None:
			storable = storableClass.Import(data).save()
		else:
			storable = (
				storableClass(owner=owner)
				if self.isScoped(owner)
				else storableClass()
			).save()
		return request.returns(
			self.toPublicValue(storable.export(**info.getExportOptions()), request)
		)

	async def onStorableUpdate(self, storableClass, info, request, sid):
		sid = unquote(sid)
		if self.readonly:
			return request.notAuthorized()
		try:
			data = await request.loadParams()
			self.validateUpdatePayload(data, dict(type=info.getName(), id=sid))
			storable = self.applyStorableUpdate(request, storableClass, sid, data)
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
		return request.returns(
			self.toPublicValue(storable.export(**info.getExportOptions()), request)
		)

	def applyStorableUpdate(self, request, storableClass, sid, data):
		data = dict(data or {})
		owner = self.resolvedOwner(request, storableClass)
		storable = (
			storableClass.Get(sid, owner=owner)
			if self.isScoped(owner)
			else storableClass.Get(sid)
		)
		if not storable:
			if "id" not in data:
				data["id"] = sid
			if self.isScoped(owner):
				data["owner"] = owner
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
				result = await self.onCommand(request, command, index=index)
				results.append(result)
		finally:
			for backend, batch in reversed(batches):
				backend.endBatch(batch)
		res = dict(results=results)
		if transaction:
			res["transaction"] = True
		return request.returns(res)

	async def onCommand(self, request, command, index=None):
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
				return self.onCreateCommand(request, command, index=index)
			elif op == "update":
				return self.onUpdateCommand(request, command, index=index)
			elif op == "remove":
				return self.onRemoveCommand(request, command, index=index)
			elif isinstance(op, str) and op.startswith("relation."):
				return self.onRelationCommand(request, command, index=index)
			elif op == "invoke":
				return self.onInvokeCommand(request, command, index=index)
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

	def onCreateCommand(self, request, command, index=None):
		match = self.commandMatch(command.get("type"))
		storableClass, info = match
		fields = command.get("fields")
		fields, owner = self.applyScopedOwner(request, storableClass, fields)
		if fields is not None:
			self.validateUpdatePayload(fields, dict(type=info.getName(), index=index))
			storable = storableClass.Import(fields).save()
		else:
			storable = (
				storableClass(owner=owner)
				if self.isScoped(owner)
				else storableClass()
			).save()
		return dict(
			ok=True,
			op="create",
			type=info.getName(),
			id=self.publicID(storableClass, storable.id, request),
			value=self.toPublicValue(storable.export(**info.getExportOptions()), request),
		)

	def onUpdateCommand(self, request, command, index=None):
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
		storable = self.applyStorableUpdate(request, storableClass, str(sid), fields)
		return dict(
			ok=True,
			op="update",
			type=command_type,
			id=str(sid),
			value=self.toPublicValue(storable.export(**info.getExportOptions()), request),
		)

	def onRemoveCommand(self, request, command, index=None):
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
		storable = self.scopedGet(request, storableClass, str(sid))
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

	def onInvokeCommand(self, request, command, index=None):
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
		storable = self.scopedGet(request, storableClass, str(sid))
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
			value=self.toPublicValue(result, request),
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
		elif kind == "query":
			owner = target.get("owner")
			ownership = storableClass.GetOwnership() if hasattr(storableClass, "GetOwnership") else None
			if owner is None:
				return None, StorageWebError(
					"BADTARGET",
					"Invalid storage channel query target.",
					"An owner-scoped query target must include an owner id.",
					received=dict(target=target),
					expected='A target such as {"kind":"query","type":"members","owner":"user-1"}.',
				)
			if not ownership:
				return None, StorageWebError(
					"UNSUPPORTED",
					"Storage query target is not supported.",
					"Owner-scoped queries currently require a stored object type that declares OWNERSHIP.",
					received=dict(type=target.get("type"), target=target),
					expected="A stored object type with OWNERSHIP metadata.",
				)
			query = StoredQuery(
				storableClass,
				owner=owner,
				target=dict(kind="query", type=target.get("type"), owner=owner),
				export=info.getExportOptions(),
			)
			return dict(
				backend=backend,
				key=query.prefix(),
				query=query,
				target=query.target,
				info=info,
			), None
		else:
			return None, StorageWebError(
				"BADTARGET",
				"Unsupported storage channel target.",
				"The storage channel target kind is not supported: %s." % kind,
				received=dict(kind=kind, target=target),
				expected='Supported target kinds: "object", "type", "relation", "query".',
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

	def onStorableRemove(self, storableClass, info, request, sid):
		sid = unquote(sid)
		if self.readonly:
			return request.notAuthorized()
		storable = self.scopedGet(request, storableClass, sid)
		if storable:
			storable.remove()
			return request.returns(True)
		else:
			return request.notFound()

	def onStorableGet(self, storableClass, info, request, sid, format=None):
		format = format or self.requestFormat(request)
		sid = self.stripFormatSuffix(unquote(sid), format)
		storable = self.scopedGet(request, storableClass, sid)
		if not storable:
			if request.param("strict") is not None:
				return request.notFound()
			else:
				return self.respondFormatted(
					request,
					self.scopedExport(request, storableClass, sid, **info.getExportOptions()),
					format=format,
				)
		return self.respondFormatted(
			request,
			self.toPublicValue(storable.export(**info.getExportOptions()), request),
			format=format,
		)

	async def onStorableInvokeMethod(
		self, storableClass, name, contentType, request, sid, *args, format=None, **kwargs
	):
		format = format or self.requestFormat(request)
		sid = unquote(sid)
		storable = self.scopedGet(request, storableClass, sid)
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
			return self.respondFormatted(
				request, self.toPublicValue(result, request), format=format
			)
		if isinstance(contentType, types.FunctionType):
			contentType = contentType(storable)
		return request.respond(result, contentType=contentType)

	def onStorableInvokeOperation(self, storableClass, name, request, *args, **kwargs):
		method = getattr(storableClass, name)
		return request.returns(method(*args, **kwargs))

	def onStorableList(self, storableClass, info, request, start=0, end=None, format=None):
		options = info.getExportOptions()
		if end is None:
			end = start + self.LIST_COUNT
		res = [_.export(**options) for _ in self.scopedList(request, storableClass, start=start, end=end)]
		return self.respondFormatted(
			request,
			self.toPublicValue(dict(start=start, end=end, count=len(res), values=res), request),
			format=format,
		)

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

		def handler_get(request: HTTPRequest, sid: str, _format=None) -> HTTPResponse:
			return self.onStorableGet(storableClass, info, request, sid, format=_format)

		def handler_get_md(request: HTTPRequest, sid: str) -> HTTPResponse:
			return self.onStorableGet(storableClass, info, request, sid, format="md")

		def handler_get_xml(request: HTTPRequest, sid: str) -> HTTPResponse:
			return self.onStorableGet(storableClass, info, request, sid, format="xml")

		def handler_remove(request: HTTPRequest, sid: str) -> HTTPResponse:
			return self.onStorableRemove(storableClass, info, request, sid)

		def handler_list(
			request: HTTPRequest, start: int = 0, end: int | None = None, _format=None
		) -> HTTPResponse:
			return self.onStorableList(
				storableClass, info, request, start, end, format=_format
			)

		def handler_list_md(
			request: HTTPRequest, start: int = 0, end: int | None = None
		) -> HTTPResponse:
			return self.onStorableList(storableClass, info, request, start, end, format="md")

		def handler_list_xml(
			request: HTTPRequest, start: int = 0, end: int | None = None
		) -> HTTPResponse:
			return self.onStorableList(storableClass, info, request, start, end, format="xml")

		yield self._handler(handler_create, ("GET", url), ("POST", url))
		yield self._handler(handler_update, ("POST", url + "/{sid:segment}"))
		yield self._handler(handler_remove, ("POST", url + "/{sid:segment}/remove"))
		yield self._handler(handler_get_md, ("GET", url + "/{sid:segment}.md"))
		yield self._handler(handler_get_xml, ("GET", url + "/{sid:segment}.xml"))
		yield self._handler(handler_get, ("GET", url + "/{sid:segment}"))
		yield self._handler(handler_list, ("GET", url + "/list"))
		yield self._handler(handler_list_md, ("GET", url + "/list.md"))
		yield self._handler(handler_list_xml, ("GET", url + "/list.xml"))
		yield self._handler(handler_list, ("GET", url + "/list/{start:int}"))
		yield self._handler(handler_list_md, ("GET", url + "/list/{start:int}.md"))
		yield self._handler(handler_list_xml, ("GET", url + "/list/{start:int}.xml"))
		yield self._handler(handler_list, ("GET", url + "/list/{start:int}:"))
		yield self._handler(handler_list_md, ("GET", url + "/list/{start:int}:.md"))
		yield self._handler(handler_list_xml, ("GET", url + "/list/{start:int}:.xml"))
		yield self._handler(handler_list, ("GET", url + "/list/{start:int}:{end:int}"))
		yield self._handler(handler_list_md, ("GET", url + "/list/{start:int}:{end:int}.md"))
		yield self._handler(handler_list_xml, ("GET", url + "/list/{start:int}:{end:int}.xml"))
		for name, meta in info.listInvocables():
			invoke_url, restrict, methods, contentType = meta
			invoke_url = invoke_url[1:] if invoke_url.startswith("/") else invoke_url

			async def handler_invoke(
				request: HTTPRequest,
				sid: str,
				_handlerName: str = name,
				_contentType=contentType,
				_format=None,
				**kwargs,
			) -> HTTPResponse:
				return await self.onStorableInvokeMethod(
					storableClass,
					_handlerName,
					_contentType,
					request,
					sid,
					format=_format,
					**kwargs,
				)

			async def handler_invoke_md(
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
					format="md",
					**kwargs,
				)

			async def handler_invoke_xml(
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
					format="xml",
					**kwargs,
				)

			urls = []
			methods = (methods,) if isinstance(methods, str) else methods
			for method in methods or ("GET", "POST"):
				method = method.upper()
				urls.append((method, url + "/{sid:segment}/" + invoke_url))
			if methods is None:
				methods = ("GET", "POST")
			if any(_.upper() == "GET" for _ in methods or ()) and not contentType:
				yield self._handler(handler_invoke_md, ("GET", url + "/{sid:segment}/" + invoke_url + ".md"))
				yield self._handler(handler_invoke_xml, ("GET", url + "/{sid:segment}/" + invoke_url + ".xml"))
			yield self._handler(handler_invoke, *urls)

		if issubclass(storableClass, StoredRaw):
			def handler_raw(request: HTTPRequest, sid: str) -> HTTPResponse:
				return self.onRawGetData(storableClass, request, sid)

			yield self._handler(handler_raw, ("GET", url + "/{sid:segment}/data"))
# EOF
