"""Key-value HTTP API routes and helpers."""

import sys
from pathlib import Path
from urllib.parse import unquote

try:
	from extra import HTTPRequest, HTTPResponse
except ImportError:
	extra_path = Path(__file__).resolve().parents[4] / "deps" / "extra" / "src" / "py"
	if str(extra_path) not in sys.path:
		sys.path.insert(0, str(extra_path))
	from extra import HTTPRequest, HTTPResponse

from ..core import restore
from .errors import StorageWebError

__all__ = ["KVWebMixin"]


class KVWebMixin:
	"""HTTP registration and handlers for named key-value stores."""

	def kv(self, name, store, readonly=None, export=None):
		"""Registers a named KV store for HTTP exposure."""
		assert name, "KV store name is required"
		self.kvStores[str(name).strip("/")] = dict(
			name=str(name).strip("/"),
			store=store,
			readonly=self.readonly if readonly is None else bool(readonly),
			export=export or {},
		)
		self._handlers = None
		return self

	def useKV(self, name, store, readonly=None, export=None):
		"""Alias for `kv`."""
		return self.kv(name, store, readonly=readonly, export=export)

	def resolveKV(self, name):
		if name is None:
			return None
		return self.kvStores.get(str(name).strip("/"))

	def kvKey(self, key):
		return unquote(str(key))

	def kvPage(self, values, request):
		start = self.intParam(request, "start", 0)
		count = self.intParam(request, "count", self.LIST_COUNT)
		end = self.intParam(request, "end", start + count)
		items = list(values)
		page = items[start:end]
		return dict(start=start, end=end, count=len(page), total=len(items), values=page)

	def kvPageData(self, values, start=0, end=None, count=None):
		count = self.LIST_COUNT if count is None else count
		end = start + count if end is None else end
		items = list(values)
		page = items[start:end]
		return dict(start=start, end=end, count=len(page), total=len(items), values=page)

	def respondFormatted(self, request, value, format=None, status=200):
		format = format or self.requestFormat(request)
		if format == "md":
			return request.respond(
				self.formatMarkdown(value),
				contentType=self.FORMAT_CONTENT_TYPES["md"],
				status=status,
			)
		elif format == "xml":
			return request.respond(
				self.formatXML(value),
				contentType=self.FORMAT_CONTENT_TYPES["xml"],
				status=status,
			)
		return request.returns(value, status=status)

	def onKVDescribe(self, request, name, format=None):
		info = self.resolveKV(name)
		if not info:
			return request.notFound()
		return self.respondFormatted(
			request,
			dict(
				name=info["name"],
				readonly=info["readonly"],
				capabilities=(
					"get",
					"set",
					"delete",
					"has",
					"list",
					"items",
					"size",
					"clear",
					"commands",
				),
			),
			format=format,
		)

	def onKVSize(self, request, name, format=None):
		info = self.resolveKV(name)
		if not info:
			return request.notFound()
		return self.respondFormatted(
			request, dict(name=info["name"], size=info["store"].size()), format=format
		)

	def onKVHas(self, request, name, key, format=None):
		format = format or self.requestFormat(request)
		info = self.resolveKV(name)
		if not info:
			return request.notFound()
		key = self.stripFormatSuffix(self.kvKey(key), format)
		return self.respondFormatted(
			request,
			dict(name=info["name"], key=key, value=info["store"].has(key)),
			format=format,
		)

	def onKVGet(self, request, name, key, format=None):
		format = format or self.requestFormat(request)
		info = self.resolveKV(name)
		if not info:
			return request.notFound()
		key = self.stripFormatSuffix(self.kvKey(key), format)
		return self.respondFormatted(
			request,
			dict(name=info["name"], key=key, value=info["store"].get(key)),
			format=format,
		)

	async def onKVSet(self, request, name, key):
		info = self.resolveKV(name)
		if not info:
			return request.notFound()
		if info["readonly"]:
			return request.notAuthorized()
		key = self.kvKey(key)
		try:
			data = await request.loadData()
		except ValueError as error:
			return self.storageError(
				request,
				StorageWebError(
					"BADPAYLOAD",
					"Invalid KV payload.",
					str(error),
					received=self.describeRequest(request),
					expected='A JSON object such as {"value":...}.',
				),
				dict(operation="kv.set", store=info["name"], key=key),
			)
		if not isinstance(data, dict) or "value" not in data:
			return self.storageError(
				request,
				StorageWebError(
					"BADPAYLOAD",
					"Invalid KV payload.",
					"The KV set payload must be an object containing a value field.",
					received=self.describeValue(data),
					expected='A JSON object such as {"value":...}.',
				),
				dict(operation="kv.set", store=info["name"], key=key),
			)
		value = info["store"].set(key, restore(data.get("value")))
		return request.returns(dict(name=info["name"], key=key, value=value))

	def onKVDelete(self, request, name, key):
		info = self.resolveKV(name)
		if not info:
			return request.notFound()
		if info["readonly"]:
			return request.notAuthorized()
		key = self.kvKey(key)
		info["store"].delete(key)
		return request.returns(dict(name=info["name"], key=key, value=True))

	def onKVClear(self, request, name):
		info = self.resolveKV(name)
		if not info:
			return request.notFound()
		if info["readonly"]:
			return request.notAuthorized()
		info["store"].clear()
		return request.returns(dict(name=info["name"], value=True))

	def onKVList(self, request, name, format=None):
		info = self.resolveKV(name)
		if not info:
			return request.notFound()
		prefix = request.param("prefix")
		return self.respondFormatted(
			request, self.kvPage(info["store"].ilist(prefix), request), format=format
		)

	def onKVItems(self, request, name, format=None):
		info = self.resolveKV(name)
		if not info:
			return request.notFound()
		prefix = request.param("prefix")
		values = [dict(key=key, value=value) for key, value in info["store"].iitems(prefix)]
		return self.respondFormatted(request, self.kvPage(values, request), format=format)

	async def onKVCommands(self, request, name):
		info = self.resolveKV(name)
		if not info:
			return request.notFound()
		if info["readonly"]:
			return request.notAuthorized()
		try:
			data = await request.loadData()
		except ValueError as error:
			return self.storageError(
				request,
				StorageWebError(
					"BADPAYLOAD",
					"Invalid KV commands payload.",
					str(error),
					received=self.describeRequest(request),
					expected='A JSON object with a "commands" list.',
				),
				dict(operation="kv.commands", store=info["name"]),
			)
		commands = data.get("commands") if isinstance(data, dict) else None
		if not isinstance(commands, list):
			return self.storageError(
				request,
				StorageWebError(
					"BADLIST",
					"Invalid KV commands list.",
					'The KV commands payload must contain a "commands" list.',
					received=self.describeValue(data),
					expected='A JSON object such as {"commands":[{"op":"set",...}]}.',
				),
				dict(operation="kv.commands", store=info["name"]),
			)
		results = [self.onKVCommand(info, _, index=i) for i, _ in enumerate(commands)]
		return request.returns(dict(results=results))

	def onKVCommand(self, info, command, index=None):
		if not isinstance(command, dict):
			return StorageWebError(
				"BADITEM",
				"Invalid KV command.",
				"Each KV command must be an object.",
				received=self.describeValue(command),
				expected='A command object such as {"op":"set","key":"...","value":...}.',
			).payload(dict(operation="kv.commands", index=index, store=info["name"]))
		op = command.get("op")
		key = command.get("key")
		store = info["store"]
		try:
			if op == "set":
				if key is None:
					raise StorageWebError("NOKEY", "KV key is required.", "The KV set command requires a key.", received=dict(key=key), expected='A "key" string.')
				value = restore(command.get("value"))
				return dict(ok=True, op=op, key=str(key), value=store.set(str(key), value))
			elif op == "get":
				if key is None:
					raise StorageWebError("NOKEY", "KV key is required.", "The KV get command requires a key.", received=dict(key=key), expected='A "key" string.')
				return dict(ok=True, op=op, key=str(key), value=store.get(str(key)))
			elif op == "has":
				if key is None:
					raise StorageWebError("NOKEY", "KV key is required.", "The KV has command requires a key.", received=dict(key=key), expected='A "key" string.')
				return dict(ok=True, op=op, key=str(key), value=store.has(str(key)))
			elif op == "delete":
				if key is None:
					raise StorageWebError("NOKEY", "KV key is required.", "The KV delete command requires a key.", received=dict(key=key), expected='A "key" string.')
				store.delete(str(key))
				return dict(ok=True, op=op, key=str(key), value=True)
			elif op == "list":
				prefix = command.get("prefix")
				return dict(ok=True, op=op, **self.kvPageData(store.ilist(prefix), start=self.intValue(command.get("start"), 0), end=self.intValue(command.get("end"), None), count=self.intValue(command.get("count"), self.LIST_COUNT)))
			elif op == "items":
				prefix = command.get("prefix")
				values = [dict(key=k, value=v) for k, v in store.iitems(prefix)]
				return dict(ok=True, op=op, **self.kvPageData(values, start=self.intValue(command.get("start"), 0), end=self.intValue(command.get("end"), None), count=self.intValue(command.get("count"), self.LIST_COUNT)))
			elif op == "size":
				return dict(ok=True, op=op, value=store.size())
			elif op == "clear":
				store.clear()
				return dict(ok=True, op=op, value=True)
			else:
				return StorageWebError(
					"BADOP",
					"Unsupported KV command.",
					"The KV command operation is not supported: %s." % op,
					received=dict(op=op),
					expected='Supported KV operations: "set", "get", "has", "delete", "list", "items", "size", "clear".',
				).payload(dict(operation="kv.commands", index=index, store=info["name"]))
		except StorageWebError as error:
			return error.payload(dict(operation="kv.commands", index=index, store=info["name"], op=op))

	def _iterKVHandlers(self, name):
		base = "kv/%s" % name

		def handler_describe(request: HTTPRequest, _name: str = name, _format=None) -> HTTPResponse:
			return self.onKVDescribe(request, _name, format=_format)

		def handler_describe_md(request: HTTPRequest, _name: str = name) -> HTTPResponse:
			return self.onKVDescribe(request, _name, format="md")

		def handler_describe_xml(request: HTTPRequest, _name: str = name) -> HTTPResponse:
			return self.onKVDescribe(request, _name, format="xml")

		def handler_size(request: HTTPRequest, _name: str = name, _format=None) -> HTTPResponse:
			return self.onKVSize(request, _name, format=_format)

		def handler_size_md(request: HTTPRequest, _name: str = name) -> HTTPResponse:
			return self.onKVSize(request, _name, format="md")

		def handler_size_xml(request: HTTPRequest, _name: str = name) -> HTTPResponse:
			return self.onKVSize(request, _name, format="xml")

		def handler_has(request: HTTPRequest, key: str, _name: str = name, _format=None) -> HTTPResponse:
			return self.onKVHas(request, _name, key, format=_format)

		def handler_has_md(request: HTTPRequest, key: str, _name: str = name) -> HTTPResponse:
			return self.onKVHas(request, _name, key, format="md")

		def handler_has_xml(request: HTTPRequest, key: str, _name: str = name) -> HTTPResponse:
			return self.onKVHas(request, _name, key, format="xml")

		def handler_get(request: HTTPRequest, key: str, _name: str = name, _format=None) -> HTTPResponse:
			return self.onKVGet(request, _name, key, format=_format)

		def handler_get_md(request: HTTPRequest, key: str, _name: str = name) -> HTTPResponse:
			return self.onKVGet(request, _name, key, format="md")

		def handler_get_xml(request: HTTPRequest, key: str, _name: str = name) -> HTTPResponse:
			return self.onKVGet(request, _name, key, format="xml")

		async def handler_set(request: HTTPRequest, key: str, _name: str = name) -> HTTPResponse:
			return await self.onKVSet(request, _name, key)

		def handler_delete(request: HTTPRequest, key: str, _name: str = name) -> HTTPResponse:
			return self.onKVDelete(request, _name, key)

		def handler_list(request: HTTPRequest, _name: str = name, _format=None) -> HTTPResponse:
			return self.onKVList(request, _name, format=_format)

		def handler_list_md(request: HTTPRequest, _name: str = name) -> HTTPResponse:
			return self.onKVList(request, _name, format="md")

		def handler_list_xml(request: HTTPRequest, _name: str = name) -> HTTPResponse:
			return self.onKVList(request, _name, format="xml")

		def handler_items(request: HTTPRequest, _name: str = name, _format=None) -> HTTPResponse:
			return self.onKVItems(request, _name, format=_format)

		def handler_items_md(request: HTTPRequest, _name: str = name) -> HTTPResponse:
			return self.onKVItems(request, _name, format="md")

		def handler_items_xml(request: HTTPRequest, _name: str = name) -> HTTPResponse:
			return self.onKVItems(request, _name, format="xml")

		def handler_clear(request: HTTPRequest, _name: str = name) -> HTTPResponse:
			return self.onKVClear(request, _name)

		async def handler_commands(request: HTTPRequest, _name: str = name) -> HTTPResponse:
			return await self.onKVCommands(request, _name)

		yield self._handler(handler_describe, ("GET", base))
		yield self._handler(handler_describe_md, ("GET", base + ".md"))
		yield self._handler(handler_describe_xml, ("GET", base + ".xml"))
		yield self._handler(handler_size, ("GET", base + "/size"))
		yield self._handler(handler_size_md, ("GET", base + "/size.md"))
		yield self._handler(handler_size_xml, ("GET", base + "/size.xml"))
		yield self._handler(handler_has_md, ("GET", base + "/has/{key:segment}.md"))
		yield self._handler(handler_has_xml, ("GET", base + "/has/{key:segment}.xml"))
		yield self._handler(handler_has, ("GET", base + "/has/{key:segment}"))
		yield self._handler(handler_get_md, ("GET", base + "/get/{key:segment}.md"))
		yield self._handler(handler_get_xml, ("GET", base + "/get/{key:segment}.xml"))
		yield self._handler(handler_get, ("GET", base + "/get/{key:segment}"))
		yield self._handler(handler_set, ("POST", base + "/set/{key:segment}"))
		yield self._handler(handler_delete, ("POST", base + "/delete/{key:segment}"))
		yield self._handler(handler_list, ("GET", base + "/list"))
		yield self._handler(handler_list_md, ("GET", base + "/list.md"))
		yield self._handler(handler_list_xml, ("GET", base + "/list.xml"))
		yield self._handler(handler_items, ("GET", base + "/items"))
		yield self._handler(handler_items_md, ("GET", base + "/items.md"))
		yield self._handler(handler_items_xml, ("GET", base + "/items.xml"))
		yield self._handler(handler_clear, ("POST", base + "/clear"))
		yield self._handler(handler_commands, ("POST", base + "/commands"))


def __getattr__(name):
	if name == "StorageServer":
		from .server import StorageServer

		return StorageServer
	raise AttributeError(name)
