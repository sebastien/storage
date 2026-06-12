import types
import sys
from pathlib import Path
from typing import Iterable

try:
	from extra import HTTPRequest, HTTPResponse, Service
	from extra.routing import Handler
except ImportError:
	extra_path = Path(__file__).resolve().parents[3] / "deps" / "extra" / "src" / "py"
	if str(extra_path) not in sys.path:
		sys.path.insert(0, str(extra_path))
	from extra import HTTPRequest, HTTPResponse, Service
	from extra.routing import Handler

from .core import Storable, restore
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


class StorageServer(Service):
	"""An Extra service that exposes storables through a REST-like API."""

	LIST_COUNT = 20

	def __init__(self, prefix="/api", classes=None, readonly=False):
		prefix = prefix.strip("/")
		prefix = prefix + "/" if prefix else ""
		Service.__init__(self, prefix=prefix)
		self.storableClasses = []
		self.readonly = readonly
		self._onUpdate = []
		if classes:
			self.add(*classes)

	def onUpdate(self, callback):
		self._onUpdate.append(callback)
		return self

	def offUpdate(self, callback):
		self._onUpdate = [_ for _ in self._onUpdate if _ is not callback]
		return self

	def _doUpdate(self):
		for _ in self._onUpdate:
			_()

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
		self._doUpdate()
		return request.returns(storable.export(**info.getExportOptions()))

	async def onStorableUpdate(self, storableClass, info, request, sid):
		if self.readonly:
			return request.notAuthorized()
		storable = storableClass.Get(sid)
		data = await request.loadParams()
		if not storable:
			storable = storableClass.Import(data)
			storable.save()
		else:
			storable.update(data)
			storable.save()
		self._doUpdate()
		return request.returns(storable.export(**info.getExportOptions()))

	def onStorableRemove(self, storableClass, info, request, sid):
		if self.readonly:
			return request.notAuthorized()
		storable = storableClass.Get(sid)
		if storable:
			storable.remove()
			self._doUpdate()
			return request.returns(True)
		else:
			return request.notFound()

	def onStorableGet(self, storableClass, info, request, sid):
		storable = storableClass.Get(sid)
		if not storable:
			if request.query and "strict" in request.query:
				return request.notFound()
			else:
				storable = storableClass(id=sid)
		return request.returns(storable.export(**info.getExportOptions()))

	async def onStorableInvokeMethod(
		self, storableClass, name, contentType, request, sid, *args, **kwargs
	):
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
				raise ValueError(f"Unsupported payload type: {type(data)}")
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

	def onRawGetData(self, storableClass, request, sid):
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

		yield self._handler(handler_create, ("GET", url), ("POST", url))
		yield self._handler(handler_update, ("POST", url + "/{sid:segment}"))
		yield self._handler(handler_remove, ("POST", url + "/{sid:segment}/remove"))
		yield self._handler(handler_get, ("GET", url + "/{sid:segment}"))
		yield self._handler(handler_list, ("GET", url + "/list"))
		yield self._handler(handler_list, ("GET", url + "/list/{start:int}"))
		yield self._handler(handler_list, ("GET", url + "/list/{start:int}:"))
		yield self._handler(handler_list, ("GET", url + "/list/{start:int}:{end:int}"))

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
