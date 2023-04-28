import types, json
from storage import Storable, restore
from storage.raw import StoredRaw
import retro.web

# FIXME: It seems that sometimes when one element is sent as a field value
# (like shootingback.model.Clip.tags=["Youth"], only "Youth" is stored
# instead of ["Youth"]. Might be in objects or JSON conversion.

# FIXME: Document me!!!

# TODO: @http("/asdsada/${asdsa}"
def http(url, restrict=None, methods=None, contentType=None, export=None):
    """Adds a `StorageDecoration` information to the give Storable subclass"""

    def wrapper(storableClassOrFunction):
        # Or did we use it to decorate a method?
        if type(storableClassOrFunction) is types.FunctionType:
            setattr(
                storableClassOrFunction,
                StorageDecoration.KEY_FUNCTION,
                (url, restrict, methods, contentType),
            )
        # Did we use @http to decorate a storable class?
        else:
            storable = storableClassOrFunction
            s = StorageDecoration(storable, url, restrict, methods, export)
            setattr(storable, StorageDecoration.KEY, s)
        return storableClassOrFunction

    return wrapper


# -----------------------------------------------------------------------------
#
# STORAGE DECORATION
#
# -----------------------------------------------------------------------------


class StorageDecoration:
    """A class that allows to store decoration information on Storable classes"""

    KEY = "_storage_web_StorageDecoration"
    KEY_FUNCTION = "_storage_web_StorageDecoration_Function"

    @classmethod
    def Get(cls, storableClass):
        return getattr(storableClass, cls.KEY)

    @classmethod
    def Has(cls, storableClass):
        return hasattr(storableClass, cls.KEY)

    def __init__(self, storableClass, url, restrict=None, methods=None, export=None):
        assert issubclass(
            storableClass, Storable
        ), "Storable class requires a Storable object"
        self.storable = storableClass
        self.url = url
        self.restrict = restrict
        self.httpMethods = [methods] if isinstance(methods, str) else methods
        # These are options to give to the object export function. If export
        # is a string, it is assumed to be a profile.
        if type(export) in (str, str):
            export = dict(profile=export)
        self.export = export or {}
        # NOTE: We add the "web" target so that object export function can
        # hide information that should not be communicated (ie. password)
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


# -----------------------------------------------------------------------------
#
# STORAGE SERVICE
#
# -----------------------------------------------------------------------------


class StorageServer(retro.web.Component):
    """A Retro web component that exposes the given storable classes through
    a RESTful Web API.

    The given classes need to have been previously decorated using the `http`
    decorator in this module.
    """

    LIST_COUNT = 20

    def __init__(self, prefix="/api", classes=None, readonly=False):
        retro.web.Component.__init__(self)
        self.PREFIX = prefix
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
        """Alias for `add`. Uses the given decorated storable classes and
        expose them through the API."""
        return self.add(*storableClasses)

    def add(self, *storableClasses):
        """Uses the given decorated storable classes and expose them through
        the API."""
        for s in storableClasses:
            info = getattr(s, StorageDecoration.KEY)
            assert info, "Storable class must be decorated with StorageDecoration"
            assert isinstance(
                info, StorageDecoration
            ), "Storable information should be StorageDecoration"
            self.storableClasses.append(s)
        return self

    def create(self, request, storableClass):
        """Creates a new instance of the given storable class based on the
        given request data."""
        if self.readonly:
            return request.notAuthorized()
        info = StorageDecoration.Get(storableClass)
        return self.onStorableCreate(storableClass, info, request)

    def remove(self, request, storableClass, sid):
        """Removes the given storable."""
        if self.readonly:
            return request.notAuthorized()
        info = StorageDecoration.Get(storableClass)
        return self.onStorableRemove(storableClass, info, request, sid)

    def update(self, request, storableClass, sid):
        """Updates an existing instance of the given storable class based on the
        given request data."""
        if self.readonly:
            return request.notAuthorized()
        info = StorageDecoration.Get(storableClass)
        return self.onStorableUpdate(storableClass, info, request, sid)

    def get(self, request, storableClass, sid):
        """Gets the instance of the given storable class with the given id."""
        info = StorageDecoration.Get(storableClass)
        return self.onStorableGet(storableClass, info, request, sid)

    # TODO: Implement invoke

    def start(self):
        """At component startup, this generates the HTTP handlers for
        the storables."""
        # We do not use a for iteration as we need to creat lambda specific to
        # each storable class, and similarilty to JavaScript a closure that closes
        # on an iteration will keep the first argument
        for _ in self.storableClasses:
            self._generateHandlers(_)

    def onStorableCreate(self, storableClass, info, request):
        """Extracts the JSON data from the given request and use it as import
        data for the  given class"""
        data = request.data()
        if data:
            data = json.loads(data)
            storable = storableClass.Import(data).save()
        # FIXME: We should have an option allowing to create an object
        # on new data
        else:
            storable = storableClass()
        self._doUpdate()
        return request.returns(storable.export(**info.getExportOptions()))

    def onStorableUpdate(self, storableClass, info, request, sid):
        storable = storableClass.Get(sid)
        request.load()
        data = request.params()
        if not storable:
            # We create the object if it does not already exist (non-strict update)
            storable = storableClass.Import(data)
            storable.save()
        else:
            storable.update(data)
            storable.save()
        self._doUpdate()
        return request.returns(storable.export(**info.getExportOptions()))

    def onStorableRemove(self, storableClass, info, request, sid):
        storable = storableClass.Get(sid)
        request.load()
        data = request.params()
        if storable:
            storable.remove()
            self._doUpdate()
            return request.returns(True)
        else:
            return request.notFound()

    def onStorableGet(self, storableClass, info, request, sid):
        storable = storableClass.Get(sid)
        # FIXME: Should have a property to tell whether we create an object
        # when it does not exist
        if not storable:
            if request.has("strict"):
                return request.notFound()
            else:
                storable = storableClass(oid=sid)
        return request.returns(storable.export(**info.getExportOptions()))

    def onStorableInvokeMethod(
        self, storableClass, name, contentType, request, sid, *args, **kwargs
    ):
        storable = storableClass.Get(sid)
        if not storable:
            return request.notFound()
        method = getattr(storable, name)
        if request.method() == "POST":
            request.load()
            kwargs = dict(list(request.params().items()) + list(kwargs.items()))
            # If post is passed without named arguments, it will be
            # mapped to the '' key.
            if "" in kwargs:
                args = [kwargs.get("")]
                del kwargs[""]
            else:
                args = None
        else:
            # If we're not posting, then we might want to grab the parameters from
            # the url. This is temporary, as the call will fail if extra, non-matching
            # kwargs/params are given.
            if not args and not kwargs:
                kwargs = request.params()
        if not storable:
            return request.notFound()
        # We restore the values, if any
        args = [restore(_) for _ in args] if args else []
        kwargs = (
            dict((k, restore(v)) for k, v in list(kwargs.items())) if kwargs else {}
        )
        if not contentType:
            return request.returns(method(*args, **kwargs))
        else:
            if isinstance(contentType, types.FunctionType):
                contentType = contentType(storable)
            return request.respond(method(*args, **kwargs), contentType=contentType)

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

    def _generateHandlers(self, s):
        """Internal method that generates HTTP handlers for the given
        storable class."""
        info = StorageDecoration.Get(s)
        url = info.url or info.getName()
        handler_create = lambda request: self.onStorableCreate(s, info, request)
        handler_update = lambda request, sid: self.onStorableUpdate(
            s, info, request, sid
        )
        handler_get = lambda request, sid: self.onStorableGet(s, info, request, sid)
        handler_remove = lambda request, sid: self.onStorableRemove(
            s, info, request, sid
        )
        handler_list = lambda request, start=0, end=None: self.onStorableList(
            s, info, request, start, end
        )
        # Generic to storable
        self.registerHandler(handler_create, dict(GET_POST=url))
        self.registerHandler(handler_update, dict(POST=url + "/{sid:segment}"))
        self.registerHandler(handler_remove, dict(POST=url + "/{sid:segment}/remove"))
        self.registerHandler(handler_get, dict(GET=url + "/{sid:segment}"))
        self.registerHandler(handler_list, dict(GET=url + "/list"))
        self.registerHandler(handler_list, dict(GET=url + "/list/{start:int}"))
        self.registerHandler(handler_list, dict(GET=url + "/list/{start:int}:"))
        self.registerHandler(
            handler_list, dict(GET=url + "/list/{start:int}:{end:int}")
        )
        # Lists the invocables defined in the storable and bind URLs
        for name, meta in info.listInvocables():

            def wrap(name, meta):
                invoke_url, restrict, methods, contentType = meta
                # TODO: What about restrict?
                handler = (
                    lambda request, sid, *args, **kwargs: self.onStorableInvokeMethod(
                        s, name, contentType, request, sid, *args, **kwargs
                    )
                )
                urls = {}
                if isinstance(methods, str):
                    methods = (methods,)
                for method in methods or ("GET", "POST"):
                    urls[method.upper()] = url + "/{sid:segment}/" + invoke_url
                self.registerHandler(handler, urls)

            wrap(name, meta)
        # Specific to StoredRaw
        if issubclass(s, StoredRaw):
            handler = lambda request, sid: self.onRawGetData(s, request, sid)
            self.registerHandler(handler, dict(GET=url + "/{sid:segment}/data"))


# EOF
