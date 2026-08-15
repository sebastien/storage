"""HTTP handlers for stored object relations."""

from urllib.parse import unquote

from ..core import getCanonicalName, isSame
from .decorators import StorageDecoration
from .errors import StorageWebError

__all__ = ["RelationWebMixin"]


class RelationWebMixin:
	"""Relation endpoints and relation command support for storage services."""

	def onRelationCommand(self, request, command, index=None):
		op = str(command.get("op") or "")
		operation = op.split(".", 1)[1] if "." in op else ""
		storableClass, info = self.commandMatch(command.get("type"))
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
		storable = self.scopedGet(request, storableClass, str(sid))
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
		data = self.commandPayload(
			command, exclude=("op", "type", "id", "relation", "return")
		)
		self.validateRelationRevision(storable, name, data)
		changed = self.applyRelationOperation(relation, operation, data)
		if changed:
			storable.save()
		if command.get("return") == "none":
			return dict(ok=True, operation=operation)
		if command.get("return") == "object":
			return dict(
				ok=True,
				op=op,
				type=info.getName(),
				id=str(sid),
				value=self.toPublicValue(
					storable.export(**info.getExportOptions()), request
				),
			)
		page = self.relationPageData(
			storable, info, relation, name, command, operation=operation
		)
		page["op"] = op
		return self.toPublicValue(page, request)

	def onStorableRelations(self, storableClass, info, request, sid, format=None):
		sid = unquote(sid)
		storable = self.scopedGet(request, storableClass, sid)
		if not storable:
			return request.notFound()
		res = {}
		for name, relation in (
			storable.iterRelations() if hasattr(storable, "iterRelations") else ()
		):
			relationClass = relation.getRelationClass()
			res[name] = dict(
				many=relation.isMany(),
				type=getCanonicalName(relationClass),
				count=len(relation),
				revision=storable.getUpdateTime(name),
			)
		return self.respondFormatted(
			request,
			self.toPublicValue(
				dict(type=info.getName(), id=sid, relations=res), request
			),
			format=format,
		)

	def onStorableRelationCount(
		self, storableClass, info, request, sid, name, format=None
	):
		format = format or self.requestFormat(request)
		sid = unquote(sid)
		storable = self.scopedGet(request, storableClass, sid)
		if not storable:
			return request.notFound()
		try:
			relation = self.getStorableRelation(storable, name)
		except StorageWebError as error:
			return self.storageError(request, error, dict(type=info.getName(), id=sid))
		return self.respondFormatted(
			request,
			dict(
				type=info.getName(),
				id=sid,
				relation=name,
				count=len(relation),
				revision=storable.getUpdateTime(name),
			),
			format=format,
		)

	def onStorableRelationGet(
		self, storableClass, info, request, sid, name, start=0, end=None, format=None
	):
		format = format or self.requestFormat(request)
		sid = unquote(sid)
		name = self.stripFormatSuffix(name, format)
		storable = self.scopedGet(request, storableClass, sid)
		if not storable:
			return request.notFound()
		try:
			relation = self.getStorableRelation(storable, name)
		except StorageWebError as error:
			return self.storageError(request, error, dict(type=info.getName(), id=sid))
		return self.respondFormatted(
			request,
			self.toPublicValue(
				self.relationPage(
					storable, info, relation, name, request, start=start, end=end
				),
				request,
			),
			format=format,
		)

	async def onStorableRelationOperation(
		self, storableClass, info, request, sid, name, operation
	):
		if self.readonly:
			return request.notAuthorized()
		sid = unquote(sid)
		storable = self.scopedGet(request, storableClass, sid)
		if not storable:
			return request.notFound()
		try:
			data = await request.loadParams()
			relation = self.getStorableRelation(storable, name)
			self.validateRelationRevision(storable, name, data)
			if self.applyRelationOperation(relation, operation, data):
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
			return request.returns(
				self.toPublicValue(storable.export(**info.getExportOptions()), request)
			)
		return request.returns(
			self.toPublicValue(
				self.relationPage(
					storable, info, relation, name, request, operation=operation
				),
				request,
			)
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

	def relationPage(
		self, storable, info, relation, name, request, start=0, end=None, operation=None
	):
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

	def relationPageData(
		self, storable, info, relation, name, options=None, operation=None
	):
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
			start, end = self.relationRange(
				data, len(values), names=("from", "end"), single=True
			)
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

	def _iterHandlers(self, storableClass):
		yield from super()._iterHandlers(storableClass)
		info = StorageDecoration.Get(storableClass)
		url = (info.url or info.getName()).lstrip("/")

		def get_relations(request, sid, _format=None):
			return self.onStorableRelations(
				storableClass, info, request, sid, format=_format
			)

		def get_relations_md(request, sid):
			return self.onStorableRelations(
				storableClass, info, request, sid, format="md"
			)

		def get_relations_xml(request, sid):
			return self.onStorableRelations(
				storableClass, info, request, sid, format="xml"
			)

		def get_count(request, sid, name, _format=None):
			return self.onStorableRelationCount(
				storableClass, info, request, sid, name, format=_format
			)

		def get_count_md(request, sid, name):
			return self.onStorableRelationCount(
				storableClass, info, request, sid, name, format="md"
			)

		def get_count_xml(request, sid, name):
			return self.onStorableRelationCount(
				storableClass, info, request, sid, name, format="xml"
			)

		def get_relation(request, sid, name, start=0, end=None, _format=None):
			return self.onStorableRelationGet(
				storableClass, info, request, sid, name, start, end, format=_format
			)

		def get_relation_md(request, sid, name, start=0, end=None):
			return self.onStorableRelationGet(
				storableClass, info, request, sid, name, start, end, format="md"
			)

		def get_relation_xml(request, sid, name, start=0, end=None):
			return self.onStorableRelationGet(
				storableClass, info, request, sid, name, start, end, format="xml"
			)

		async def operate(request, sid, name, operation):
			return await self.onStorableRelationOperation(
				storableClass, info, request, sid, name, operation
			)

		base = url + "/{sid:segment}/relations"
		yield self._handler(
			get_relations,
			("GET", base),
		)
		yield self._handler(get_relations_md, ("GET", base + ".md"))
		yield self._handler(get_relations_xml, ("GET", base + ".xml"))
		path = base + "/{name:segment}"
		yield self._handler(get_relation_md, ("GET", path + ".md"))
		yield self._handler(get_relation_xml, ("GET", path + ".xml"))
		yield self._handler(get_relation, ("GET", path))
		yield self._handler(get_count_md, ("GET", path + "/count.md"))
		yield self._handler(get_count_xml, ("GET", path + "/count.xml"))
		yield self._handler(get_count, ("GET", path + "/count"))
		for suffix, handler in (
			("", get_relation),
			(".md", get_relation_md),
			(".xml", get_relation_xml),
		):
			yield self._handler(handler, ("GET", path + "/list" + suffix))
			yield self._handler(handler, ("GET", path + "/list/{start:int}" + suffix))
			yield self._handler(handler, ("GET", path + "/list/{start:int}:" + suffix))
			yield self._handler(
				handler, ("GET", path + "/list/{start:int}:{end:int}" + suffix)
			)
		yield self._handler(operate, ("POST", path + "/{operation:segment}"))
