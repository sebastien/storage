"""Stored object schema snapshots, diffs, and validation."""

import inspect
from copy import deepcopy
from typing import Any

from .core import getCanonicalName
from .migrations import MigrationOperator
from .objects import Ownership, StoredObject

SCHEMA_METADATA_KEY = "schema.objects"
SCHEMA_VERSION = 1


def _canonicalClassName(value: Any) -> str:
	if isinstance(value, str):
		return value
	if inspect.isclass(value):
		return getCanonicalName(value)
	raise ValueError(f"Expected class or canonical class name, got {value}")


def _resolveDeclaration(value: Any):
	if isinstance(value, dict):
		return value
	if value is None:
		return {}
	if callable(value):
		parameters = inspect.signature(value).parameters
		return value(None) if len(parameters) == 1 else value()
	return value


def _normalizePropertyType(value: Any) -> dict[str, str]:
	if isinstance(value, dict) and set(value.keys()) == {"type"}:
		return deepcopy(value)
	return {"type": str(value)}


def _normalizeRelation(value: Any) -> dict[str, Any]:
	many = isinstance(value, (list, tuple))
	target = value[0] if many else value
	return {"target": _canonicalClassName(target), "many": many}


def _normalizeOwnership(value: Ownership | None) -> dict[str, Any] | None:
	if value is None:
		return None
	return {
		"owner": _canonicalClassName(value.ownerType),
		"required": bool(value.required),
		"cascade": bool(value.cascade),
	}


def _normalizeMigrationChange(change: dict[str, Any]) -> dict[str, Any]:
	result = dict(change)
	if "class" in result:
		result["class"] = _canonicalClassName(result["class"])
	if "fromClass" in result:
		result["fromClass"] = _canonicalClassName(result["fromClass"])
	if "toClass" in result:
		result["toClass"] = _canonicalClassName(result["toClass"])
	if result.get("op") == "addProperty" and "type" in result:
		result["type"] = _normalizePropertyType(result["type"])
	if result.get("op") == "changePropertyType":
		if "from" in result:
			result["from"] = str(result["from"])
		if "to" in result:
			result["to"] = str(result["to"])
	if result.get("op") == "splitProperty" and "to" in result:
		result["to"] = {
			name: _normalizePropertyType(value)
			for name, value in list(result["to"].items())
		}
	if result.get("op") in ("addRelation", "changeRelation"):
		if "target" in result:
			result["target"] = _canonicalClassName(result["target"])
		if "fromTarget" in result:
			result["fromTarget"] = _canonicalClassName(result["fromTarget"])
		if "toTarget" in result:
			result["toTarget"] = _canonicalClassName(result["toTarget"])
	if result.get("op") == "changeOwnership":
		if "from" in result:
			result["from"] = _normalizeOwnership(result["from"])
		if "to" in result:
			result["to"] = _normalizeOwnership(result["to"])
	return result


def getMigrationSchemaChanges(module) -> list[dict[str, Any]]:
	changes = []
	moduleChanges = getattr(module, "SCHEMA_CHANGES", None) or []
	apply = getattr(module, "apply", None)
	applyChanges = getattr(apply, "SCHEMA_CHANGES", None) or []
	for change in moduleChanges:
		changes.append(_normalizeMigrationChange(change))
	for change in applyChanges:
		changes.append(_normalizeMigrationChange(change))
	return changes


class _SchemaChanges:
	def _wrap(self, change: dict[str, Any]):
		change = _normalizeMigrationChange(change)

		def decorator(function):
			existing = list(getattr(function, "SCHEMA_CHANGES", []))
			function.SCHEMA_CHANGES = [change] + existing
			return function

		return decorator

	def addProperty(self, storedObjectClass, name: str, type: Any):
		return self._wrap(
			{"op": "addProperty", "class": storedObjectClass, "name": name, "type": type}
		)

	def removeProperty(self, storedObjectClass, name: str):
		return self._wrap(
			{"op": "removeProperty", "class": storedObjectClass, "name": name}
		)

	def renameProperty(self, storedObjectClass, old: str, new: str):
		return self._wrap(
			{"op": "renameProperty", "class": storedObjectClass, "from": old, "to": new}
		)

	def splitProperty(self, storedObjectClass, old: str, to: dict[str, Any]):
		return self._wrap(
			{"op": "splitProperty", "class": storedObjectClass, "from": old, "to": to}
		)

	def changePropertyType(self, storedObjectClass, name: str, old: Any, new: Any):
		return self._wrap(
			{
				"op": "changePropertyType",
				"class": storedObjectClass,
				"name": name,
				"from": old,
				"to": new,
			}
		)


changes = _SchemaChanges()


class SchemaDelta:
	SAFE_OPERATIONS = {"addClass", "addProperty", "addRelation"}

	def __init__(self, changes: list[dict[str, Any]]):
		self.changes = changes

	def requiresMigration(self) -> bool:
		return any(_["op"] not in self.SAFE_OPERATIONS for _ in self.changes)

	def isEmpty(self) -> bool:
		return not self.changes


class Schema:
	def __init__(self, version: int = SCHEMA_VERSION, classes: dict[str, dict[str, Any]] | None = None):
		self.version = version
		self.classes = classes or {}

	@classmethod
	def FromData(cls, data: dict[str, Any] | None):
		if not data:
			return cls()
		return cls(version=data.get("version", SCHEMA_VERSION), classes=deepcopy(data.get("classes", {})))

	@classmethod
	def FromClasses(cls, classes: list[type[StoredObject]]):
		result = {}
		for storedObjectClass in classes:
			properties = _resolveDeclaration(getattr(storedObjectClass, "PROPERTIES", {})) or {}
			relations = _resolveDeclaration(getattr(storedObjectClass, "RELATIONS", {})) or {}
			ownership = storedObjectClass.GetOwnership()
			result[getCanonicalName(storedObjectClass)] = {
				"type": getCanonicalName(storedObjectClass),
				"collection": storedObjectClass.StoragePrefix(),
				"properties": {
					name: _normalizePropertyType(value)
					for name, value in list(properties.items())
				},
				"relations": {
					name: _normalizeRelation(value)
					for name, value in list(relations.items())
				},
				"ownership": _normalizeOwnership(ownership),
				"indexes": [],
			}
		return cls(classes=result)

	def clone(self):
		return Schema.FromData(self.export())

	def export(self) -> dict[str, Any]:
		return {"version": self.version, "classes": deepcopy(self.classes)}

	def subset(self, names: set[str]):
		return Schema(
			version=self.version,
			classes={key: deepcopy(value) for key, value in list(self.classes.items()) if key in names},
		)

	def update(self, other: "Schema"):
		for key, value in list(other.classes.items()):
			self.classes[key] = deepcopy(value)
		return self

	def diff(self, other: "Schema") -> SchemaDelta:
		changes = []
		for className in sorted(set(self.classes.keys()) | set(other.classes.keys())):
			current = self.classes.get(className)
			target = other.classes.get(className)
			if current is None:
				changes.append({"op": "addClass", "class": className})
				continue
			if target is None:
				changes.append({"op": "removeClass", "class": className})
				continue
			if current.get("collection") != target.get("collection"):
				changes.append(
					{
						"op": "changeCollection",
						"class": className,
						"from": current.get("collection"),
						"to": target.get("collection"),
					}
				)
			if current.get("ownership") != target.get("ownership"):
				changes.append(
					{
						"op": "changeOwnership",
						"class": className,
						"from": deepcopy(current.get("ownership")),
						"to": deepcopy(target.get("ownership")),
					}
				)
			currentProperties = current.get("properties", {})
			targetProperties = target.get("properties", {})
			for name in sorted(set(currentProperties.keys()) | set(targetProperties.keys())):
				left = currentProperties.get(name)
				right = targetProperties.get(name)
				if left is None:
					changes.append(
						{
							"op": "addProperty",
							"class": className,
							"name": name,
							"type": deepcopy(right),
						}
					)
				elif right is None:
					changes.append(
						{
							"op": "removeProperty",
							"class": className,
							"name": name,
							"type": deepcopy(left),
						}
					)
				elif left != right:
					changes.append(
						{
							"op": "changePropertyType",
							"class": className,
							"name": name,
							"from": left.get("type"),
							"to": right.get("type"),
						}
					)
			currentRelations = current.get("relations", {})
			targetRelations = target.get("relations", {})
			for name in sorted(set(currentRelations.keys()) | set(targetRelations.keys())):
				left = currentRelations.get(name)
				right = targetRelations.get(name)
				if left is None:
					changes.append(
						{
							"op": "addRelation",
							"class": className,
							"name": name,
							"target": right.get("target"),
							"many": right.get("many"),
						}
					)
				elif right is None:
					changes.append(
						{
							"op": "removeRelation",
							"class": className,
							"name": name,
							"target": left.get("target"),
							"many": left.get("many"),
						}
					)
				elif left != right:
					changes.append(
						{
							"op": "changeRelation",
							"class": className,
							"name": name,
							"fromTarget": left.get("target"),
							"toTarget": right.get("target"),
							"fromMany": left.get("many"),
							"toMany": right.get("many"),
						}
					)
		return SchemaDelta(changes)

	def applyChanges(self, changes: list[dict[str, Any]]):
		for change in changes:
			self.applyChange(change)
		return self

	def applyChange(self, change: dict[str, Any]):
		change = _normalizeMigrationChange(change)
		op = change.get("op")
		className = change.get("class")
		if op == "addClass":
			self.classes[className] = change.get("schema", {})
			return self
		if className not in self.classes:
			raise RuntimeError(f"Migration change references unknown class: {className}")
		storedClass = self.classes[className]
		properties = storedClass.setdefault("properties", {})
		relations = storedClass.setdefault("relations", {})
		if op == "addProperty":
			properties[change["name"]] = deepcopy(change["type"])
		elif op == "removeProperty":
			properties.pop(change["name"], None)
		elif op == "renameProperty":
			if change["from"] not in properties:
				raise RuntimeError(
					f"Migration cannot rename missing property {className}.{change['from']}"
				)
			properties[change["to"]] = properties.pop(change["from"])
		elif op == "splitProperty":
			if change["from"] not in properties:
				raise RuntimeError(
					f"Migration cannot split missing property {className}.{change['from']}"
				)
			properties.pop(change["from"], None)
			for name, spec in list(change["to"].items()):
				properties[name] = deepcopy(spec)
		elif op == "changePropertyType":
			if change["name"] not in properties:
				raise RuntimeError(
					f"Migration cannot update missing property {className}.{change['name']}"
				)
			properties[change["name"]] = _normalizePropertyType(change["to"])
		elif op == "addRelation":
			relations[change["name"]] = {
				"target": change["target"],
				"many": bool(change.get("many")),
			}
		elif op == "removeRelation":
			relations.pop(change["name"], None)
		elif op == "changeRelation":
			if change["name"] not in relations:
				raise RuntimeError(
					f"Migration cannot update missing relation {className}.{change['name']}"
				)
			relations[change["name"]] = {
				"target": change["toTarget"],
				"many": bool(change.get("toMany")),
			}
		elif op == "changeOwnership":
			storedClass["ownership"] = deepcopy(change.get("to"))
		else:
			raise RuntimeError(f"Unsupported schema migration operation: {op}")
		return self


class SchemaValidationError(RuntimeError):
	pass


class SchemaValidator:
	def __init__(self, storage, migrate: bool = True):
		self.storage = storage
		self.backend = storage.backend
		self.migrate = migrate

	def validate(self):
		declaredClassNames = set(self.storage._declaredClasses.keys())
		if not declaredClassNames:
			return None
		currentSchema = Schema.FromClasses(list(self.storage._declaredClasses.values()))
		storedSchema = Schema.FromData(self.backend.getMetadata(SCHEMA_METADATA_KEY))
		if not storedSchema.classes:
			self._store(storedSchema.update(currentSchema))
			return currentSchema
		currentSubset = currentSchema.subset(declaredClassNames)
		storedSubset = storedSchema.subset(declaredClassNames)
		directDelta = storedSubset.diff(currentSubset)
		if not directDelta.requiresMigration():
			if not directDelta.isEmpty():
				merged = storedSchema.clone().update(currentSchema)
				self._store(merged)
			return currentSchema
		operator = MigrationOperator(self.storage)
		simulated = storedSchema.clone()
		for migration in operator.pending():
			changes = operator.getSchemaChanges(migration)
			if changes:
				simulated.applyChanges(changes)
		remaining = simulated.subset(declaredClassNames).diff(currentSubset)
		if not remaining.isEmpty():
			raise SchemaValidationError(self._formatFailure(remaining))
		if not self.migrate:
			raise SchemaValidationError(
				self._formatFailure(directDelta if remaining.isEmpty() else remaining)
			)
		operator.apply()
		self._store(storedSchema.clone().update(currentSchema))
		return currentSchema

	def _store(self, schema: Schema):
		self.backend.setMetadata(SCHEMA_METADATA_KEY, schema.export())
		if hasattr(self.backend, "sync"):
			self.backend.sync()
		return schema

	def _formatFailure(self, delta: SchemaDelta) -> str:
		lines = ["Schema migration required.", "", "Uncovered changes:"]
		for change in delta.changes:
			lines.append(f"- {self._describeChange(change)}")
		return "\n".join(lines)

	def _describeChange(self, change: dict[str, Any]) -> str:
		op = change["op"]
		className = change.get("class")
		if op == "removeProperty":
			return f"remove property {className}.{change['name']}"
		if op == "changePropertyType":
			return (
				f"change property type {className}.{change['name']} "
				f"from {change['from']} to {change['to']}"
			)
		if op == "removeRelation":
			return f"remove relation {className}.{change['name']}"
		if op == "changeRelation":
			return (
				f"change relation {className}.{change['name']} "
				f"from {change['fromTarget']} to {change['toTarget']}"
			)
		if op == "changeOwnership":
			return f"change ownership for {className}"
		if op == "removeClass":
			return f"remove class {className}"
		if op == "changeCollection":
			return f"change collection for {className} from {change['from']} to {change['to']}"
		if op == "addProperty":
			return f"add property {className}.{change['name']}"
		if op == "addRelation":
			return f"add relation {className}.{change['name']}"
		if op == "addClass":
			return f"add class {className}"
		return repr(change)


__all__ = [
	"SCHEMA_METADATA_KEY",
	"SCHEMA_VERSION",
	"Schema",
	"SchemaDelta",
	"SchemaValidationError",
	"SchemaValidator",
	"changes",
	"getMigrationSchemaChanges",
]


# EOF
